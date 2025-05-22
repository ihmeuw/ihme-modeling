import concurrent.futures
import copy
import sys
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from core_maths import interpolate
from gbd import constants as gbd_constants
from gbd.estimation_years import estimation_years_from_release_id
from ihme_dimensions.dimensionality import DataFrameDimensions

from como.lib import common, constants, draw_io, utils, version
from como.lib.exceptions import InputCollectionException
from como.lib.types import InputCollectionResult, MultiInputCollectionArgs
from como.lib.utils import startup_jitter


def get_dimensions(
    como_version: version.ComoVersion,
    location_id: int,
    sex_id: int,
    measure_id: Union[int, list[int]],
    input_collector_type: str,
) -> DataFrameDimensions:
    """Get location- and sex-specific dimensions."""
    dimensions = como_version.nonfatal_dimensions
    dimensions.simulation_index[gbd_constants.columns.LOCATION_ID] = location_id
    dimensions.simulation_index[gbd_constants.columns.SEX_ID] = sex_id

    if input_collector_type == constants.InputCollector.BIRTH_PREV:
        dimensions.simulation_index[gbd_constants.columns.AGE_GROUP_ID] = [
            gbd_constants.age.BIRTH
        ]
    return dimensions.get_simulation_dimensions(measure_id=measure_id, at_birth=False)


def construct_filters(
    dimensions: DataFrameDimensions, release_id: int
) -> Dict[str, List[int]]:
    """Create filters for input collection."""
    filters = dimensions.index_dim.to_dict()["levels"]

    req_years = list(filters[gbd_constants.columns.YEAR_ID])
    estim_years = estimation_years_from_release_id(release_id)
    if not set(req_years).issubset(set(estim_years)):
        filters[gbd_constants.columns.YEAR_ID] = list(set(req_years + estim_years))

    # Remove derivation filters for wormhole
    content_filters = common.remove_derivation_filters(filters=filters)
    return content_filters


def get_time_series_group_cols(
    df: pd.DataFrame, dimensions: DataFrameDimensions, input_collector_type: str
) -> List[str]:
    """Extract the group columns needed for time-series interpolation."""
    input_collector_column_map: Dict[str, List[str]] = {
        constants.InputCollector.BIRTH_PREV: [
            gbd_constants.columns.SEQUELA_ID,
            gbd_constants.columns.CAUSE_ID,
        ],
        constants.InputCollector.SEXUAL_VIOLENCE: [gbd_constants.columns.CAUSE_ID],
        constants.InputCollector.SEQUELA: [
            gbd_constants.columns.SEQUELA_ID,
            gbd_constants.columns.CAUSE_ID,
            gbd_constants.columns.HEALTHSTATE_ID,
        ],
        constants.InputCollector.EN_INJURY: [
            gbd_constants.columns.SEQUELA_ID,
            gbd_constants.columns.CAUSE_ID,
            gbd_constants.columns.HEALTHSTATE_ID,
            gbd_constants.columns.REI_ID,
        ],
    }

    dimensions = copy.deepcopy(dimensions)
    for column in input_collector_column_map[input_collector_type]:
        dimensions.index_dim.add_level(column, df[column].unique().tolist())

    return dimensions.index_names


def transform_result(
    df: pd.DataFrame, index_names: List[str], req_years: List[int], input_collector_type: str
) -> pd.DataFrame:
    """Optionally interpolate years, filter years, and assign measure."""
    if not set(df.year_id.unique()).issuperset(set(req_years)):
        draw_cols = utils.ordered_draw_columns(df=df)
        interp_df = interpolate.pchip_interpolate(
            df=df,
            id_cols=index_names,
            value_cols=draw_cols,
            time_col=gbd_constants.columns.YEAR_ID,
            time_vals=req_years,
        )
        df = df[df.year_id.isin(req_years)]
        df = pd.concat([df, interp_df])
    else:
        df = df[df.year_id.isin(req_years)]

    if input_collector_type == constants.InputCollector.BIRTH_PREV:
        df[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.INCIDENCE
    return df


def read_multi_inputs(args: MultiInputCollectionArgs) -> List[InputCollectionResult]:
    """Sequentially collects a set of inputs."""
    startup_jitter()
    como_version = version.ComoVersion(como_dir=args.como_dir, read_only=True)
    como_version.load_cache()

    dimensions = get_dimensions(
        como_version=como_version,
        location_id=args.location_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        input_collector_type=args.input_collector_type,
    )
    filters = construct_filters(dimensions=dimensions, release_id=como_version.release_id)

    source_factory = draw_io.SourceSinkFactory(como_version)
    results: List[InputCollectionResult] = []

    source_function_map = {
        constants.InputCollector.BIRTH_PREV: (
            source_factory.get_birth_prev_modelable_entity_source
        ),
        constants.InputCollector.SEQUELA: (
            source_factory.get_sequela_modelable_entity_source
        ),
        constants.InputCollector.SEXUAL_VIOLENCE: (
            source_factory.get_sexual_violence_modelable_entity_source
        ),
        constants.InputCollector.EN_INJURY: (
            source_factory.get_en_injuries_modelable_entity_source
        ),
    }

    for model_source in args.model_sources:
        result = InputCollectionResult(
            modelable_entity_id=model_source.modelable_entity_id,
            model_version_id=model_source.model_version_id,
        )
        source_function = source_function_map.get(args.input_collector_type)
        df = source_function(
            modelable_entity_id=model_source.modelable_entity_id,
            model_version_id=model_source.model_version_id,
        ).content(filters=filters)

        if not df.empty:
            index_names = get_time_series_group_cols(
                df=df, dimensions=dimensions, input_collector_type=args.input_collector_type
            )
            result.df = transform_result(
                df=df,
                index_names=index_names,
                req_years=como_version.year_id,
                input_collector_type=args.input_collector_type,
            )
        results.append(result)

    return results


def collect_input_data(chunked_args: List, n_processes: int) -> List[InputCollectionResult]:
    """Attempt to pull input data."""
    result_list = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_processes) as executor:
        worker_futures = {
            executor.submit(read_multi_inputs, args): args for args in chunked_args
        }
        for worker in concurrent.futures.as_completed(worker_futures):
            try:
                result = worker.result()
                result_list.append(result)
            except Exception as err:
                message = f"Exception when collecting batch {worker_futures[worker]}."
                sigkill = False
                if isinstance(err, concurrent.futures.process.BrokenProcessPool):
                    logger.error(
                        message + "\nProcess pool died abruptly. Assuming sigkill due to OOM "
                        f"killer. Returning exit code 137 for jobmon resource retry: \n{err}"
                    )
                    sigkill = True
                if any(
                    [i in str(err) for i in ["Signals.SIGKILL: 9", "Cannot allocate memory"]]
                ):
                    logger.error(message + f"\n{err}")
                    sigkill = True
                if sigkill:
                    sys.exit(137)
                raise InputCollectionException(message) from err

    # turn list of list into list
    result_list = [i for j in result_list for i in j]
    return result_list
