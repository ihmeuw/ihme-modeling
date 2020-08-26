import os
import pandas as pd
from typing import Callable, List, Optional

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from db_queries import get_envelope, get_population
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from get_draws.base.utils import ndraw_grouper
from get_draws.api import _downsample
from hierarchies import dbtrees

from fauxcorrect.parameters.master import MachineParameters
from fauxcorrect.utils import constants, io
from fauxcorrect.validations.error_check import check_duplicates


def validate_mortality_process_argument(mort_process: str) -> None:
    """
    Raises an error if the mort_process is not one of 'population',
    'envelope_draws', or 'envelope_summary'.

    Raises:
        RuntimeError
    """
    if mort_process not in ValidMortalityProcessInputs.OPTIONS:
        raise RuntimeError(
            f"Cannot cache mortality input type: {mort_process}. Valid "
            "mortality process types are: "
            f"{', '.join(constants.MortalityInputs.ALL_INPUTS)}."
        )


def cache_data(mort_process: str, version: MachineParameters) -> None:
    """
    Saves mortality data to the DeathMachine filesystem.

    Using the input_type argument, we create a mortality controller. This
    object will return the correct mortality function, file name pattern, and
    HDF key for use in fauxcorrect.utils.io.cache_hdf.

    Arguments:
         mort_process (str): the mortality process to save to cache. One of
             'population' or 'envelope'.
         version (MachineParameters): master parameters for the given
             machine run.
    """
    controller = MortController(mort_process)
    filepath = os.path.join(
        _get_mortality_input_path(version.parent_dir),
        controller.file_pattern
    )
    # Read in data
    data = controller.get_data(version=version)
    check_duplicates(data)
    # Save to disk
    # Need mode to be 'w' (as opposed to 'a') so duplicate data is not
    # appended during jobmon retries
    io.cache_hdf(
        data,
        filepath=filepath,
        key=controller.hdf_key,
        data_columns=constants.Columns.DEMOGRAPHIC_INDEX
    )


def _get_mortality_input_path(parent_dir: str):
    return os.path.join(
        parent_dir,
        constants.FilePaths.INPUT_FILES_DIR
    )


def _get_envelope_summary(version: MachineParameters) -> pd.DataFrame:
    """
    Wrapper around get_envelope; used to unpack arguments from version object
    as well as rename columns and sub-select dataframe columns.

    Arguments:
        version (FauxCorrectParameters): object containing all the demographic
            and fauxcorrect configuration data needed to query envelope
            estimates.

    Return:
        pd.DataFrame
    """
    env = get_envelope(
        age_group_id=version.all_age_group_ids,
        location_id=version.location_ids,
        year_id=version.year_ids,
        sex_id=version.all_sex_ids,
        decomp_step=version.decomp_step,
        gbd_round_id=version.gbd_round_id,
        run_id=version.envelope_version_id,
        with_shock=0,
        with_hiv=0
    )
    env.rename(
        columns={constants.Columns.MEAN: constants.Columns.ENVELOPE},
        inplace=True
    )
    keep_cols = (
            constants.Columns.DEMOGRAPHIC_INDEX + [constants.Columns.ENVELOPE]
    )
    return env[keep_cols]


def _get_envelope_draws(version: MachineParameters) -> pd.DataFrame:
    """
    Used to unpack arguments from version object as well as rename columns
    and sub-select dataframe columns.

    Arguments:
        version (MachineParameters): object containing all the demographic
            and codcorrect configuration data needed to query envelope
            draws.

    Return:
        pd.DataFrame
    """
    file_path = constants.FilePaths.MORT_ENVELOPE_DRAW_DIR.format(
        version_id=version.envelope_version_id)
    all_files = (
        [
            os.path.join(
                file_path,
                constants.FilePaths.MORT_ENVELOPE_FILE.format(year_id = year))
            for year in version.year_ids
        ]
    )
    downsample = False
    if constants.Draws.N_DRAWS < constants.Draws.MAX_DRAWS:
        downsample = True

    rename_dict = {
        key:value for (key,value) in zip(
            [
                f'{constants.Columns.ENV_PREFIX}{draw_num}'
                for draw_num in range(constants.Draws.MAX_DRAWS)
            ],
            [
                f'{constants.Columns.DRAW_PREFIX}{draw_num}'
                for draw_num in range(constants.Draws.MAX_DRAWS)
            ]
        )
    }

    envelope_list = []
    for f in all_files:
        df = pd.read_hdf(f, key='data')
        df = df.rename(columns=rename_dict)
        if downsample:
            groups = ndraw_grouper(df, "year_id")
            if len(groups) > 2:
                raise DrawException("More than two draw lengths in data: {}. "
                                    "Unable to downsample while retaining "
                                    "draw correlation.".format(list(groups)))
            df = _downsample(downsample, constants.Draws.N_DRAWS, groups)
        envelope_list.append(df)
    envelope = pd.concat(envelope_list)
    envelope = (
        envelope[constants.Columns.DEMOGRAPHIC_INDEX +
        constants.Columns.DRAWS]
    )

    # Filter to just the most-detailed
    envelope = envelope.loc[
        (envelope['location_id'].isin(version.most_detailed_location_ids)) &
        (envelope['year_id'].isin(version.year_ids)) &
        (envelope['sex_id'].isin(version.sex_ids)) &
        (envelope['age_group_id'].isin(version.most_detailed_age_group_ids))
    ].reset_index(drop=True)

    return envelope


def _get_population(
        version: MachineParameters,
        location_set_id: int = constants.LocationSetId.OUTPUTS,
        agg_loc_sets: Optional[List[int]] = (
            constants.LocationAggregation.Ids.SPECIAL_LOCATIONS +
            [constants.LocationSetId.OUTPUTS]
        )
) -> pd.DataFrame:
    """
    Unpacks arguments from version object to use with get_population
    function. Requests most detailed ages and most detailed sexes because
    age-sex population aggregates are created in the summarize module.
    Dependant on demographics team to upload population for majority of
    aggregate locations but currently uses AggSynchronous to create population
    information for select Norway locations in LocationSetId.OUTPUTS.

    Arguments:
        version (MachineParameters): object containing all the demographic
            and configuration data needed to query population
            estimates.
        location_set_id (int): The id for hierarchy to aggregate up
        agg_loc_sets (list): Additional location sets to create special
                aggregates

    Return:
        pd.DataFrame
    """
    pop = get_population(
        age_group_id=version.most_detailed_age_group_ids,
        location_id=version.location_ids,
        year_id=version.year_ids,
        sex_id=version.sex_ids,
        run_id=version.population_version_id,
        decomp_step=version.decomp_step,
        gbd_round_id=version.gbd_round_id
    )
    io_mock = {}
    source = DrawSource(
        {
            "draw_dict": io_mock,
            "name": "tmp"
        },
        mem_read_func
    )
    sink = DrawSink(
        {
            "draw_dict": io_mock,
            "name": "tmp"
        },
        mem_write_func
    )
    index_cols = constants.Columns.DEMOGRAPHIC_INDEX
    data_cols = [constants.Columns.POPULATION]
    sink.push(pop[index_cols + data_cols])
    # location
    if agg_loc_sets:
        assert len(agg_loc_sets) == len(set(agg_loc_sets))
        assert agg_loc_sets[-1] == constants.LocationSetId.OUTPUTS

        for set_id in agg_loc_sets:
            loc_tree = dbtrees.loctree(
                location_set_id=set_id,
                gbd_round_id=version.gbd_round_id
            )
            operator = Sum(
                index_cols=(
                    [
                        col for col in index_cols
                        if col != constants.Columns.LOCATION_ID
                    ]
                ),
                value_cols=data_cols
            )
            aggregator = AggSynchronous(
                draw_source=source,
                draw_sink=sink,
                index_cols=(
                    [
                        col for col in index_cols
                        if col != constants.Columns.LOCATION_ID
                    ]
                ),
                aggregate_col=constants.Columns.LOCATION_ID,
                operator=operator
            )
            aggregator.run(loc_tree)
        special_locations = source.content()
    else:
        special_locations = pd.DataFrame()

    return pd.concat(
        [
            pop,
            special_locations.loc[
            ~special_locations.location_id.isin(
                pop.location_id.unique()
            )]
        ],
        ignore_index=True
    )


class ValidMortalityProcessInputs:
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'

    OPTIONS: List[str] = [ENVELOPE_SUMMARY, ENVELOPE_DRAWS, POPULATION]


class MortController:
    """
    Controls the relevant function and data to be returned for either the
    envelope or population caching process.

    The following data structures take a mortality input process and map it to
    the appropriate db_queries function, file pattern, and hdf key for caching.
    """
    _function_map = {
        constants.MortalityInputs.ENVELOPE_DRAWS: _get_envelope_draws,
        constants.MortalityInputs.ENVELOPE_SUMMARY: _get_envelope_summary,
        constants.MortalityInputs.POPULATION: _get_population
    }

    _file_pattern_map = {
        constants.MortalityInputs.ENVELOPE_DRAWS: (
            constants.FilePaths.ENVELOPE_DRAWS_FILE
        ),
        constants.MortalityInputs.ENVELOPE_SUMMARY: (
            constants.FilePaths.ENVELOPE_SUMMARY_FILE
        ),
        constants.MortalityInputs.POPULATION: (
            constants.FilePaths.POPULATION_FILE
        )
    }

    _hdf_key_map = {
        constants.MortalityInputs.ENVELOPE_DRAWS: (
            constants.Keys.ENVELOPE_DRAWS
        ),
        constants.MortalityInputs.ENVELOPE_SUMMARY: (
            constants.Keys.ENVELOPE_SUMMARY
        ),
        constants.MortalityInputs.POPULATION: constants.Keys.POPULATION
    }

    def __init__(self, mort_process: str):
        self.mort_process = mort_process

    @property
    def get_data(self) -> Callable:
        return self._function_map[self.mort_process]

    @property
    def file_pattern(self) -> str:
        return self._file_pattern_map[self.mort_process]

    @property
    def hdf_key(self) -> str:
        return self._hdf_key_map[self.mort_process]
