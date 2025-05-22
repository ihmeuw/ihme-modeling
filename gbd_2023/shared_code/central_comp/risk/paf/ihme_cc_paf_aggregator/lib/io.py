import os
import pathlib
import re
from typing import List, Tuple

import pandas as pd

import db_queries
from get_draws import api
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from ihme_cc_paf_aggregator.lib import constants, logging_utils, types

logger = logging_utils.module_logger(__name__)


def draw_df_from_dir(draw_dir: types.PathOrStr) -> pd.DataFrame:
    """Reads one draw file and returns the contents."""
    draw_file_regex = 
    draw_files = (
        f for f in pathlib.Path(draw_dir).iterdir() if re.match(draw_file_regex, f.name)
    )
    df = pd.read_csv(next(draw_files))
    return df


def read_total_pafs(
    input_models_df: pd.DataFrame,
    location_id: int,
    year_ids: List[int],
    release_id: int,
    n_draws: int,
) -> pd.DataFrame:
    """Read draws for a list of total PAF input models for a given location and years.

    Arguments:
        input_models_df: maps rei_id to model_version_id
        location_id: location to retrieve draws for
        year_ids: years to retrieve draws for
        release_id: release the models are associated with
        n_draws: how many draws to read

    Returns:
        dataframe of concatenated draws
    """
    return _read_pafs("paf", input_models_df, location_id, year_ids, release_id, n_draws)


def read_unmediated_pafs(
    input_models_df: pd.DataFrame,
    location_id: int,
    year_ids: List[int],
    release_id: int,
    n_draws: int,
) -> pd.DataFrame:
    """Read draws for a list of unmediated PAF input models for a given location and years.

    Arguments:
        input_models_df: maps rei_id to model_version_id
        location_id: location to retrieve draws for
        year_ids: years to retrieve draws for
        release_id: release the models are associated with
        n_draws: how many draws to read

    Returns:
        dataframe of concatenated draws
    """
    if len(input_models_df.query("draw_type == 'paf_unmediated'")) == 0:
        return pd.DataFrame()
    return _read_pafs(
        "paf_unmediated", input_models_df, location_id, year_ids, release_id, n_draws
    )


def _read_pafs(
    draw_type: str,
    input_models_df: pd.DataFrame,
    location_id: int,
    year_ids: List[int],
    release_id: int,
    n_draws: int,
) -> pd.DataFrame:
    """Read draws for a list of PAF input models for a given location and years.

    Ages groups are restricted to most-detailed ages for the given release.

    Arguments:
        draw_type: which type of PAFs are these?
        input_models_df: maps rei_id to model_version_id
        location_id: location to retrieve draws for
        year_ids: years to retrieve draws for
        release_id: release the models are associated with
        n_draws: how many draws to read

    Returns:
        dataframe of concatenated draws
    """
    demo = db_queries.get_demographics("epi", release_id=release_id)
    input_models_df = input_models_df[input_models_df.draw_type == draw_type]

    logger.info(f"Reading draws for {draw_type} models")
    paf_dfs = []
    for rei_id, model_version_id in zip(
        input_models_df.rei_id.tolist(), input_models_df.model_version_id.tolist()
    ):
        logger.info(f"Reading REI ID {rei_id}, model version ID {model_version_id}")
        paf_df = api.get_draws(
            constants.REI_ID,
            rei_id,
            source=draw_type,
            version_id=model_version_id,
            location_id=location_id,
            year_id=year_ids,
            age_group_id=demo["age_group_id"],
            sex_id=demo["sex_id"],
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
        )
        # Cap alcohol PAFs at 0.9999 for non-PAFs-of-one causes to avoid problems in the SEV
        # Calculator. Since PAFs of one are appended separately (even if they also occur in
        # the get_draws results), we can simply apply the cap to all causes here.
        if rei_id == constants.ALCOHOL_REI_ID:
            draw_cols = [f"draw_{i}" for i in range(n_draws)]
            paf_df[draw_cols] = paf_df[draw_cols].clip(upper=constants.PAFS_LESS_THAN_ONE_CAP)
        paf_dfs.append(paf_df)
    return pd.concat(paf_dfs).drop(
        [
            constants.METRIC_ID,
            constants.MODEL_VERSION_ID,
            constants.MODELABLE_ENTITY_ID,
            constants.WORMHOLE_MODEL_VERSION_ID,
        ],
        axis=1,
        errors="ignore",
    )


def write_output_for_location(
    df: pd.DataFrame, location_id: int, year_ids: List[int], output_directory: types.PathOrStr
) -> None:
    """Write aggregated draws to disk.

    Output files are saved by location/year as gzipped csvs.

    Arguments:
        df: draws to write to disk
        location_id: location_id of draws
        year_ids: years of draws
        output_directory: where to save output file
    """
    locs_in_draws = set(df[constants.LOCATION_ID])
    if set([location_id]) != locs_in_draws:
        raise RuntimeError(
            f"Tried to write data for location {location_id} but found {locs_in_draws} "
            "in draws"
        )

    if df.duplicated().any():
        raise RuntimeError(
            f"Tried to write data for location {location_id} but found "
            "duplicate rows in draws"
        )

    for year_id in year_ids:
        subdf = df[df[constants.YEAR_ID] == year_id]
        filename = constants.OUTPUT_DRAW_PATTERN.format(
            location_id=location_id, year_id=year_id
        )
        full_path = 
        logger.info(f"Writing results to {str(full_path)}")
        subdf.to_csv(full_path, index=False)


def directory_from_paf_compile_version_id(
    paf_compile_version_id: int, test: bool
) -> pathlib.Path:
    """Return results directory corresponding to an existing PAF compile version."""
    if test:
        return 
    else:
        return 


def create_new_output_directory(output_version_id: int, test: bool) -> pathlib.Path:
    """Returns a path to the new output directory (creating it, if it doesn't eixst).

    Arguments:
        output_version_id: An integer indicating the unique version, incorporated into
            the output dir path.
        test: If true, a test subdir of the PAF_ROOT will be used.

    """
    output_dir = directory_from_paf_compile_version_id(output_version_id, test)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # write draw storage metadata
    storage_metadata = ModelStorageMetadata.from_dict(
        {"storage_pattern": constants.OUTPUT_DRAW_PATTERN}
    )
    storage_metadata.to_file(directory=output_dir)

    return output_dir


def read_paf_model_n_draws_and_years(model_version_id: int) -> Tuple[int, List[int]]:
    """Read PAF model version metadata directly records on the file system.

    PAF model metadata is expected to be saved like:

    Metadata contains:
        * number of draws
        * years for existing results

    Returns:
        dictionary of metadata.
    """
    draw_dir = 
    metadata_dir = 

    # Extract number of draws from directory name
    inner_draw_dir = 
    n_draws = 

    # Extract years from file names
    year_id = 

    return (n_draws, year_id)
