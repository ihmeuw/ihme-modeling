import dataclasses
import json
import logging
from typing import List, Tuple

import db_queries

from hale.common import age_utils, path_utils
from hale.common.constants import columns, years


@dataclasses.dataclass
class HaleMeta:
    """
    Stores metadata associated with a HALE run (i.e. everything needed to
    recreate a HALE run)

    Attributes:
        hale_version: The GBD process version associated with a HALE run
        gbd_round_id: The ID of the GBD round associated with a HALE run
        decomp_step_id: The ID of the decomp step associated with a HALE run
        location_set_version_ids: The IDs of location set versions containing
            locations associated with a HALE run
        location_ids: The IDs of locations associated with a HALE run
        year_ids: The IDs of years associated with a HALE run
        percent_change_years: mapping of years for which to calculate percent
            change
        age_group_ids: The IDs of age groups associated with a HALE run
        under_one_age_group_ids: The IDs of age groups aggregated into an
            under-one age group during a HALE run
        draws: The number of draws in life table and YLD inputs
        population_run_id: The mortality run ID used to pull populations
        life_table_run_id: The mortality run ID used to pull life tables
        como_version: The COMO version associated with YLD inputs
        conn_def: The connection definition used to connect to the GBD database
    """
    hale_version: int
    gbd_round_id: int
    decomp_step_id: int
    location_set_version_ids: List[int]
    location_ids: List[int]
    year_ids: List[int]
    percent_change_years: List[Tuple[int, int]]
    age_group_ids: List[int]
    under_one_age_group_ids: List[int]
    draws: int
    population_run_id: int
    life_table_run_id: int
    como_version: int
    conn_def: str


def load_metadata(hale_version: int) -> HaleMeta:
    """Loads metadata associated with a HALE run"""
    metadata_filepath = path_utils.get_metadata_path(hale_version)
    with open(metadata_filepath) as metadata_file:
        metadata = json.load(metadata_file)
        return HaleMeta(**metadata)


def save_metadata(
        hale_version: int,
        gbd_round_id: int,
        decomp_step_id: int,
        location_set_ids: List[int],
        year_ids: List[int],
        draws: int,
        population_run_id: int,
        life_table_run_id: int,
        como_version: int,
        conn_def: str
) -> HaleMeta:
    """Saves metadata associated with a HALE run"""
    logging.info('Pulling location metadata')
    location_set_version_ids, location_ids = _get_location_metadata(
        location_set_ids, gbd_round_id
    )
    hale_ages, under_one_ages = age_utils.get_hale_ages(gbd_round_id)

    metadata_filepath = path_utils.get_metadata_path(hale_version)
    logging.info(f'Saving metadata to {metadata_filepath}')
    # pytype: disable=wrong-keyword-args
    metadata = HaleMeta(
        hale_version=hale_version,
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id,
        location_set_version_ids=location_set_version_ids,
        location_ids=location_ids,
        year_ids=year_ids,
        percent_change_years=years.PERCENT_CHANGE_YEARS[gbd_round_id],
        age_group_ids=hale_ages,
        under_one_age_group_ids=under_one_ages,
        draws=draws,
        population_run_id=population_run_id,
        life_table_run_id=life_table_run_id,
        como_version=como_version,
        conn_def=conn_def
    )
    # pytype: enable=wrong-keyword-args
    with open(metadata_filepath, 'w') as metadata_file:
        json.dump(dataclasses.asdict(metadata), metadata_file)

    return metadata


def _get_location_metadata(
        location_set_ids: List[int],
        gbd_round_id: int
) -> Tuple[List[int], List[int]]:
    location_set_version_ids = set()
    location_ids = set()
    for location_set_id in location_set_ids:
        location_df = db_queries.get_location_metadata(
            location_set_id, gbd_round_id=gbd_round_id
        )
        location_set_version_ids.update(
            location_df[columns.LOCATION_SET_VERSION_ID].unique().tolist()
        )
        location_ids.update(location_df[columns.LOCATION_ID].tolist())

    return list(location_set_version_ids), list(location_ids)
