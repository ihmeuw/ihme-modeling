"""Helpers for working with location metadata."""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import orm

import db_stgpr
from db_queries.api import internal as db_queries_internal
from gbd import constants as gbd_constants

from stgpr_helpers.lib.constants import columns, location

# Dict used to map location set IDs to alternate prediction location sets to run on.
# For certain release/location set combinations, ST-GPR uses a different location set for
# modeling vs aggregation. This dict stores location sets that are used for modeling, while
# the location set keys are used for aggregation steps. If the release/location set combination
# are not keys to this dict, the passed location set is used for both modeling and
# aggregation
_ALTERNATE_PREDICTION_LOCATION_SET_IDS: Dict[int, Dict[int, int]] = {
    gbd_constants.release.GBD_2024: {35: 150}
}


def get_location_set_version_id(
    location_set_id: int, release_id: int, session: orm.Session
) -> Optional[int]:
    """Pulls the active location set version for a location set."""
    location_set_version_id = db_stgpr.get_active_location_set_version(
        location_set_id, session, release_id=release_id
    )
    if not location_set_version_id:
        standard = "standard " if location_set_id == location.STANDARD_LOCATION_SET_ID else ""
        raise ValueError(
            f"There is no active location set version for {standard}location set "
            f"{location_set_id}, release {release_id}"
        )
    return location_set_version_id


def get_prediction_location_set_version_id(
    location_set_id: int, release_id: int, session: orm.Session
) -> Optional[int]:
    """Pulls the prediction location set version for a location set.

    If location_set_id and release_id are keys to _ALTERNATE_PREDICTION_LOCATION_SET_IDS,
    then ST-GPR will run on a different location set ID than was passed for modeling steps,
    but will use the passed location_set_id for aggregation steps. For GBD 2024, French
    overseas territories are aggregated/raked to France, but during modeling are a level under
    the regions they are physically a part of.
    """
    prediction_location_set_id = location_set_id
    release_alternate_location_set_id_dict = _ALTERNATE_PREDICTION_LOCATION_SET_IDS.get(
        release_id
    )
    if release_alternate_location_set_id_dict:
        if release_alternate_location_set_id_dict.get(location_set_id):
            prediction_location_set_id = release_alternate_location_set_id_dict.get(
                location_set_id
            )
            logging.info(
                f"For release {release_id}, ST-GPR uses an alternate location set ID for "
                f"modeling and uses location_set {location_set_id} only for model aggregation "
                f"steps. Setting the location set ID for modeling steps to "
                f"{prediction_location_set_id}."
            )

    return get_location_set_version_id(
        location_set_id=prediction_location_set_id, release_id=release_id, session=session
    )


def get_locations(
    prediction_location_set_version_id: int,
    standard_location_set_version_id: Optional[int],
    session: orm.Session,
) -> Tuple[pd.DataFrame, List[int]]:
    """Pulls full location hierarchy and locations IDs needed for an ST-GPR model.

    Returns:
        Tuple of location hierarchy, IDs of locations at the national level or more specific.
    """
    # Get location hierarchy.
    standard_locations = set()
    if standard_location_set_version_id:
        standard_locations = set(
            db_queries_internal.get_location_hierarchy_by_version(
                standard_location_set_version_id, session
            )[columns.LOCATION_ID]
            .unique()
            .tolist()
        )
    location_hierarchy_df = (
        db_queries_internal.get_location_hierarchy_by_version(
            prediction_location_set_version_id, session
        )
        .pipe(_add_levels_to_location_hierarchy)
        .assign(
            **{
                columns.STANDARD_LOCATION: lambda df: df[columns.LOCATION_ID].isin(
                    standard_locations
                )
            }
        )
    )

    # Get list of locations at or above national level.
    location_ids = location_hierarchy_df[
        location_hierarchy_df.level >= location.NATIONAL_LEVEL
    ][columns.LOCATION_ID].tolist()
    if not location_ids:
        raise ValueError(
            f"Location set version {prediction_location_set_version_id} does not contain any "
            "locations at or above (more specific than) national level"
        )

    # Filter to relevant location columns.
    keep_cols = columns.LOCATION + [
        col for col in location_hierarchy_df.columns if "level" in col
    ]
    return location_hierarchy_df[keep_cols], location_ids


def _add_levels_to_location_hierarchy(hierarchy_df: pd.DataFrame) -> pd.DataFrame:
    """Adds level_{i} columns to location hierarchy dataframe."""
    max_level = hierarchy_df[columns.LEVEL].max()
    paths_to_parent = hierarchy_df[columns.PATH_TO_TOP_PARENT].apply(
        lambda col: pd.Series(col.split(","), dtype=float)
    )
    levels = paths_to_parent.rename(
        columns=dict(
            zip(paths_to_parent.columns, [f"level_{level}" for level in range(max_level + 1)])
        )
    )
    return pd.concat([hierarchy_df, levels], axis=1, sort=False)
