"""Helpers for working with location metadata."""
from typing import List, Optional, Tuple

import pandas as pd
from sqlalchemy import orm

import db_stgpr
from db_queries.api import internal as db_queries_internal
from gbd import decomp_step as gbd_decomp_step

from stgpr_helpers.lib.constants import columns, location


def get_location_set_version_id(
    location_set_id: int,
    gbd_round_id: int,
    decomp_step: str,
    session: orm.Session,
) -> Optional[int]:
    """Pulls the active location set version for a location set."""
    decomp_step_id = gbd_decomp_step.decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id
    )
    location_set_version_id = db_stgpr.get_active_location_set_version(
        location_set_id, gbd_round_id, decomp_step_id, session
    )
    if not location_set_version_id:
        standard = "standard " if location_set_id == location.STANDARD_LOCATION_SET_ID else ""
        raise ValueError(
            f"There is no active location set version for {standard}location set "
            f"{location_set_id}, GBD round ID {gbd_round_id}, decomp step '{decomp_step}'"
        )
    return location_set_version_id


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
