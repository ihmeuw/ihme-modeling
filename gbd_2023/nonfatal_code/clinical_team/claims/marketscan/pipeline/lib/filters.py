"""
Logic for removing rows of data that that don't fit the requirements for a bundle
"""
from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from crosscutting_functions.mapping import clinical_mapping
from db_queries import get_location_metadata
from gbd.constants import measures
from loguru import logger


def remove_inc_facilities(df: pd.DataFrame) -> pd.DataFrame:
    """Removes pharma and lab facility types for incidence bundles.

    This matches some processing from the existing Marketscan pipeline
    """

    # validate measure_id
    if not (df["measure_id"] == measures.INCIDENCE).all():
        raise RuntimeError(
            "Non-incidence measures are in the data. This filter should only "
            "be applied to incidence data"
        )

    df = df[~df["facility_id"].isin(constants.INCIDENCE_FACILITY_REMOVALS)]

    return df


def apply_clinical_age_sex_restrictions(
    df: pd.DataFrame,
    map_version: int,
    clinical_age_group_set_id: int,
    break_if_not_contig: bool,
) -> pd.DataFrame:
    """Applies bundle-level age and sex restrictions to the Marketscan data using a shared
    method from clinical mapping
    """
    pre = len(df)
    df = clinical_mapping.apply_restrictions(
        df,
        age_set="indv",
        cause_type="bundle",
        clinical_age_group_set_id=clinical_age_group_set_id,
        map_version=map_version,
        prod=True,
        break_if_not_contig=break_if_not_contig,
    )

    if pre < len(df):
        raise RuntimeError(
            f"{len(df)-pre} Rows of data have been added after this filter. "
            "This is unexpected behavior"
        )
    logger.info(f"{pre-len(df)} rows were removed while applying age-sex restrictions")
    return df


def get_us_location_ids(release_id: int) -> List[int]:
    """The Marketscan GBD estimates should only contain the location_ids which are directly
    under the US (id 102) parent."""
    loc_meta = get_location_metadata(location_set_id=9, release_id=release_id)
    return loc_meta[loc_meta.parent_id == constants.PARENT_LOCATION_ID]["location_id"].tolist()


def retain_us_child_locations(df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """Uses the list of US child location_ids to remove rows"""
    return df[df["location_id"].isin(get_us_location_ids(release_id))]