"""
Helper functions that are generally useful
"""

import pandas as pd

import db_queries

from codcorrect.legacy.utils.constants import Columns


def age_group_id_to_age_start(age_group_id: int) -> float:
    """
    Converts an age group id to age group years start to allow comparison
    of age groups.

    This function anticipates being called in rapid succession, so it
    relies on age_group database table that it queries being cached.

    Raises:
        ValueError: if the age_group_id does not exist in shared

    Returns:
        age group year start of the age group id
    """
    age_metadata = db_queries.get_age_spans()
    age_start = age_metadata.loc[
        age_metadata[Columns.AGE_GROUP_ID] == age_group_id, Columns.AGE_GROUP_YEARS_START
    ]

    if age_start.empty:
        raise ValueError(f"Age group {age_group_id} is not in the age metadata.")

    return age_start.iat[0]


def add_measure_id_to_sink(df: pd.DataFrame, measure_id: int) -> pd.DataFrame:
    """Adds given measure id as a column to dataframe."""
    df[Columns.MEASURE_ID] = measure_id
    return df
