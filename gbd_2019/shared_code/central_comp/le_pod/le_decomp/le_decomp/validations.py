import pandas as pd
import numpy as np
from typing import List, Union, Dict

from gbd_outputs_versions.db import DBEnvironment

from le_decomp.constants import ErrorChecking


def no_dups(df: pd.DataFrame, columns: List[str]):
    if df[columns].duplicated().any():
        raise ValueError(
            f"Duplicated rows in dataframe for columns {columns}")


def validate_year_lists(start_years: List[int], end_years: List[int]):
    """
    Validates that two lists of years a) have the same length and
    b) start_years[i] < end_years[i] for each valid value of i.
    """
    if len(start_years) != len(end_years):
        raise ValueError(f"'start_years' must be the same length as "
                         f"'end_years'. Given: start_years={start_years} "
                         f"end_years={end_years}")
    failed_index = []
    for ind in range(len(start_years)):
        if start_years[ind] >= end_years[ind]:
            failed_index.append(ind)
    if failed_index:
        raise ValueError(f"end_years[i] must be strictly greater "
                         f"the start_years[i] for every index i. Failed for "
                         f"indexes {failed_index}")


def validate_year_ids(year_ids: List[int]):
    if len(year_ids) < 2:
        raise ValueError(f"You need at least two years to perform "
                         f"life expectancy decomposition. Given: "
                         f"year_ids={year_ids}")
    if len(year_ids) != len(set(year_ids)):
        raise ValueError(f"When passing 'year_ids' to generate all "
                         f"combinations of year pairs for decomposition "
                         f"each year id should be unique. Given: "
                         f"year_ids={year_ids}")


def validate_cause_level(cause_level: Union[int, str],
                         cause_metadata_df: pd.DataFrame):

    valid_cause_levels = (
        ['most_detailed'] + cause_metadata_df['level'].unique().tolist())
    if cause_level not in valid_cause_levels:
        raise ValueError(f"Valid values for cause_level are "
                         f"'most_detailed' or one of "
                         f"{valid_cause_levels}")


def validate_env(env: str):
    """
    Validates that 'env' is one of 'prod' or 'dev', and returns
    an Enum from gbd_outputs_versions.DBEnvironment so that we
    can pass it back to the gbd_outputs_versions as an argument
    it won't reject.
    """
    if env == 'prod':
        return DBEnvironment.PROD
    elif env == 'dev':
        return DBEnvironment.DEV
    else:
        raise ValueError(f"env must be one of 'prod' or 'dev'. "
                         f"Passed: {env}")


def check_decomp_result(decomp_df: pd.DataFrame,
                        life_ex_at_birth: Dict[int, float],
                        year_start: int,
                        year_end: int,
                        sum_column: str,
                        location_id: int,
                        sex_id: int):
    """
    Check that the sum of the series decomp_df[sum_column] is
    equal to the difference between life expectancy at birth
    at year_end and life expectancy at birth at year_start.
    The location_id and sex_id arguments are only used for returning
    a descriptive error. (Maybe we should have a default?)
    """
    diff = life_ex_at_birth[year_end] - life_ex_at_birth[year_start]
    all_cause_sum = decomp_df[sum_column].sum()
    if not np.isclose(all_cause_sum, diff, atol=ErrorChecking.ERROR_TOLERANCE):
        raise LEDecompError(f"The sum of attributable differences over "
                            f"cause is not equal to difference in all cause "
                            f"life expectancy from life tables for "
                            f"location_id {location_id}, sex_id {sex_id}, "
                            f"year start = {year_start} and year end = "
                            f"{year_end}")


class LEDecompError(Exception):
    pass
