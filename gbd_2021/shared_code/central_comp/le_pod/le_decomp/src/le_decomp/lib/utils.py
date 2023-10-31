"""Utility functions for LE decomp."""
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import xarray

import db_queries

from le_decomp.legacy import validations
from le_decomp.lib.constants import AgeGroup, Demographics, LifeTable


def generate_paired_years(
    start_years: Optional[Union[List[int], int]] = None,
    end_years: Optional[Union[List[int], int]] = None,
    year_ids: Optional[Union[List[int], int]] = None,
) -> Tuple[List[int], List[int]]:
    """
    Takes either two equal length lists of start_years and end_years or a single list
    of year_ids. If two lists are passed, they are validated using
    le_decomp.validations.validate_year_lists and then returned. If year_ids
    are passed, every possible time span that can be created using those years is
    generated and returned in the form of two lists.

    Example:
        if year_ids:
            [1990, 2000, 2010, 2019],
        this function returns:
            [1990, 1990, 1990, 2000, 2000, 2010] and
            [2000, 2010, 2019, 2010, 2019, 2019]

    Arguments:
        start_years: list of start ('before') years. If given, end_years must also be passed.
            Optional, defaults to None.
        end_years: list of end ('after') years. If given, start_years must also be passed.
            Optional, defaults to None.
        year_ids: list of all years to create comparisions for for every valid year pair.
            If given, start_years and end_years must be None. Optional, defaults to None.

    Raises:
        ValueError: if an invalid pairing of start_years/end_years/year_ids is given
    """
    if start_years and end_years and not year_ids:
        start_years = list(np.atleast_1d(start_years))
        end_years = list(np.atleast_1d(end_years))
        validations.validate_year_lists(start_years, end_years)
        return start_years, end_years
    elif not start_years and not end_years and year_ids:
        validations.validate_year_ids(year_ids)
        year_ids = sorted(year_ids)
        start_years = []
        end_years = []
        for start_year_ind in range(len(year_ids) - 1):
            for end_year_ind in range(start_year_ind + 1, len(year_ids)):
                start_years.append(year_ids[start_year_ind])
                end_years.append(year_ids[end_year_ind])
        return start_years, end_years
    else:
        raise ValueError(
            f"Either both of 'start_years' and 'end_years' OR 'year_ids' can be specified."
            f"Given: start_years={start_years}, end_years={end_years}, year_ids={year_ids}"
        )


def life_exp_at_birth(life_table: pd.DataFrame) -> pd.DataFrame:
    """Retrieves life expectancy at birth (at youngest age group) from given lfie table.

    Used to make sure change in life exp at birth matches delta_x summed.

    Arguments:
        life_table: dataframe of life table values for all ages
    """
    return life_table.loc[
        life_table["age_group_id"] == AgeGroup.YOUNGEST_AGE_ID,
        [LifeTable.LIFE_EXPECTANCY_ABBR, "year_id"],
    ].set_index("year_id")


def reshape_lt_wide(data: pd.DataFrame) -> pd.DataFrame:
    """Takes a life table dataframe in the form returned by get_life_table_with_shocks
    and returns it in 'wide' format, ie with columns for each life table parameter,
    'ex', 'lx', etc.

    Arguements:
        data: life table dataframe
    """
    value_col = "mean"
    new_cols = data[LifeTable.PARAMETER_NAME].drop_duplicates()
    reshaped = []
    for i, c in enumerate(new_cols):
        temp = data.loc[(data[LifeTable.PARAMETER_NAME] == c)].copy(deep=True)
        temp = temp[Demographics.INDEX_COLUMNS + [value_col]]
        temp = temp.rename(columns={value_col: c})
        if i == 0:
            reshaped = temp.copy(deep=True)
        else:
            reshaped = pd.merge(reshaped, temp, on=Demographics.INDEX_COLUMNS, how="left")
    return reshaped


def dataframe_to_xarray(
    df: pd.DataFrame,
    index: str,
    filters: Dict[str, Any] = None,
    keep_columns: List[str] = None,
) -> xarray.DataArray:
    """Converts a dataframe to xarry.

    Arguments:
        df: pandas dataframe
        index: column to become the index
        filters: a dictionary with column names as keys and values to filter the df
            to as keys
        keep_columns: columns to maintain
    """
    temp = df.copy(deep=True)
    if filters:
        for key in filters:
            values = list(np.atleast_1d(filters[key]))
            temp = temp.loc[df[key].isin(values)]
    if keep_columns:
        keep = list(set(keep_columns + [index]))
        temp = temp[keep]
    return temp.set_index(index).to_xarray()


def get_age_group_ids(source: str, gbd_round_id: int) -> List[int]:
    """Get age group ids, specific to source: either 'outputs' (GBD) or 'life_table' (mort).

    Arguments:
        source: what are the age group ids for? Options are 'outputs' or 'life_table'
        gbd_round_id: the GBD round

    Returns:
        list of age group ids, sorted from youngest to oldest

    Raises:
        ValueError: if source is not one of the two options
    """
    if source not in ["outputs", "life_table"]:
        raise ValueError(f"'source' must be either 'outputs' or 'life_table'; got '{source}'")

    age_spans = db_queries.get_age_spans()
    youngest_age_group_years_end = age_spans.loc[
        age_spans["age_group_id"] == AgeGroup.YOUNGEST_AGE_ID, "age_group_years_end"
    ].iat[0]

    core_ages = db_queries.get_age_metadata(gbd_round_id=gbd_round_id).sort_values(
        by=["age_group_years_end"]
    )
    core_ages = core_ages[core_ages["age_group_years_end"] > youngest_age_group_years_end]

    if source == "life_table":
        core_ages = core_ages[
            core_ages["age_group_years_end"] != core_ages["age_group_years_end"].max()
        ]

        age_group_ids = (
            [AgeGroup.YOUNGEST_AGE_ID]
            + core_ages["age_group_id"].tolist()
            + [AgeGroup.OLDEST_LIFE_TABLE_AGE_ID]
        )
    elif source == "outputs":
        age_group_ids = [AgeGroup.YOUNGEST_AGE_ID] + core_ages["age_group_id"].tolist()

    return age_group_ids


def get_code_version() -> str:
    """Returns the short git hash from the last commit."""
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"])
        .decode("ascii")
        .strip()
    )
