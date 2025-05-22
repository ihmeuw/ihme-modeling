"""Core functions for imported cases."""

from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

import db_queries
from gbd import estimation_years

from imported_cases.lib import constants, spacetime_restrictions


def get_spacetime_restricted_cause_ids(release_id: int) -> List[int]:
    """Return a list of the ids for spacetime restricted causes.

    Includes causes offically restricted in db codcorrect.spacetime_restriction
    as well as additional causes that are keys in constants.CAUSE_AGE_RESTRICTIONS.
    """
    cause_ids = (
        spacetime_restrictions.get_all_spacetime_restrictions(release_id)
        .cause_id.unique()
        .tolist()
    )
    return cause_ids + list(constants.CAUSE_AGE_RESTRICTIONS.keys())


def get_data_rich_locations(release_id: int) -> List[int]:
    """Return list of data rich locations from the data dense and sparse location set."""
    location_hierarchy = db_queries.get_location_metadata(
        location_set_id=constants.DENSE_AND_SPARE_LOCATION_SET_ID, release_id=release_id
    )
    loc_ids = (
        location_hierarchy[
            location_hierarchy["parent_id"] == constants.DATA_DENSE_LOCATION_ID
        ]
        .location_id.unique()
        .tolist()
    )
    return loc_ids


def get_cause_specific_locations(cause_id: int, release_id: int) -> List[int]:
    """Return a list of data-rich restricted location ids for the given cause."""
    restrictions = spacetime_restrictions.get_all_spacetime_restrictions(release_id)
    data_rich = get_data_rich_locations(release_id)

    return (
        restrictions[
            restrictions.location_id.isin(data_rich) & (restrictions.cause_id == cause_id)
        ]
        .location_id.unique()
        .tolist()
    )


def get_age_group_ids(
    release_id: int,
    age_group_id_lower: Optional[int] = None,
    age_group_id_upper: Optional[int] = None,
) -> List[int]:
    """Get all most detailed age group ids for the given round.

    Can limit to a range of age group ids with 'age_group_id_lower' and
    'age_group_id_upper'.

    Args:
        release_id: release ID
        age_group_id_lower: lower limit of age group ids to return. Inclusive
        age_group_id_upper: upper limit of age group ids to return. Inclusive

    Returns:
        list of age group ids
    """
    age_group_df = db_queries.get_age_metadata(release_id=release_id)

    # Convert age group ids into age group year start
    age_group_years_lower = (
        age_group_df.age_group_years_start.min()
        if age_group_id_lower is None
        else age_group_df.loc[
            age_group_df.age_group_id == age_group_id_lower, "age_group_years_start"
        ].iat[0]
    )
    age_group_years_upper = (
        age_group_df.age_group_years_start.max()
        if age_group_id_upper is None
        else age_group_df.loc[
            age_group_df.age_group_id == age_group_id_upper, "age_group_years_start"
        ].iat[0]
    )

    return age_group_df[
        (age_group_df.age_group_years_start >= age_group_years_lower)
        & (age_group_df.age_group_years_start <= age_group_years_upper)
    ].age_group_id.tolist()


def subset_to_restricted_location_years(data: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """Subsets the data by the location and year pulled from spacetime restrictions.

    Causes in the CAUSE_AGE_RESTRICTIONS dictionary are skipped from the merge
    step since they don't have entries in the codcorrect.spacetime_restriction
    table and merging spacetime restrictions onto their CoD data will result in
    data being dropped.

    Args:
        data: CoD data to be used for imported case models
        release_id: release ID to pull the spacetime restrictions for

    Returns:
        CoD data subsetted by location and year
    """
    # age-restricted causes aren't present in the spacetime restrictions DB
    if data.cause_id.iloc[0] in constants.CAUSE_AGE_RESTRICTIONS:
        year_restricted_data = data
    else:
        restrictions = spacetime_restrictions.get_all_spacetime_restrictions(release_id)
        year_restricted_data = pd.merge(
            data, restrictions, on=["cause_id", "location_id", "year_id"]
        )
    return year_restricted_data


def generate_distribution(data: pd.DataFrame) -> pd.DataFrame:
    """Generate a 1000-draw beta distribution for the data based on deaths and sample size.

    For each VR data point, we have restricted cause deaths (d_cause), deaths from other
    causes (d_other), and the sample size of the study (d_all). The relationship between the
    three is dc_ause + d_other = d_all. If d_cause is less than 1 or d_other is 0 or less,
    implying all the deaths from this study are from the cause of interest, we create
    1000 draws of 0.

    Otherwise, 1000 draws are taken from a beta distribution where
    d_cause and d_other are alpha and beta, respectively.
    """
    data = data.reset_index(drop=True)
    data["i"] = data.index
    temp = []
    for idx, row in data.iterrows():
        row_dict = row.to_dict()
        cf = row_dict["cf"]
        sample_size = row_dict["sample_size"]
        deaths = cf * sample_size
        other_deaths = sample_size - deaths
        if deaths < 1 or other_deaths <= 0:
            draws = np.zeros(shape=(1000,))
        else:
            draws = np.random.beta(deaths, other_deaths, 1000)
        draws = draws * sample_size
        t = pd.DataFrame([draws], columns=["draw_{}".format(x) for x in range(1000)])
        t["i"] = idx
        temp.append(t)
    temp = pd.concat(temp)
    data = pd.merge(data, temp, on="i")
    keep_cols = ["location_id", "year_id", "sex_id", "age_group_id", "cause_id"] + [
        "draw_{}".format(x) for x in range(1000)
    ]
    return data[keep_cols]


def expand_id_set(
    input_data: pd.DataFrame, eligible_ids: Iterable[int], id_name: str
) -> pd.DataFrame:
    """Duplicate input data by the number of items in eligible ids and creates new column.

    Args:
        input_data: non-empty dataframe to expand
        eligible_ids: list of all values (levels) of ids that exist
            to expand the dataframe by.
        id_name: name of the id; becomes the new columns name

    Example:
        input_data has one column, A with one value, b. eligible_ids = [1, 2, 3] and
        id_name = "sex_id". The output dataframe:
        | A | sex_id |
        | - | ------ |
        | a |    1   |
        | a |    2   |
        | a |    3   |

    Returns:
        newly expanded DataFrame
    """
    result_df = []
    for i in eligible_ids:
        temp = input_data.copy(deep=True)
        temp[id_name] = i
        result_df.append(temp)
    return pd.concat(result_df)


def make_square_data(
    cause_id: int, age_group_ids: List[int], release_id: int
) -> pd.DataFrame:
    """Create a dataframe square by demographics with 1000 draws initiated with 0s.

    Demographics include location_id, year_id, age_group_id, sex_id.

    Returns:
        square dataframe with columns: "cause_id", "location_id", "year_id",
        "age_group_id", "sex_id", and "draw_0", ..., "draw_999"
    """
    start_year = 1980
    final_year = estimation_years.estimation_years_from_release_id(release_id)[-1]
    data = pd.DataFrame(
        db_queries.get_demographics("cod", release_id=release_id)["location_id"],
        columns=["location_id"],
    )
    data = expand_id_set(data, range(start_year, final_year + 1), "year_id")
    data = expand_id_set(data, [1, 2], "sex_id")
    data = expand_id_set(data, age_group_ids, "age_group_id")
    data["cause_id"] = cause_id
    for x in range(1000):
        data["draw_{}".format(x)] = 0
    return data
