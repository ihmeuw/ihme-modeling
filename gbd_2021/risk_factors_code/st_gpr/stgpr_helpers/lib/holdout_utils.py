"""Functions that create holdouts (aka knockouts)."""
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from stgpr_helpers.lib.constants import columns, enums
from stgpr_helpers.lib.constants import holdouts as holdout_constants


def make_holdouts(
    square_df: pd.DataFrame,
    data_df: pd.DataFrame,
    location_hierarchy_df: pd.DataFrame,
    holdouts: int,
    random_seed: int,
    is_usa_re_model: bool,
) -> Optional[pd.DataFrame]:
    """Assigns holdouts.

    Holdout columns are "holdout_{i}" columns for i up to `holdouts`.
    Value 1 means that a point will be used in model fitting, and value 0 means that a point
    will be held out for model evaluation.

    We merge the data with the square here before generating holdouts as part of
    the holdouts algorithm. See _add_holdout for more details.

    Holdouts are unique by demographic, but we drop duplicates just to be sure.
    Also, holdouts are only taken at the national level.

    Args:
        square_df: DataFrame of square modeling demographics.
        data_df: DataFrame of data from which to generate holdouts.
        location_hierarchy_df: Location metadata to use for generating holdouts.
        holdouts: The number of holdouts to generate.
        random_seed: Seed with which to generate holdouts.
        is_usa_re_model: True iff the model is a USA R/E model, which has a different
            holdout selection strategy.

    Returns:
        DataFrame with demographic columns and holdout columns.
    """
    if not holdouts:
        return None

    input_df = square_df.merge(data_df, how="left", on=columns.DEMOGRAPHICS).merge(
        location_hierarchy_df, how="left", on=columns.LOCATION_ID
    )
    holdout_df = _add_holdouts_to_dataframe(input_df, holdouts, random_seed, is_usa_re_model)
    return holdout_df[
        columns.DEMOGRAPHICS + [col for col in holdout_df if columns.HOLDOUT_PREFIX in col]
    ].drop_duplicates(columns.DEMOGRAPHICS)


def _add_holdouts_to_dataframe(
    df: pd.DataFrame, holdouts: int, random_seed: int, is_usa_re_model: bool
) -> pd.DataFrame:
    """Adds holdout columns to a DataFrame.

    Args:
        df: DataFrame to which to add holdout columns.
        holdouts: Number of model instances for which to hold out data.
        random_seed: Seed with which to generate holdouts.
        is_usa_re_model: True iff the model is a USA R/E model.

    Returns:
        DataFrame with holdout columns added on.
    """
    if is_usa_re_model:
        # Use preselected list of locations to select holdouts from
        eligible_holdout_data = df.loc[
            df[columns.LOCATION_ID].isin(holdout_constants.USA_RE_HOLDOUT_LOCATIONS)
        ]
        non_eligible_data = df.loc[
            ~df[columns.LOCATION_ID].isin(holdout_constants.USA_RE_HOLDOUT_LOCATIONS)
        ]
    else:
        # Use data at the HOLDOUT_LOCATION_LEVEL (national) to select holdouts
        eligible_holdout_data = df.loc[
            df[columns.LEVEL] == holdout_constants.HOLDOUT_LOCATION_LEVEL
        ]
        non_eligible_data = df.loc[
            df[columns.LEVEL] > holdout_constants.HOLDOUT_LOCATION_LEVEL
        ]

    if eligible_holdout_data[eligible_holdout_data[columns.VAL].notnull()].empty:
        msg = (
            "states with at least 100,000 people in the smallest R/E group"
            if is_usa_re_model
            else f"location level {holdout_constants.HOLDOUT_LOCATION_LEVEL}"
        )
        raise RuntimeError(
            f"No data exists where holdouts are drawn from: {msg}. Cannot select holdouts."
        )

    holdouts_df = _generate_holdouts(
        eligible_holdout_data, holdouts, random_seed, holdout_constants.HOLDOUT_PROPORTION
    ).append(non_eligible_data, sort=True)

    holdout_cols = [f"{columns.HOLDOUT_PREFIX}{i}" for i in range(1, holdouts + 1)]
    holdouts_df[holdout_cols] = holdouts_df[holdout_cols].fillna(
        enums.Holdout.USED_IN_MODEL.value
    )
    return holdouts_df


def _generate_holdouts(
    df: pd.DataFrame, holdouts: int, random_seed: int, prop_held_out: float
) -> pd.DataFrame:
    """Makes DataFrames with training and testing data split out.

    Args:
        df: DataFrame to which to add holdout columns.
        holdouts: Number of model instances for which to hold out data.
        random_seed: seed with which to generate holdouts.
        prop_held_out: proportion of data to hold out for testing.

    Returns:
        DataFrame with generated holdout columns.
    """
    dataframe = df.copy().reset_index().rename(columns={"index": "idx"})
    np.random.seed(random_seed)
    pd.set_option("chained_assignment", None)
    data, no_na_data, all_location_ids, location_ids_with_data = _generate_sub_frames(
        dataframe
    )
    kos = pd.DataFrame({"idx": data["idx"]})
    for i in range(holdouts):
        np.random.shuffle(all_location_ids)
        np.random.shuffle(location_ids_with_data)
        holdout_temp = _holdout_run(
            data, no_na_data, all_location_ids, location_ids_with_data, prop_held_out, i + 1
        )
        kos = kos.merge(holdout_temp, on="idx")

    dataframe = dataframe.merge(kos, on="idx")
    dataframe.drop(columns="idx", inplace=True)

    return dataframe


def _generate_sub_frames(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[int], List[int]]:
    """Makes DataFrames of all data, non-null data, and location IDs for each.

    Returns:
        Tuple: df subsetted to index, demographics, and val; df subsetted to
        demographics where there is data; list of all location ids in df;
        list of all location ids in df with data
    """
    dt = df[["idx", *columns.DEMOGRAPHICS, columns.VAL]]
    no_na_data = dt[dt[columns.VAL].notnull()]
    all_location_ids = dt[columns.LOCATION_ID].unique().tolist()
    location_ids_with_data = no_na_data[columns.LOCATION_ID].unique().tolist()
    return dt, no_na_data, all_location_ids, location_ids_with_data


def _holdout_run(
    data: pd.DataFrame,
    no_na_data: pd.DataFrame,
    all_location_ids: List[int],
    location_ids_with_data: List[int],
    prop_held_out: float,
    holdout: int,
) -> pd.DataFrame:
    """Splits DataFrame into testing and training data.

    Algorithm:
        Build dataframe ("holdouts")
        While we haven't selected 20% of data to be held out:
            Cycle through all locations with data:
                Add a holdout (_add_holdout):
                    Match demographics where KO location (where we have data)
                    lines up with missing data for a random location
            Re-shuffle all locations list and locations w/ data list
    """
    holdouts = pd.DataFrame()
    N = len(no_na_data)
    while len(holdouts) / N < prop_held_out:
        for i in range(len(location_ids_with_data)):
            holdouts = _add_holdout(
                data,
                no_na_data,
                all_location_ids[i],
                location_ids_with_data[i],
                holdouts,
                f"{columns.HOLDOUT_PREFIX}{holdout}",
            )
        np.random.shuffle(all_location_ids)
        np.random.shuffle(location_ids_with_data)

    return _get_holdout_data_frame(data, holdouts, holdout)


def _add_holdout(
    data: pd.DataFrame,
    no_na_data: pd.DataFrame,
    random_location_id: int,
    ko_location_id: int,
    holdouts: pd.DataFrame,
    holdout_column: str,
) -> pd.DataFrame:
    """Adds new holdouts to 'holdouts' dataframe for KO location.

    Takes two random location ids, one from any location in the square data
    (RANDOM) and one location where the modeler has data (KO). Matches age/sex/year
    demographics where RANDOM location is missing data with demographics where
    KO location has data. Returns 'holdouts' with added rows of KO location data to holdout.

    Note:
        Could add 0 rows if RANDOM location has data for the same demographics
        that KO location does.

    Args:
        data: Full dataset.
        no_na_data: Dataset with data NAs removed.
        random_location_id: any location id in prediction set
        ko_location_id: location id where there is data that will be held out (knockout)
        holdouts: DataFrame to which to append a holdout column.
        holdout_column: Name of the holdout column.

    Returns:
        DataFrame with data holdouts + holdout column added (bool).
    """
    # Subset dataframes:
    #   * data -> random location id where there is NO data
    #   * no_na_data -> ko location is where there IS data
    random_missing_data = data[
        (data["location_id"] == random_location_id) & data[columns.VAL].isnull()
    ]
    ko_data = no_na_data[no_na_data["location_id"] == ko_location_id]

    # Merge two data sets together on year/age/sex, keeping KO LOCATION id.
    # Results in df with demographics where KO LOCATION has data and
    # RANDOM LOCATION does not
    ko_data = ko_data.merge(
        random_missing_data[["year_id", "age_group_id", "sex_id"]],
        how="inner",
        on=["year_id", "age_group_id", "sex_id"],
    )

    # Create holdout column, clean up and append
    ko_data[holdout_column] = enums.Holdout.HELD_OUT.value
    ko_data = ko_data[
        ["idx", "location_id", "year_id", "age_group_id", "sex_id", holdout_column]
    ]
    holdouts = holdouts.append(ko_data)
    holdouts = holdouts.drop_duplicates()
    return holdouts


def _get_holdout_data_frame(
    data: pd.DataFrame, holdouts: pd.DataFrame, holdout: int
) -> pd.DataFrame:
    """Builds final holdout DataFrame for one holdout.

    Sets holdout column to 1 (for model use) if the column is currently null
    or there is no data.

    Args:
        data: Full dataset. This was the starting point for building holdouts.
        holdouts: DataFrame with holdout column.
        holdout: Holdout number.

    Returns:
        DataFrame with data columns and holdout column.
    """
    var = f"{columns.HOLDOUT_PREFIX}{holdout}"
    partitioned = pd.merge(data, holdouts, how="left")
    partitioned[var][partitioned[var].isnull()] = enums.Holdout.USED_IN_MODEL.value
    partitioned[var][partitioned[columns.VAL].isnull()] = enums.Holdout.USED_IN_MODEL.value
    return partitioned[["idx", var]]
