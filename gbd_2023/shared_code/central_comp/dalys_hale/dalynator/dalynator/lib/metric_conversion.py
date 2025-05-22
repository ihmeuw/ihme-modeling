from typing import List

import pandas as pd

import gbd.constants as gbd
from transforms.transforms import DEFAULT_MERGE_COLUMNS, transform_metric

from dalynator.lib.utils import get_index_draw_columns


def _get_number_space_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Validates has a metric column and number values, returns rows with number metric.

    Args:
        df: Any dataframe

    Returns:
       number_df: The rows of the original dataframe that were in number-space.

    Raises:
        ValueError: if there is not a metric column
        ValueError: if there is no number space entries

    """
    index_cols, _ = get_index_draw_columns(df)

    if gbd.columns.METRIC_ID not in index_cols:
        raise ValueError(
            f"Can't convert from number space without a column {gbd.columns.METRIC_ID}. "
            f"Non-draw columns provided are {index_cols}."
        )

    if gbd.metrics.NUMBER not in df[gbd.columns.METRIC_ID].unique():
        raise ValueError(
            "Can't convert from number space unless at least one entry has "
            f"{gbd.columns.METRIC_ID} = {gbd.metrics.NUMBER}."
        )

    number_df = df.loc[df[gbd.columns.METRIC_ID] == gbd.metrics.NUMBER].copy(deep=True)

    return number_df


def convert_number_to_percent(df: pd.DataFrame, include_pre_df: bool) -> pd.DataFrame:
    """Convert DataFrame in number-space to pct-of-total-cause space.

    Args:
        df: the input DataFrame. This must contain the cause_id column
            and at least one entry musy be for ALL_CAUSE
        include_pre_df: includes original dataframe rows which were not
            in percent-space (for example, number or rate space)

    Returns:
        new_df: A dataframe where rows which had a matching demographic
            with an ALL_CAUSE row have been converted to pct. Unmatched rows are
            dropped.

    Raises:
        ValueError: if cause_id column not present
        ValueError: if no ALL_CAUSE entries exist

    """
    index_cols, value_cols = get_index_draw_columns(df)
    number_df = _get_number_space_rows(df)

    if gbd.columns.CAUSE_ID not in index_cols:
        raise ValueError(
            "Can't convert to percent in cause space without "
            f"column {gbd.columns.CAUSE_ID}. Non-draw columns provided "
            f"are {index_cols}."
        )

    if gbd.cause.ALL_CAUSE not in df[gbd.columns.CAUSE_ID].unique():
        raise ValueError(
            "Can't convert to percent in cause space without "
            f"at least one entry with {gbd.columns.CAUSE_ID} "
            f"having the all-cause value {gbd.cause.ALL_CAUSE}."
        )

    merge_cols = [i for i in index_cols if i != gbd.columns.CAUSE_ID]

    # convert to percent of all-cause, with 0-fill if NA from divide-by-zero
    all_cause_df = number_df.query(f"{gbd.columns.CAUSE_ID} == {gbd.cause.ALL_CAUSE}")
    pct_df = number_df.merge(all_cause_df, on=merge_cols, suffixes=("", "_envelope"))
    envelope_cols = [f"{col}_envelope" for col in value_cols]
    pct_df[value_cols] = pct_df[value_cols].values / pct_df[envelope_cols].values
    pct_df[value_cols] = pct_df[value_cols].fillna(0)
    pct_df[gbd.columns.METRIC_ID] = gbd.metrics.PERCENT
    new_df = pct_df[index_cols + value_cols]

    if include_pre_df:
        # retain non-pct entries from the input, i.e. number or rate
        df = df.query(f"{gbd.columns.METRIC_ID} != {gbd.metrics.PERCENT}")
        new_df = pd.concat([new_df, df])

    return new_df


def convert_number_to_rate(
    df: pd.DataFrame,
    pop_df: pd.DataFrame,
    include_pre_df: bool,
    merge_columns: List[str] = DEFAULT_MERGE_COLUMNS,
) -> pd.DataFrame:
    """Takes a number-space DataFrame and converts it to rate space.

    Args:
        df: An input dataframe with metric_id column and some values in NUMBER space.
        pop_df: A population DataFrame, as described in
            transforms.transforms.transform_metric():  Needs
            columns "location_id", "year_id", "age_group_id", "sex_id", and
            either "population" or "pop_scaled".
        include_pre_df: includes any original non-rate space entries, and
            any AGE_STANDARDIZED entries
        merge_columns: The columns on which to merge the data and the population
            data frames

    Returns:
        new_df: a dataframe with converted draw_column values, updated metric
            columns, and, optionally, the original number-space entries.

    Raises:
        AssertionError: if the pop_df does not reflect all demographics in df when converting
            to rate space.

    """
    number_df = _get_number_space_rows(df)

    # Convert data to rate space
    new_df = transform_metric(
        df=number_df,
        from_id=gbd.metrics.NUMBER,
        to_id=gbd.metrics.RATE,
        pop_df=pop_df,
        merge_columns=merge_columns,
    )
    if len(new_df) != len(number_df):
        raise AssertionError(
            "Rows before and after population merge do not match: "
            f"before {len(number_df)}, after {len(new_df)}. Not all "
            "demographic entries must have had matching entries "
            "in the population dataframe."
        )

    if include_pre_df:
        # retain non-rate and age-standardized entries in the input
        df = df.loc[
            (df[gbd.columns.AGE_GROUP_ID] == gbd.age.AGE_STANDARDIZED)
            | (df[gbd.columns.METRIC_ID] != gbd.metrics.RATE)
        ]
        new_df = pd.concat([new_df, df])

    return new_df
