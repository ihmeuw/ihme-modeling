"""Data validations: columns are present, no NaNs, etc."""

import logging
import os
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd
from pandas.api import types

import stgpr_schema

from stgpr_helpers.lib.constants import columns, exceptions


def validate_path_to_data_location(path_to_data: Optional[str]) -> None:
    """Checks that `path_to_data` exists."""
    if path_to_data and not os.path.exists(path_to_data):
        raise exceptions.InvalidPathToData(
            f"File {path_to_data} does not exist or is not readable"
        )


def validate_columns_exist(
    df: pd.DataFrame, entity: str, required_columns: Set[str]
) -> pd.DataFrame:
    """Validates that data contains required columns."""
    data_cols = set(df.columns)
    missing_cols = required_columns - data_cols
    if missing_cols:
        raise ValueError(
            f"{entity} are missing required columns {missing_cols}. Required columns are "
            f"{required_columns}"
        )
    return df


def validate_squareness(
    square_df: pd.DataFrame,
    df_to_check: pd.DataFrame,
    entity: str,
    cols: Optional[List[str]] = None,
) -> None:
    """Validates that data is square.

    Args:
        square_df: a square DataFrame of demographics columns.
        df_to_check: a DataFrame with at least the demographics columns.
        entity: the name of the thing being validated, e.g. "custom inputs".
        cols: if specified, validates squareness using these columns instead of standard
            demographics columns.

    Raises:
        ValueError: if the passed DataFrame is not square.
    """
    squareness_columns = cols or columns.DEMOGRAPHICS
    merged_df = square_df.merge(df_to_check, on=squareness_columns)
    if len(merged_df) != len(square_df):
        square_indices = square_df.set_index(squareness_columns).index
        merged_indices = merged_df.set_index(squareness_columns).index
        missing_rows = square_df[~square_indices.isin(merged_indices)]
        sample_missing_row = missing_rows[squareness_columns].iloc[0]
        raise ValueError(
            f"{entity} are not square: your {entity} have {len(df_to_check)} rows, and the "
            f"square has {len(square_df)} rows. After merging {entity} with the square, "
            f"there are {len(merged_df)} rows. An example of a row that is present in the "
            f"square but is missing from your {entity} is {sample_missing_row.to_dict()}"
        )


def validate_no_nan_infinity(
    df: pd.DataFrame, data_type: str, columns_to_check: List[str]
) -> pd.DataFrame:
    """Validates that data does not contain NaN or infinity."""
    if df[columns_to_check].isna().any(axis=1).iat[0]:
        sample_bad_row = (
            df[df[columns_to_check].isna().any(axis=1)].iloc[0].loc[columns_to_check]
        )
        raise ValueError(
            f"Found illegal NaN in {data_type}. Example bad row of data: "
            f"{sample_bad_row.to_dict()}"
        )
    for col in columns_to_check:
        if not types.is_numeric_dtype(df[col]):
            continue
        infinity_rows = df[np.isinf(df[col])]
        if not infinity_rows.empty:
            raise ValueError(
                f"Found illegal infinity in {data_type}. Example bad row of data: "
                f"{infinity_rows.iloc[0].to_dict()}"
            )
    return df


def validate_no_duplicates(
    df: pd.DataFrame, data_type: str, columns_to_check: Optional[List[str]] = None
) -> pd.DataFrame:
    """Validates that data does not contain duplicates."""
    columns_to_check = columns_to_check or columns.DEMOGRAPHICS
    duplicates = df.duplicated(columns_to_check)
    if duplicates.any():
        duplicate_columns = df.loc[duplicates, columns_to_check].iloc[:1].to_dict("records")
        raise ValueError(
            f"{data_type} contains duplicate rows for columns: {duplicate_columns}"
        )
    return df


def validate_at_least_one_column_present(
    df: pd.DataFrame, entity: str, columns: List[str]
) -> pd.DataFrame:
    """Checks that at least one of passed columns is not null in each row."""
    bad_rows = df[df[columns].isna().all(axis=1)]
    if not bad_rows.empty:
        sample_bad_row = bad_rows.iloc[0].to_dict()
        raise ValueError(
            f"{entity} contain at least one row missing all of {columns}. Sample bad row: "
            f"{sample_bad_row}"
        )
    return df


def validate_data_bounds_for_transformation(
    df: pd.DataFrame, data_column: str, transform: str
) -> None:
    """Validates data against transform type."""
    if transform == stgpr_schema.TransformType.none.name:
        return

    if (df[data_column] <= 0).any():
        sample_bad_row = (
            df[df[data_column] <= 0].iloc[0].loc[columns.DEMOGRAPHICS + [data_column]]
        )
        raise ValueError(
            f"Unable to apply {transform} transform: after offsetting, data contains "
            f"non-positive values. Example bad row of data: {sample_bad_row.to_dict()}"
        )
    if transform == stgpr_schema.TransformType.logit.name and (df[data_column] >= 1).any():
        sample_bad_row = (
            df[df[data_column] >= 1].iloc[0].loc[columns.DEMOGRAPHICS + [data_column]]
        )
        raise ValueError(
            f"Unable to apply {transform} transform: after offsetting, data contains values "
            "greater than or equal to one. Example bad row of data: "
            f"{sample_bad_row.to_dict()}"
        )


def validate_population_matches_data(
    population_df: pd.DataFrame, square_df: pd.DataFrame
) -> None:
    """Validates that population estimate demographics match the square.

    A mismatch is possible when the population estimates are incorrect or when a modeler tries
    to run ST-GPR with demographics that are not present in the population estimates.
    """
    merged_df = square_df.merge(population_df, on=columns.DEMOGRAPHICS)
    if len(merged_df) != len(square_df):
        square_indices = square_df.set_index(columns.DEMOGRAPHICS).index
        merged_indices = merged_df.set_index(columns.DEMOGRAPHICS).index
        missing_rows = square_df[~square_indices.isin(merged_indices)]
        sample_missing_row = missing_rows[columns.DEMOGRAPHICS].iloc[0]
        raise ValueError(
            "There is a mismatch between the population estimate demographics and the "
            f"square. The population estimates have {len(population_df)} rows, and the "
            f"square has {len(square_df)} rows. After merging population estimates with the "
            f"square, there are {len(merged_df)} rows. An example of a row that is present "
            "in the square but is missing from the population estimates is "
            f"{sample_missing_row.to_dict()}"
        )


def validate_demographic_filter_types(filters: Dict[str, Optional[List[int]]]) -> None:
    """Validates that demographic filters are actually integers."""
    for filter_name, filter_ids in filters.items():
        if not filter_ids:
            continue

        for filter_id in filter_ids:
            if not isinstance(filter_id, int):
                raise exceptions.SqlSanitationError(
                    f"Filter {filter_name} contains non-integer value {filter_id} with type "
                    f"{type(filter_id)}"
                )


def validate_data_demographics(
    data_df: pd.DataFrame,
    location_ids: List[int],
    year_ids: List[int],
    age_group_ids: List[int],
    sex_ids: List[int],
) -> None:
    """Validates that data demographics line up with requested prediction demographics."""
    _validate_demographic(data_df, columns.LOCATION_ID, location_ids, True)
    _validate_demographic(data_df, columns.YEAR_ID, year_ids, True)
    _validate_demographic(data_df, columns.AGE_GROUP_ID, age_group_ids, True)
    _validate_demographic(data_df, columns.SEX_ID, sex_ids, False)


def _validate_demographic(
    data_df: pd.DataFrame, demographic: str, expected_ids: List[int], warn: bool
) -> None:
    """Checks that config demographic values match data demographic values.

    Location, year, and age data does not need to match, but sex data must line up.
    NOTE: I think this is wrong. But ST-GPR has been doing this for ages, and changing it
    now would reject datasets that were accepted in previous GBD releases.
    """
    data_values = data_df[demographic].unique()
    expected_ids_set = set(expected_ids)
    data_values_set = set(data_values)
    if expected_ids_set != data_values_set:
        difference = expected_ids_set.symmetric_difference(data_values_set)
        differences_text = (
            f"The difference is " f'{", ".join([str(d) for d in sorted(difference)])}'
            if len(difference) < 20
            else f"There are {len(difference)} different values"
        )
        message = (
            f"The values for {demographic} in the data do not match the "
            f"values for {demographic} in the config. {differences_text}"
        )
        if warn:
            # Info instead of warn because typically this is something we want to make folks
            # aware of and warn could be a problem, but it's so frequent that warnings have
            # just caused confusion in the past.
            logging.info(message)
        else:
            raise ValueError(message)


def validate_minimum_required_data_points(
    df: pd.DataFrame, location_hierarchy_df: pd.DataFrame
) -> None:
    """Validates data has minimum for number of data points (10) for at least one level.

    NSV calculation requires at least one location level with at least 10 datapoints.
    Models with less data would fail at that point.
    """
    data_by_level = (
        df.merge(
            location_hierarchy_df[[columns.LOCATION_ID, columns.LEVEL]],
            on=columns.LOCATION_ID,
        )
        .assign(data_points=1)[[columns.LEVEL, "data_points"]]
        .groupby([columns.LEVEL])
        .count()
        .reset_index()
    )

    if data_by_level[data_by_level["data_points"] >= 10].empty:
        raise ValueError(
            "Not enough data to run ST-GPR. Data is required to contain at least 10 data "
            "points for a minimum of one location level. Data points per level:\n"
            f"{data_by_level}"
        )


def validate_prediction_and_aggregation_locations(
    prediction_location_ids: List[int], aggregation_location_ids: List[int]
) -> None:
    """Validates aggregation_location_ids and prediction_location_ids have the same IDs.

    Population is pulled for prediction_location_ids, but aggregation and raking use a
    hierarchy composed of aggregation_location_ids. This validation ensures a model will fail
    during prep if the locations do not match instead of at a later step.
    """
    if set(prediction_location_ids) != set(aggregation_location_ids):
        raise ValueError(
            "The locations hierarchy used in modeling does not match the location hierarchy "
            "used for raking and aggregation! If you are seeing this error, please file a "
            "ticket with Central Computation."
        )
