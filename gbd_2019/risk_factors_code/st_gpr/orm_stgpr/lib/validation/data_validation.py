import logging
from typing import List

import numpy as np
import pandas as pd

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import columns


def validate_data_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Validates that data contains required columns"""
    required_cols = set(columns.CROSSWALK_DATA)
    data_cols = set(df.columns)
    missing_cols = required_cols - data_cols
    if missing_cols:
        raise ValueError(
            f'Data is missing required columns {missing_cols}. Required '
            f'columns are {required_cols}'
        )
    return df


def validate_no_nan_infinity(
        df: pd.DataFrame,
        columns_to_check: List[str],
        data_type: str
) -> pd.DataFrame:
    """Validates that data does not contain NaN or infinity"""
    if df[columns_to_check].isna().any(axis=1).iat[0]:
        sample_bad_row = df[df[columns_to_check].isna().any(axis=1)]\
            .iloc[0]\
            .loc[columns.DEMOGRAPHICS + columns_to_check]
        raise ValueError(
            f'Found illegal NaN in {data_type}. Example bad row of data: '
            f'{sample_bad_row.to_dict()}'
        )
    if np.isinf(df[columns_to_check]).any(axis=1).iat[0]:
        sample_bad_row = df[np.isinf(df[columns_to_check]).any(axis=1)]\
            .iloc[0]\
            .loc[columns.DEMOGRAPHICS + columns_to_check]
        raise ValueError(
            f'Found illegal infinity in {data_type}. Example bad row of data: '
            f'{sample_bad_row.to_dict()}'
        )
    return df


def validate_no_duplicates(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Validates that data does not contain duplicate demographics"""
    duplicates = df.duplicated(columns.DEMOGRAPHICS)
    if duplicates.any():
        duplicate_demographics = df\
            .loc[duplicates, columns.DEMOGRAPHICS]\
            .to_dict('records')
        raise ValueError(
            f'{data_type} contains duplicate rows for demographics: '
            f'{duplicate_demographics}'
        )
    return df


def validate_data_bounds_for_transformation(
        df: pd.DataFrame,
        data_column: str,
        transform: str
) -> None:
    """
    Validates data values against transform type to ensure no NaNs or
    infinities are returned
    """
    if transform == lookup_tables.TransformType.none.name:
        return

    if (df[data_column] <= 0).any():
        sample_bad_row = df[df[data_column] <= 0]\
            .iloc[0]\
            .loc[columns.DEMOGRAPHICS + [data_column]]
        raise ValueError(
            f'Unable to apply {transform} transform: after offsetting, data '
            'contains non-positive values. Example bad row of data: '
            f'{sample_bad_row.to_dict()}'
        )
    if transform == lookup_tables.TransformType.logit.name and \
            (df[data_column] >= 1).any():
        sample_bad_row = df[df[data_column] >= 1]\
            .iloc[0]\
            .loc[columns.DEMOGRAPHICS + [data_column]]
        raise ValueError(
            f'Unable to apply {transform} transform: after offsetting, data '
            'contains values greater than or equal to one. Example bad row of '
            f'data: {sample_bad_row.to_dict()}'
        )


def validate_data_demographics(
        data_df: pd.DataFrame,
        location_ids: List[int],
        year_ids: List[int],
        age_group_ids: List[int],
        sex_ids: List[int]
) -> None:
    """
    Validates that data demographics line up with requested prediction
    demographics.
    """
    _validate_demographic(data_df, columns.LOCATION_ID, location_ids, True)
    _validate_demographic(data_df, columns.YEAR_ID, year_ids, True)
    _validate_demographic(data_df, columns.AGE_GROUP_ID, age_group_ids, True)
    _validate_demographic(data_df, columns.SEX_ID, sex_ids, False)


def validate_population_matches_data(
        population_df: pd.DataFrame,
        square_df: pd.DataFrame
) -> None:
    """
    Validates that population estimate demographics match the square. A
    mismatch is possible when the population estimates are incorrect or when
    a modeler tries to run ST-GPR with demographics that are not present in the
    population estimates.
    """
    merged_df = square_df.merge(population_df, on=columns.DEMOGRAPHICS)
    if len(merged_df) != len(square_df):
        square_indices = square_df.set_index(columns.DEMOGRAPHICS).index
        merged_indices = merged_df.set_index(columns.DEMOGRAPHICS).index
        missing_rows = square_df[~square_indices.isin(merged_indices)]
        sample_missing_row = missing_rows[columns.DEMOGRAPHICS].iloc[0]
        raise ValueError(
            'There is a mismatch between the population estimate demographics '
            'and the square. The population estimates have '
            f'{len(population_df)} rows, and the square has {len(square_df)} '
            'rows. After merging population estimates with the square, there '
            f'are {len(merged_df)} rows. An example of a row that is present '
            'in the square but is missing from the population estimates is '
            f'{sample_missing_row.to_dict()}'
        )


def _validate_demographic(
        data_df: pd.DataFrame,
        demographic: str,
        expected_ids: List[int],
        warn: bool
) -> None:
    """
    Checks that demographic values specified in the config match demographic
    values in the dataset. Location, year, and age data does not necessarily
    need to match, but sex data must line up.
    """
    data_values = data_df[demographic].unique()
    expected_ids_set = set(expected_ids)
    data_values_set = set(data_values)
    if expected_ids_set != data_values_set:
        difference = expected_ids_set.symmetric_difference(data_values_set)
        differences_text = (
            f'The difference is '
            f'{", ".join([str(d) for d in sorted(difference)])}'
            if len(difference) < 20
            else f'There are {len(difference)} different values'
        )
        message = (
            f'The values for {demographic} in the data do not match the '
            f'values for {demographic} in the config. {differences_text}'
        )
        if warn:
            logging.info(message)
        else:
            raise ValueError(message)
