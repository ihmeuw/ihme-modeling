"""Helpers for reading custom covariates and custom stage 1 estimates."""
import logging
from typing import Optional

import pandas as pd

from stgpr_helpers.lib import general_utils
from stgpr_helpers.lib.constants import columns, dtype
from stgpr_helpers.lib.validation import data as data_validation


def read_custom_stage_1(path_to_custom_stage_1: Optional[str]) -> Optional[pd.DataFrame]:
    """Reads custom stage 1 model from a CSV, if present."""
    if path_to_custom_stage_1:
        return _read_custom_stage_1_csv(path_to_custom_stage_1)
    return None


def read_custom_covariates(
    path_to_custom_covariates: Optional[str],
) -> Optional[pd.DataFrame]:
    """Reads custom covariates from a CSV, if present."""
    if path_to_custom_covariates:
        return _read_custom_covariates_csv(path_to_custom_covariates)
    return None


def _read_custom_stage_1_csv(path_to_custom_stage_1: str) -> pd.DataFrame:
    """Reads custom stage 1 estimates from a CSV.

    Steps include:
    - Reading relevant columns and casting them to appropriate types.
    - Sorting the columns.
    - Checking for duplicates, NaNs, and infinities.
    - Renaming the custom stage 1 input column to line up with the database column.

    Args:
        path_to_custom_stage_1: path to CSV with custom stage 1 estimates.

    Returns:
        DataFrame of custom stage 1 estimates.

    Raises:
        RuntimeError: if required columns are missing from custom stage 1 estimates.
    """
    try:
        logging.info("Found custom stage 1")
        return (
            pd.read_csv(
                path_to_custom_stage_1,
                usecols=columns.DEMOGRAPHICS + [columns.CUSTOM_STAGE_1],
                dtype=dtype.DEMOGRAPHICS,
            )
            .pipe(general_utils.sort_columns)
            .pipe(lambda df: data_validation.validate_no_duplicates(df, "custom stage 1"))
            .pipe(
                lambda df: data_validation.validate_no_nan_infinity(
                    df, "custom stage 1", [*columns.DEMOGRAPHICS, columns.CUSTOM_STAGE_1]
                )
            )
            .rename(columns={columns.CUSTOM_STAGE_1: columns.CUSTOM_STAGE_1_VALUE})
        )
    except ValueError as error:
        # Calling pd.read_csv with the usecols argument will raise a ValueError
        # on missing columns. It's an informative error, but we still want to
        # catch it and provide some context before re-raising.
        if "Usecols do not match columns" in error.args[0]:
            raise RuntimeError(
                f"Could not read custom stage 1 from {path_to_custom_stage_1}"
            ) from error
        raise


def _read_custom_covariates_csv(path_to_custom_covariates: str) -> pd.DataFrame:
    """Reads custom covariate estimates from a CSV.

    Steps include:
    - Reading relevant columns and casting them to appropriate types.
    - Sorting the columns.
    - Checking for duplicates, NaNs, and infinities.

    Raises:
        RuntimeError: if required columns are missing from custom covariate estimates.
    """
    custom_covs_df = pd.read_csv(path_to_custom_covariates, dtype=dtype.DEMOGRAPHICS)
    missing_demo_cols = set(columns.DEMOGRAPHICS) - set(custom_covs_df.columns)
    if missing_demo_cols:
        raise RuntimeError(
            f"Custom covariates data is missing the following columns: {missing_demo_cols}"
        )
    cov_cols = [col for col in custom_covs_df if columns.CV_PREFIX in col]
    if not cov_cols:
        raise RuntimeError(
            "Did not find any custom covariate columns in the data at "
            f"{path_to_custom_covariates}. Note that custom covariate columns must start "
            f"with {columns.CV_PREFIX} Found the following columns: "
            f"{custom_covs_df.columns.tolist()}"
        )
    logging.info(f'Found custom covariates: {",".join(cov_cols)}')
    return (
        custom_covs_df.loc[:, columns.DEMOGRAPHICS + cov_cols]
        .pipe(general_utils.sort_columns)
        .pipe(lambda df: data_validation.validate_no_duplicates(df, "custom covariates"))
        .pipe(
            lambda df: data_validation.validate_no_nan_infinity(
                df, "custom covariates", columns.DEMOGRAPHICS + cov_cols
            )
        )
    )
