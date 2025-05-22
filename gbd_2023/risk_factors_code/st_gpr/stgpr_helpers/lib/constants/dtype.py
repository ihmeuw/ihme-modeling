"""Specifies dtypes for pandas dataframes.

Using dtypes ensures that user data has the required types and that dataframes do not use
excess memory.
"""

from typing import Any, Dict, Final, Union

import pandas as pd

import stgpr_schema

from stgpr_helpers.lib.constants import columns, parameters


def str_to_int(val: str) -> int:
    """Casts a string to an int, ex: '22.0' -> 22.

    For whatever reason, native int() cannot cast a string to an int
    if the string is actually a float. We need this behavior because
    if a column in a config has any missingness, pandas will read the
    column in as float rather than an int and that causes problems.

    Args:
        val: The string to cast to an int.

    Returns:
        Value cast to an integer.

    Raises:
        TypeError: if the string is not a whole number.
    """
    try:
        as_float = float(val)
    except (ValueError, TypeError) as err:
        raise TypeError(f"Value {val} is not a number.") from err
    if not as_float.is_integer():
        raise TypeError(f"Value {val} is not an int.")
    return int(as_float)


def str_to_int_or_str(val: str) -> Union[int, str]:
    """Casts a string to an int or string.

    Very similiar to str_to_int, where '22.0' becomes 22 BUT conserves
    true strings, ex: 'default' -> 'default'.

    For whatever reason, native int() cannot cast a string to an int
    if the string is actually a float. We need this behavior because
    if a column in a config has any missingness, pandas will read the
    column in as float rather than an int and that causes problems.

    Args:
        val: The string to cast to an int or returned as string.

    Returns:
        Value cast to an integer.

    Raises:
        TypeError: if the string is not a whole number or string
    """
    try:
        as_float = float(val)
    except ValueError:
        # Return if a true string (not a number)
        return val
    except TypeError as err:
        # Fail if None
        raise TypeError(f"Value {val} is not a number or a string.") from err
    if not as_float.is_integer():
        raise TypeError(f"Value {val} is not an int.")
    return int(as_float)


DATA_TRANSFORM_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    [tt.name for tt in stgpr_schema.TransformType]
)
GPR_AMP_METHOD_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    [amp_method.name for amp_method in stgpr_schema.AmplitudeMethod]
)
VERSION_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    [entry.name for entry in stgpr_schema.Version]
)

# Types of parameters that can only have a single value.
# Use native Python types instead of numpy/pandas types because efficiency doesn't matter for
# reading configs, databases require native types, and some pandas types don't play nicely
# with feather.
CONFIG_SCALAR: Final[Dict[str, Any]] = {
    parameters.ADD_NSV: int,
    parameters.BUNDLE_ID: int,
    parameters.COVARIATE_ID: int,
    parameters.CROSSWALK_VERSION_ID: int,
    parameters.CUSTOM_AMPLITUDE: float,
    parameters.DATA_TRANSFORM: DATA_TRANSFORM_CATEGORICAL,
    parameters.GPR_AMP_CUTOFF: int,
    parameters.GPR_AMP_FACTOR: float,
    parameters.GPR_AMP_METHOD: GPR_AMP_METHOD_CATEGORICAL,
    parameters.GPR_DRAWS: int,
    parameters.HOLDOUTS: int,
    parameters.LOCATION_SET_ID: int,
    parameters.METRIC_ID: int,
    parameters.MODEL_INDEX_ID: int,
    parameters.MODELABLE_ENTITY_ID: int,
    parameters.PATH_TO_CUSTOM_COVARIATES: object,
    parameters.PATH_TO_CUSTOM_STAGE_1: object,
    parameters.PATH_TO_DATA: object,
    parameters.PREDICT_RE: int,
    parameters.PREDICTION_UNITS: str,
    parameters.RAKE_LOGIT: int,
    parameters.RELEASE_ID: int,
    parameters.ST_VERSION: VERSION_CATEGORICAL,
    parameters.STAGE_1_MODEL_FORMULA: object,
    parameters.TRANSFORM_OFFSET: float,
    parameters.YEAR_END: int,
    parameters.YEAR_START: int,
}

# Types of parameters that can have a list of values.
# This is the type of each value in the list.
CONFIG_LIST: Final[Dict[str, Any]] = {
    parameters.AGE_EXPAND: str_to_int,
    parameters.GBD_COVARIATE_MODEL_VERSION_IDS: str_to_int_or_str,
    parameters.DENSITY_CUTOFFS: str_to_int,
    parameters.GBD_COVARIATES: str,
    parameters.GPR_SCALE: float,
    parameters.LEVEL_4_TO_3_AGGREGATE: str_to_int,
    parameters.LEVEL_5_TO_4_AGGREGATE: str_to_int,
    parameters.LEVEL_6_TO_5_AGGREGATE: str_to_int,
    parameters.PREDICTION_AGE_GROUP_IDS: str_to_int,
    parameters.PREDICTION_SEX_IDS: str_to_int,
    parameters.PREDICTION_YEAR_IDS: str_to_int,
    parameters.SEX_EXPAND: str_to_int,
    parameters.ST_CUSTOM_AGE_VECTOR: float,
    parameters.ST_LAMBDA: float,
    parameters.ST_OMEGA: float,
    parameters.ST_ZETA: float,
}

DEMOGRAPHICS: Final[Dict[str, Any]] = {
    columns.LOCATION_ID: int,
    columns.YEAR_ID: int,
    columns.AGE_GROUP_ID: int,
    columns.SEX_ID: int,
}
