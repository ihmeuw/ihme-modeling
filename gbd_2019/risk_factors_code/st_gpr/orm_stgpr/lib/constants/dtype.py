"""
Specifies dtypes for pandas dataframes.
Using dtypes ensures that user data has the required types and that dataframes
do not use excess memory.
"""

from typing import Any, Dict

import numpy as np
import pandas as pd

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import amp_method, columns, parameters, version


def str_to_int(val: str):
    """
    Casts a string to an int, ex: '22.0' -> 22

    For whatever reason, native int() cannot cast a string to an int
    if the string is actually a float. We need this behavior because
    if a column in a config has any missingness, pandas will read the
    column in as float rather than an int and that causes problems.

    Raises:
        TypeError if the string is not a whole number.
    """
    try:
        as_float = float(val)
    except (ValueError, TypeError):
        raise TypeError(f'Value {val} is not a number.')
    if not as_float.is_integer():
        raise TypeError('Value {val} is not an int.')
    return int(as_float)


DATA_TRANSFORM_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    [tt.name for tt in lookup_tables.TransformType]
)
GPR_AMP_METHOD_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    amp_method.ALL_GPR_AMP_METHODS
)
VERSION_CATEGORICAL: pd.CategoricalDtype = pd.CategoricalDtype(
    version.ALL_VERSIONS
)

# Types of parameters that can only have a single value.
CONFIG_SCALAR: Dict[str, Any] = {
    parameters.ADD_NSV: np.int8,
    parameters.BUNDLE_ID: pd.Int32Dtype(),
    parameters.COVARIATE_ID: pd.Int32Dtype(),
    parameters.CROSSWALK_VERSION_ID: pd.Int32Dtype(),
    parameters.DATA_TRANSFORM: DATA_TRANSFORM_CATEGORICAL,
    parameters.DECOMP_STEP: object,
    parameters.GBD_ROUND_ID: np.uint8,
    parameters.GPR_AMP_CUTOFF: pd.Int32Dtype(),
    parameters.GPR_AMP_FACTOR: np.float64,
    parameters.GPR_AMP_METHOD: GPR_AMP_METHOD_CATEGORICAL,
    parameters.GPR_DRAWS: pd.Int16Dtype(),
    parameters.HOLDOUTS: pd.Int16Dtype(),
    parameters.LOCATION_SET_ID: np.uint32,
    parameters.MODEL_INDEX_ID: pd.Int32Dtype(),
    parameters.MODELABLE_ENTITY_ID: pd.Int32Dtype(),
    parameters.NOTES: object,
    parameters.PATH_TO_CUSTOM_COVARIATES: object,
    parameters.PATH_TO_CUSTOM_STAGE_1: object,
    parameters.PATH_TO_DATA: object,
    parameters.PREDICT_RE: np.int8,
    parameters.PREDICTION_UNITS: str,
    parameters.RAKE_LOGIT: np.int8,
    parameters.ST_VERSION: VERSION_CATEGORICAL,
    parameters.STAGE_1_MODEL_FORMULA: object,
    parameters.TRANSFORM_OFFSET: np.float,
    parameters.YEAR_END: np.uint16,
    parameters.YEAR_START: np.uint16
}

# Types of parameters that can have a list of values.
# This is the type of each value in the list.
CONFIG_LIST: Dict[str, Any] = {
    parameters.DENSITY_CUTOFFS: str_to_int,
    parameters.GBD_COVARIATES: str,
    parameters.GPR_SCALE: float,
    parameters.LEVEL_4_TO_3_AGGREGATE: str_to_int,
    parameters.LEVEL_5_TO_4_AGGREGATE: str_to_int,
    parameters.LEVEL_6_TO_5_AGGREGATE: str_to_int,
    parameters.PREDICTION_AGE_GROUP_IDS: str_to_int,
    parameters.PREDICTION_SEX_IDS: str_to_int,
    parameters.PREDICTION_YEAR_IDS: str_to_int,
    parameters.ST_CUSTOM_AGE_VECTOR: float,
    parameters.ST_LAMBDA: float,
    parameters.ST_OMEGA: float,
    parameters.ST_ZETA: float
}

DEMOGRAPHICS: Dict[str, Any] = {
    columns.LOCATION_ID: np.uint32,
    columns.YEAR_ID: np.uint16,
    columns.AGE_GROUP_ID: np.uint16,
    columns.SEX_ID: np.uint8
}
