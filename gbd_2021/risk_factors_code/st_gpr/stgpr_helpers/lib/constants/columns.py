"""Columns used in ST-GPR database tables and files.

The parameter columns line up with the parameter names, so those are stored in
constants/parameters.py for the most part.
"""
from typing import List

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from stgpr_helpers.lib.constants import parameters

# Generic columns
ID: Final[str] = "id"
LOWER: Final[str] = "lower"
UPPER: Final[str] = "upper"
VAL: Final[str] = "val"

# Demographics
AGE_GROUP_ID: Final[str] = "age_group_id"
LEVEL: Final[str] = "level"
LOCATION_ID: Final[str] = "location_id"
LOCATION_SET_ID: Final[str] = "location_set_id"
LOCATION_SET_VERSION_ID: Final[str] = "location_set_version_id"
MEASURE: Final[str] = "measure"
MEASURE_ID: Final[str] = "measure_id"
PARENT_ID: Final[str] = "parent_id"
PATH_TO_TOP_PARENT: Final[str] = "path_to_top_parent"
POPULATION: Final[str] = "population"
REGION_ID: Final[str] = "region_id"
SEX: Final[str] = "sex"
SEX_ID: Final[str] = "sex_id"
STANDARD_LOCATION: Final[str] = "standard_location"
SUPER_REGION_ID: Final[str] = "super_region_id"
YEAR_ID: Final[str] = "year_id"
DEMOGRAPHICS: Final[List[str]] = [YEAR_ID, LOCATION_ID, SEX_ID, AGE_GROUP_ID]
LOCATION: Final[List[str]] = [
    LOCATION_ID,
    LOCATION_SET_ID,
    STANDARD_LOCATION,
    PARENT_ID,
    REGION_ID,
    SUPER_REGION_ID,
]

# Holdouts
HOLDOUT_PREFIX: Final[str] = "holdout_"

# Covariates
COVARIATE: Final[str] = "covariate"
COVARIATE_LOOKUP: Final[str] = "covariate_lookup"
COVARIATE_LOOKUP_ID: Final[str] = "covariate_lookup_id"
COVARIATE_NAME_SHORT: Final[str] = "covariate_name_short"
COVARIATE_VALUE: Final[str] = "val"
CUSTOM_COVARIATE_ID: Final[str] = "custom_covariate_id"
CV_PREFIX: Final[str] = "cv_"
GBD_COVARIATE_ID: Final[str] = "gbd_covariate_id"
MEAN_VALUE: Final[str] = "mean_value"

# Bundles
BUNDLE_ID: Final[str] = "bundle_id"
CROSSWALK_VERSION_ID: Final[str] = "crosswalk_version_id"
NID: Final[str] = "nid"
SEQ: Final[str] = "seq"

# GBD metadata
COVARIATE_ID: Final[str] = "covariate_id"
COVARIATE_MODEL_VERSION_ID: Final[str] = "covariate_model_version_id"
DECOMP_STEP_ID: Final[str] = "decomp_step_id"
EPI_MODEL_VERSION_ID: Final[str] = "epi_model_version_id"
GBD_ROUND_ID: Final[str] = "gbd_round_id"
MODELABLE_ENTITY_ID: Final[str] = "modelable_entity_id"
STGPR_VERSION_ID: Final[str] = "stgpr_version_id"

# Data
DATA: Final[str] = "data"
DATA_STAGE_ID: Final[str] = "data_stage_id"
DATA_STAGE_NAME: Final[str] = "data_stage_name"
IS_OUTLIER: Final[str] = "is_outlier"
ORIGINAL_DATA: Final[str] = "original_data"
ORIGINAL_VARIANCE: Final[str] = "original_variance"
SAMPLE_SIZE: Final[str] = "sample_size"
VARIANCE: Final[str] = "variance"
CROSSWALK_DATA: Final[List[str]] = [
    YEAR_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    MEASURE_ID,
    NID,
    SEQ,
    SAMPLE_SIZE,
    VAL,
    VARIANCE,
    IS_OUTLIER,
]
UPLOAD_DATA: Final[List[str]] = [
    STGPR_VERSION_ID,
    DATA_STAGE_ID,
    YEAR_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    VAL,
    VARIANCE,
    NID,
    SEQ,
    SAMPLE_SIZE,
    LOWER,
    UPPER,
    IS_OUTLIER,
]

# Model type
DESCRIPTION: Final[str] = "description"
MODEL_TYPE: Final[str] = "model_type"
MODEL_TYPE_ID: Final[str] = "model_type_id"

# Model stage
MODEL_STAGE_ID: Final[str] = "model_stage_id"
MODEL_STAGE_NAME: Final[str] = "model_stage_name"

# Model status
MODEL_STATUS_ID: Final[str] = "model_status_id"
MODEL_STATUS_NAME: Final[str] = "model_status_name"

# Transform type
TRANSFORM_TYPE: Final[str] = "transform_type"
TRANSFORM_TYPE_ID: Final[str] = "transform_type_id"

# Model iterations
BEST_ITERATION: Final[str] = "best_iteration"
MODEL_ITERATION_ID: Final[str] = "model_iteration_id"
RMSE: Final[str] = "rmse"

# Density cutoffs
DATA_DENSITY_END: Final[str] = "data_density_end"
DATA_DENSITY_START: Final[str] = "data_density_start"

# GPR amp method
GPR_AMP_METHOD_ID: Final[str] = "gpr_amp_method_id"
GPR_AMP_METHOD_NAME: Final[str] = "gpr_amp_method_name"

# ST version type
ST_VERSION_TYPE_ID: Final[str] = "st_version_type_id"
ST_VERSION_TYPE_NAME: Final[str] = "st_version_type_name"

# Stage 1
BETA: Final[str] = "beta"
CUSTOM_STAGE_1: Final[str] = "cv_custom_stage_1"
CUSTOM_STAGE_1_VALUE: Final[str] = "val"
FACTOR: Final[str] = "factor"
P_VALUE: Final[str] = "p_value"
STANDARD_ERROR: Final[str] = "standard_error"
Z_VALUE: Final[str] = "z_value"
STAGE_1_ESTIMATE: Final[List[str]] = [*DEMOGRAPHICS, VAL]
STAGE_1_STATISTICS: Final[List[str]] = [
    SEX_ID,
    COVARIATE,
    FACTOR,
    BETA,
    STANDARD_ERROR,
    Z_VALUE,
    P_VALUE,
]

# Spacetime
SPACETIME_ESTIMATE: Final[List[str]] = [*DEMOGRAPHICS, VAL]

# GPR
GPR_ESTIMATE: Final[List[str]] = [*DEMOGRAPHICS, VAL, LOWER, UPPER]

# Final/raked
FINAL_ESTIMATE: Final[List[str]] = [*DEMOGRAPHICS, VAL, LOWER, UPPER]

# Fit statistics
IN_SAMPLE_RMSE: Final[str] = "in_sample_rmse"
OUT_OF_SAMPLE_RMSE: Final[str] = "out_of_sample_rmse"
FIT_STATISTICS: Final[List[str]] = [MODEL_STAGE_ID, IN_SAMPLE_RMSE, OUT_OF_SAMPLE_RMSE]

# Amplitude and NSV
AMPLITUDE: Final[str] = "amplitude"
NON_SAMPLING_VARIANCE: Final[str] = "non_sampling_variance"
AMP_NSV_UPLOAD: Final[List[str]] = [LOCATION_ID, SEX_ID, AMPLITUDE, NON_SAMPLING_VARIANCE]

# Hyperparameter sets
HYPERPARAMETER_SET_ID: Final[str] = "hyperparameter_set_id"
HYPERPARAMETERS: Final[List[str]] = [
    parameters.ST_LAMBDA,
    parameters.ST_OMEGA,
    parameters.ST_ZETA,
    parameters.GPR_SCALE,
]
