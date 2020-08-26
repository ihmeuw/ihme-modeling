"""
Columns used in ST-GPR database tables and files.
The parameter columns line up with the parameter names, so those are stored in
constants/parameters.py for the most part.
"""

from typing import List

# Demographics
AGE_GROUP_ID: str = 'age_group_id'
LEVEL: str = 'level'
LOCATION_ID: str = 'location_id'
LOCATION_SET_ID: str = 'location_set_id'
LOCATION_SET_VERSION_ID: str = 'location_set_version_id'
MEASURE: str = 'measure'
MEASURE_ID: str = 'measure_id'
PARENT_ID: str = 'parent_id'
PATH_TO_TOP_PARENT: str = 'path_to_top_parent'
POPULATION: str = 'population'
REGION_ID: str = 'region_id'
SEX: str = 'sex'
SEX_ID: str = 'sex_id'
STANDARD_LOCATION: str = 'standard_location'
SUPER_REGION_ID: str = 'super_region_id'
YEAR_ID: str = 'year_id'
DEMOGRAPHICS: List[str] = [LOCATION_ID, YEAR_ID, AGE_GROUP_ID, SEX_ID]
LOCATION: List[str] = [
    LOCATION_ID,
    LOCATION_SET_ID,
    STANDARD_LOCATION,
    PATH_TO_TOP_PARENT,
    PARENT_ID,
    REGION_ID,
    SUPER_REGION_ID
]

# Data
DATA: str = 'data'
IS_OUTLIER: str = 'is_outlier'
LOWER: str = 'lower'
SAMPLE_SIZE: str = 'sample_size'
UPPER: str = 'upper'
VAL: str = 'val'
VARIANCE: str = 'variance'

# Covariates
COVARIATE_LOOKUP: str = 'covariate_lookup'
COVARIATE_LOOKUP_ID: str = 'covariate_lookup_id'
COVARIATE_NAME_SHORT: str = 'covariate_name_short'
COVARIATE_VALUE: str = 'val'
CV_PREFIX: str = 'cv_'
MEAN_VALUE: str = 'mean_value'

# Bundles
BUNDLE_ID: str = 'bundle_id'
CROSSWALK_VERSION_ID: str = 'crosswalk_version_id'
NID: str = 'nid'
SEQ: str = 'seq'
CROSSWALK_DATA: List[str] = [
    LOCATION_ID,
    SEX_ID,
    YEAR_ID,
    AGE_GROUP_ID,
    MEASURE,
    NID,
    VAL,
    VARIANCE,
    SAMPLE_SIZE,
    IS_OUTLIER
]

# GBD metadata
COVARIATE_ID: str = 'covariate_id'
COVARIATE_MODEL_VERSION_ID: str = 'covariate_model_version_id'
DECOMP_STEP_ID: str = 'decomp_step_id'
EPI_MODEL_VERSION_ID: str = 'epi_model_version_id'
GBD_ROUND_ID: str = 'gbd_round_id'
MODELABLE_ENTITY_ID: str = 'modelable_entity_id'
STGPR_VERSION_ID = 'stgpr_version_id'

# Model type
DESCRIPTION: str = 'description'
MODEL_TYPE: str = 'model_type'
MODEL_TYPE_ID: str = 'model_type_id'

# Transform type
DESCRIPTION: str = 'description'
TRANSFORM_TYPE: str = 'transform_type'
TRANSFORM_TYPE_ID: str = 'transform_type_id'

# Model iterations
BEST_ITERATION: str = 'best_iteration'
DATA_DENSITY_END: str = 'data_density_end'
DATA_DENSITY_START: str = 'data_density_start'
GPR_PARAM_SET_ID: str = 'gpr_param_set_id'
MODEL_ITERATION_ID: str = 'model_iteration_id'
MODEL_ITERATION_PARAM_SET_ID: str = 'model_iteration_param_set_id'
ST_PARAM_SET_ID: str = 'st_param_set_id'

# Stage 1
CUSTOM_STAGE_1: str = 'cv_custom_stage_1'
CUSTOM_STAGE_1_VALUE: str = 'val'

# Stats
AMPLITUDE: str = 'amplitude'
IN_SAMPLE_RMSE: str = 'in_sample_rmse'
NON_SAMPLING_VARIANCE: str = 'non_sampling_variance'
OUT_OF_SAMPLE_RMSE: str = 'out_of_sample_rmse'
