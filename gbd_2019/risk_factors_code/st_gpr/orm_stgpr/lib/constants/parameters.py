from typing import Any, Dict, Set

import numpy as np

import gbd.constants as gbd

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import amp_method, version


ADD_NSV: str = 'add_nsv'
BUNDLE_ID: str = 'bundle_id'
CODE_VERSION: str = 'code_version'
COVARIATE_MODEL_VERSION_ID: str = 'covariate_model_version_id'
COVARIATE_ID: str = 'covariate_id'
CROSSWALK_VERSION_ID: str = 'crosswalk_version_id'
CUSTOM_INPUTS: str = 'custom_inputs'
CUSTOM_STAGE_1: str = 'custom_stage_1'
DATA_TRANSFORM: str = 'data_transform'
DECOMP_STEP: str = 'decomp_step'
DENSITY_CUTOFFS: str = 'density_cutoffs'
EPI_MODEL_VERSION_ID: str = 'epi_model_version_id'
GBD_COVARIATES: str = 'gbd_covariates'
GBD_ROUND_ID: str = 'gbd_round_id'
GPR_AMP_CUTOFF: str = 'gpr_amp_cutoff'
GPR_AMP_FACTOR: str = 'gpr_amp_factor'
GPR_AMP_METHOD: str = 'gpr_amp_method'
GPR_DRAWS: str = 'gpr_draws'
GPR_SCALE: str = 'gpr_scale'
HOLDOUTS: str = 'holdouts'
IS_CUSTOM_OFFSET: str = 'is_custom_offset'
LEVEL_4_TO_3_AGGREGATE: str = 'agg_level_4_to_3'
LEVEL_5_TO_4_AGGREGATE: str = 'agg_level_5_to_4'
LEVEL_6_TO_5_AGGREGATE: str = 'agg_level_6_to_5'
LOCATION_SET_ID: str = 'location_set_id'
ME_NAME: str = 'me_name'
MODEL_INDEX_ID: str = 'model_index_id'
MODEL_TYPE_ID: str = 'model_type_id'
MODELABLE_ENTITY_ID: str = 'modelable_entity_id'
NOTES: str = 'notes'
PATH_TO_CUSTOM_COVARIATES: str = 'path_to_custom_covariates'
PATH_TO_CUSTOM_STAGE_1: str = 'path_to_custom_stage_1'
PATH_TO_DATA: str = 'path_to_data'
PREDICT_RE: str = 'predict_re'
PREDICTION_AGE_GROUP_IDS: str = 'prediction_age_group_ids'
PREDICTION_LOCATION_SET_VERSION_ID: str = 'prediction_location_set_version_id'
PREDICTION_SEX_IDS: str = 'prediction_sex_ids'
PREDICTION_UNITS: str = 'prediction_units'
PREDICTION_YEAR_IDS: str = 'prediction_year_ids'
RAKE_LOGIT: str = 'rake_logit'
RANDOM_SEED: str = 'random_seed'
RUN_TYPE: str = 'run_type'
ST_CUSTOM_AGE_VECTOR: str = 'st_custom_age_vector'
ST_LAMBDA: str = 'st_lambda'
ST_OMEGA: str = 'st_omega'
ST_VERSION: str = 'st_version'
ST_ZETA: str = 'st_zeta'
STANDARD_LOCATION_SET_VERSION_ID: str = 'standard_location_set_version_id'
STAGE_1_MODEL_FORMULA: str = 'stage_1_model_formula'
TRANSFORM_OFFSET: str = 'transform_offset'
USER: str = 'user'
YEAR_END: str = 'year_end'
YEAR_START: str = 'year_start'

# These parameters must be specified.
REQUIRED_PARAMETERS: Set[str] = {
    DECOMP_STEP,
    GPR_SCALE,
    MODELABLE_ENTITY_ID,
    PREDICTION_UNITS,
    ST_LAMBDA,
    ST_OMEGA,
    ST_ZETA
}

# These parameters are optional, and default values are assigned if the
# parameters are not present in the config. Some combination of these
# parameters may be required, but it is possible to run ST-GPR without
# specifying each of these individual parameters.
# Note that assigning lists in pandas is done with [[]] instead of [].
DEFAULT_PARAMETERS: Dict[str, Any] = {
    ADD_NSV: 1,
    BUNDLE_ID: None,
    COVARIATE_ID: None,
    CROSSWALK_VERSION_ID: None,
    DATA_TRANSFORM: lookup_tables.TransformType.none.name,
    DENSITY_CUTOFFS: [[]],
    GPR_DRAWS: 0,
    GBD_COVARIATES: [[]],
    GBD_ROUND_ID: gbd.GBD_ROUND_ID,
    GPR_AMP_CUTOFF: None,
    GPR_AMP_FACTOR: 1,
    GPR_AMP_METHOD: amp_method.GLOBAL_ABOVE_CUTOFF,
    HOLDOUTS: 0,
    LEVEL_4_TO_3_AGGREGATE: [[]],
    LEVEL_5_TO_4_AGGREGATE: [[]],
    LEVEL_6_TO_5_AGGREGATE: [[]],
    LOCATION_SET_ID: 22,
    ME_NAME: None,
    MODEL_INDEX_ID: None,
    MODELABLE_ENTITY_ID: None,
    NOTES: None,
    PATH_TO_CUSTOM_COVARIATES: None,
    PATH_TO_CUSTOM_STAGE_1: None,
    PATH_TO_DATA: None,
    PREDICT_RE: 0,
    PREDICTION_AGE_GROUP_IDS: [[gbd.age.ALL_AGES]],
    PREDICTION_SEX_IDS: [[gbd.sex.BOTH]],
    RAKE_LOGIT: 0,
    ST_CUSTOM_AGE_VECTOR: [[]],
    ST_VERSION: version.BETA,
    STAGE_1_MODEL_FORMULA: None,
    TRANSFORM_OFFSET: None,
    YEAR_END: gbd.GBD_ROUND,
    YEAR_START: 1980
}


# Outline of parameters to return from a call to get_parameters.
# List parameters are flattened.
PARAMETER_SKELETON = {
    **{
        name: np.ravel(val).tolist() if isinstance(val, list) else val
        for name, val in DEFAULT_PARAMETERS.items()
        if name not in (
            ME_NAME,
            MODEL_INDEX_ID,
            PATH_TO_CUSTOM_COVARIATES,
            PATH_TO_CUSTOM_STAGE_1,
            PATH_TO_DATA)
    },
    **{
        CODE_VERSION: None,
        COVARIATE_MODEL_VERSION_ID: None,
        CUSTOM_STAGE_1: None,
        DECOMP_STEP: None,
        EPI_MODEL_VERSION_ID: None,
        GPR_SCALE: [],
        IS_CUSTOM_OFFSET: None,
        MODELABLE_ENTITY_ID: None,
        PREDICTION_UNITS: None,
        PREDICTION_YEAR_IDS: [],
        RANDOM_SEED: None,
        RUN_TYPE: None,
        ST_LAMBDA: [],
        ST_OMEGA: [],
        ST_ZETA: [],
        USER: None
    },
}
