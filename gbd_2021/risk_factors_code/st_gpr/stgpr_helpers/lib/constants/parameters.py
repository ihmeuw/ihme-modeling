"""ST-GPR parameters.

Lists all ST-GPR parameters, required parameters, and default parameters.
"""
from typing import Any, Dict, FrozenSet

import numpy as np

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from db_stgpr.api.enums import AmplitudeMethod, TransformType, Version
from gbd import constants as gbd_constants

# Base parameters.
ADD_NSV: Final[str] = "add_nsv"
BUNDLE_ID: Final[str] = "bundle_id"
COVARIATE_MODEL_VERSION_ID: Final[str] = "covariate_model_version_id"
COVARIATE_ID: Final[str] = "covariate_id"
CROSSWALK_VERSION_ID: Final[str] = "crosswalk_version_id"
CUSTOM_GPR_AMP_CUTOFF: Final[str] = "custom_gpr_amp_cutoff"
CUSTOM_INPUTS: Final[str] = "custom_inputs"
CUSTOM_STAGE_1: Final[str] = "custom_stage_1"
DATA_TRANSFORM: Final[str] = "data_transform"
DECOMP_STEP: Final[str] = "decomp_step"
DENSITY_CUTOFFS: Final[str] = "density_cutoffs"
DESCRIPTION: Final[str] = "description"
EPI_MODEL_VERSION_ID: Final[str] = "epi_model_version_id"
GBD_COVARIATES: Final[str] = "gbd_covariates"
GBD_COVARIATE_IDS: Final[str] = "gbd_covariate_ids"
GBD_ROUND_ID: Final[str] = "gbd_round_id"
GPR_AMP_CUTOFF: Final[str] = "gpr_amp_cutoff"
GPR_AMP_FACTOR: Final[str] = "gpr_amp_factor"
GPR_AMP_METHOD: Final[str] = "gpr_amp_method"
GPR_AMP_METHOD_ID: Final[str] = "gpr_amp_method_id"
GPR_DRAWS: Final[str] = "gpr_draws"
GPR_SCALE: Final[str] = "gpr_scale"
HOLDOUTS: Final[str] = "holdouts"
IS_CUSTOM_OFFSET: Final[str] = "is_custom_offset"
LEVEL_4_TO_3_AGGREGATE: Final[str] = "agg_level_4_to_3"
LEVEL_5_TO_4_AGGREGATE: Final[str] = "agg_level_5_to_4"
LEVEL_6_TO_5_AGGREGATE: Final[str] = "agg_level_6_to_5"
LOCATION_SET_ID: Final[str] = "location_set_id"
MEASURE_ID: Final[str] = "measure_id"
ME_NAME: Final[str] = "me_name"
MODEL_INDEX_ID: Final[str] = "model_index_id"
MODEL_STATUS_ID: Final[str] = "model_status_id"
MODEL_TYPE: Final[str] = "model_type"
MODEL_TYPE_ID: Final[str] = "model_type_id"
MODELABLE_ENTITY_ID: Final[str] = "modelable_entity_id"
NOTES: Final[str] = "notes"
PATH_TO_CUSTOM_COVARIATES: Final[str] = "path_to_custom_covariates"
PATH_TO_CUSTOM_STAGE_1: Final[str] = "path_to_custom_stage_1"
PATH_TO_DATA: Final[str] = "path_to_data"
PREDICT_RE: Final[str] = "predict_re"
PREDICTION_AGE_GROUP_IDS: Final[str] = "prediction_age_group_ids"
PREDICTION_LOCATION_SET_VERSION_ID: Final[str] = "prediction_location_set_version_id"
PREDICTION_SEX_IDS: Final[str] = "prediction_sex_ids"
PREDICTION_UNITS: Final[str] = "prediction_units"
PREDICTION_YEAR_IDS: Final[str] = "prediction_year_ids"
RAKE_LOGIT: Final[str] = "rake_logit"
RANDOM_SEED: Final[str] = "random_seed"
RELEASE_ID: Final[str] = "release_id"
ST_CUSTOM_AGE_VECTOR: Final[str] = "st_custom_age_vector"
ST_LAMBDA: Final[str] = "st_lambda"
ST_OMEGA: Final[str] = "st_omega"
ST_VERSION: Final[str] = "st_version"
ST_VERSION_TYPE_ID: Final[str] = "st_version_type_id"
ST_ZETA: Final[str] = "st_zeta"
STANDARD_LOCATION_SET_VERSION_ID: Final[str] = "standard_location_set_version_id"
STAGE_1_MODEL_FORMULA: Final[str] = "stage_1_model_formula"
TRANSFORM_OFFSET: Final[str] = "transform_offset"
TRANSFORM_TYPE_ID: Final[str] = "transform_type_id"
USER: Final[str] = "user"
YEAR_END: Final[str] = "year_end"
YEAR_START: Final[str] = "year_start"

# Additional metadata added to parameters for a model run.
AGE_GROUP_IDS: Final[str] = "age_group_ids"
BEST_MODEL_ID: Final[str] = "best_model_id"
DECOMP_STEP_ID: Final[str] = "decomp_step_id"
LOCATION_IDS: Final[str] = "location_ids"
OUTPUT_PATH: Final[str] = "output_path"
SEX_IDS: Final[str] = "sex_ids"
YEAR_IDS: Final[str] = "year_ids"

# These parameters must be specified.
REQUIRED_PARAMETERS: FrozenSet[str] = frozenset(
    {
        DECOMP_STEP,
        DESCRIPTION,
        GPR_SCALE,
        MODELABLE_ENTITY_ID,
        PREDICTION_UNITS,
        ST_LAMBDA,
        ST_OMEGA,
        ST_ZETA,
    }
)

# These parameters are optional, and default values are assigned if the
# parameters are not present in the config. Some combination of these
# parameters may be required, but it is possible to run ST-GPR without
# specifying each of these individual parameters.
# Note that assigning lists in pandas is done with [[]] instead of [].
DEFAULT_PARAMETERS: Final[Dict[str, Any]] = {
    ADD_NSV: 1,
    BUNDLE_ID: None,
    COVARIATE_ID: None,
    CROSSWALK_VERSION_ID: None,
    DATA_TRANSFORM: TransformType.none.name,
    DENSITY_CUTOFFS: [[]],
    GPR_DRAWS: 0,
    GBD_COVARIATES: [[]],
    GBD_ROUND_ID: gbd_constants.GBD_ROUND_ID,
    GPR_AMP_CUTOFF: None,
    GPR_AMP_FACTOR: 1,
    GPR_AMP_METHOD: AmplitudeMethod.global_above_cutoff.name,
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
    PREDICTION_AGE_GROUP_IDS: [[gbd_constants.age.ALL_AGES]],
    PREDICTION_SEX_IDS: [[gbd_constants.sex.BOTH]],
    RAKE_LOGIT: 0,
    ST_CUSTOM_AGE_VECTOR: [[]],
    ST_VERSION: Version.beta.name,
    STAGE_1_MODEL_FORMULA: None,
    TRANSFORM_OFFSET: None,
    YEAR_END: gbd_constants.GBD_ROUND,
    YEAR_START: 1980,
}


# Outline of parameters to return from a call to get_parameters.
# This includes default values for every parameter to return from get_parameters, and it's
# expected that get_parameters fills in the actual parameter values.
PARAMETER_SKELETON: Final[Dict[str, Any]] = {
    **{
        name: np.ravel(val).tolist() if isinstance(val, list) else val  # Flatten lists.
        for name, val in DEFAULT_PARAMETERS.items()  # Start from default parameters.
        if name
        not in (
            ME_NAME,  # Don't include these parameters in the skeleton.
            MODEL_INDEX_ID,
            PATH_TO_CUSTOM_COVARIATES,
            PATH_TO_CUSTOM_STAGE_1,
            PATH_TO_DATA,
        )
    },
    **{
        COVARIATE_MODEL_VERSION_ID: None,  # Include these parameters in the skeleton.
        CUSTOM_GPR_AMP_CUTOFF: None,
        CUSTOM_STAGE_1: None,
        DECOMP_STEP: None,
        DECOMP_STEP_ID: None,
        DESCRIPTION: None,
        EPI_MODEL_VERSION_ID: None,
        GBD_COVARIATE_IDS: None,
        GPR_AMP_METHOD_ID: None,
        GPR_SCALE: [],
        IS_CUSTOM_OFFSET: None,
        MEASURE_ID: None,
        MODEL_STATUS_ID: None,
        MODEL_TYPE: None,
        MODEL_TYPE_ID: None,
        MODELABLE_ENTITY_ID: None,
        OUTPUT_PATH: None,
        PREDICTION_LOCATION_SET_VERSION_ID: None,
        PREDICTION_UNITS: None,
        PREDICTION_YEAR_IDS: [],
        RANDOM_SEED: None,
        MODEL_TYPE: None,
        ST_LAMBDA: [],
        ST_OMEGA: [],
        ST_VERSION_TYPE_ID: None,
        ST_ZETA: [],
        DATA_TRANSFORM: None,
        TRANSFORM_TYPE_ID: None,
        USER: None,
    },
}
