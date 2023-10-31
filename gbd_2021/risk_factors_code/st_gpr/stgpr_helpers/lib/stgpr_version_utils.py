"""Helpers for creating and managing ST-GPR versions."""
import getpass
import logging
from typing import Any, Dict, Optional

import numpy as np
from sqlalchemy import orm

import db_stgpr
from db_stgpr import columns as db_columns
from db_stgpr.api.enums import AmplitudeMethod, ModelStatus, ModelType, TransformType, Version
from gbd import constants as gbd_constants
from gbd import decomp_step as gbd_decomp_step
from gbd import release as gbd_release
from rules import enums as rules_enums
from rules.RulesManager import RulesManager

from stgpr_helpers.lib import (
    config_utils,
    covariate_utils,
    file_utils,
    general_utils,
    location_utils,
    model_type_utils,
)
from stgpr_helpers.lib.constants import location, parameters, paths
from stgpr_helpers.lib.validation import parameter as parameter_validation
from stgpr_helpers.lib.validation import rules as rules_validation

_MODEL_TYPE_STR: Dict[ModelType, str] = {
    ModelType.base: "standard",
    ModelType.dd: "density cutoffs",
    ModelType.in_sample_selection: "in-sample selection",
    ModelType.oos_evaluation: "out-of-sample evaluation",
    ModelType.oos_selection: "out-of-sample selection",
}


def create_stgpr_version(
    path_to_config: str,
    model_index_id: Optional[int],
    output_path: Optional[str],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
) -> int:
    """Creates a new ST-GPR version and adds it to the database."""
    logging.info("Reading parameters")
    params = config_utils.get_parameters_from_config(path_to_config, model_index_id)
    rules_manager = _get_rules_manager(params)
    logging.info("Validating parameters")
    _validate_parameters(params, rules_manager, stgpr_session, epi_session)

    # Density cutoffs can be passed as e.g. either [5, 10] or [0, 5, 10], and get_parameters
    # returns them in the [5, 10] format. Standardize on [0, 5, 10] for data prep.
    params[parameters.DENSITY_CUTOFFS].insert(0, 0)  # type: ignore

    model_type = model_type_utils.determine_model_type(
        params[parameters.DENSITY_CUTOFFS],
        params[parameters.HOLDOUTS],
        params[parameters.ST_LAMBDA],
        params[parameters.ST_OMEGA],
        params[parameters.ST_ZETA],
        params[parameters.GPR_SCALE],
    )
    params[parameters.MODEL_TYPE] = model_type.name
    logging.info(f"This is a {_MODEL_TYPE_STR[model_type]} model")

    logging.info("Getting active location set version")
    prediction_location_set_version_id = location_utils.get_location_set_version_id(
        params[parameters.LOCATION_SET_ID],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        session=stgpr_session,
    )
    standard_location_set_version_id = location_utils.get_location_set_version_id(
        location.STANDARD_LOCATION_SET_ID,
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        session=stgpr_session,
    )

    logging.info("Creating ST-GPR version")
    random_seed = np.random.randint(0, np.iinfo(np.int32).max - 1)
    _update_parameters(
        params,
        prediction_location_set_version_id,
        standard_location_set_version_id,
        random_seed,
    )
    output_path_format = output_path or paths.DEFAULT_OUTPUT_ROOT_FORMAT
    stgpr_version_id = _create_stgpr_version(params, output_path_format, stgpr_session)
    logging.info(f"Created ST-GPR version with ID {stgpr_version_id}")

    logging.info("Caching parameters")
    default_output_path = output_path_format.format(stgpr_version_id=stgpr_version_id)
    params[parameters.OUTPUT_PATH] = output_path or default_output_path
    file_utility = file_utils.StgprFileUtility(params[parameters.OUTPUT_PATH])

    file_utility.make_root_parameters_directory()
    file_utility.cache_parameters(params)

    return stgpr_version_id


def _get_rules_manager(params: Dict[str, Any]) -> Optional[RulesManager]:
    """Returns an instantiated RulesManager this is not a `path_to_data` model."""
    path_to_data: Optional[str] = params[parameters.PATH_TO_DATA]
    gbd_round_id: Optional[int] = params[parameters.GBD_ROUND_ID]
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    if path_to_data:
        return None
    return RulesManager(
        rules_enums.ResearchAreas.EPI,
        rules_enums.Tools.STGPR,
        decomp_step,
        gbd_round_id=gbd_round_id,
    )


def _validate_parameters(
    params: Dict[str, Any],
    rules_manager: Optional[RulesManager],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
) -> None:
    """Checks that parameters are valid."""
    gbd_round_id: int = params[parameters.GBD_ROUND_ID]
    decomp_step: str = params[parameters.DECOMP_STEP]

    parameter_validation.validate_parameters(params, stgpr_session, epi_session)
    rules_validation.validate_model_can_run(rules_manager, gbd_round_id, decomp_step)


def _update_parameters(
    params: Dict[str, Any],
    prediction_location_set_version_id: int,
    standard_location_set_version_id: int,
    random_seed: int,
) -> None:
    """Updates parameters to unify cached and db parameters before adding to db.

    Updates happen in place.
    """
    params[parameters.DECOMP_STEP_ID] = gbd_decomp_step.decomp_step_id_from_decomp_step(
        params[parameters.DECOMP_STEP], params[parameters.GBD_ROUND_ID]
    )
    params[parameters.RELEASE_ID] = gbd_release.get_release_id(
        decomp_step=params[parameters.DECOMP_STEP],
        gbd_round_id=params[parameters.GBD_ROUND_ID],
        tool_type_id=gbd_constants.tool_types.STGPR,
    )
    params[parameters.CUSTOM_STAGE_1] = bool(params[parameters.PATH_TO_CUSTOM_STAGE_1])
    params[parameters.PREDICTION_YEAR_IDS] = list(
        range(params[parameters.YEAR_START], params[parameters.YEAR_END] + 1)
    )
    params[parameters.USER] = getpass.getuser()
    params[parameters.PREDICTION_LOCATION_SET_VERSION_ID] = prediction_location_set_version_id
    params[parameters.STANDARD_LOCATION_SET_VERSION_ID] = standard_location_set_version_id
    params[parameters.IS_CUSTOM_OFFSET] = params[parameters.TRANSFORM_OFFSET] is not None
    params[parameters.RANDOM_SEED] = random_seed
    params[parameters.CUSTOM_GPR_AMP_CUTOFF] = params[parameters.GPR_AMP_CUTOFF] is not None


def _create_stgpr_version(
    params: Dict[str, Any],
    output_path_format: str,
    stgpr_session: orm.Session,
) -> int:
    """Creates a new ST-GPR version in the database. Maps parameters to database columns."""
    covariate_info = (
        covariate_utils.get_gbd_covariate_info(
            params[parameters.GBD_COVARIATES],
            params[parameters.GBD_ROUND_ID],
            params[parameters.DECOMP_STEP],
            stgpr_session,
        ).to_dict("records")
        if params[parameters.GBD_COVARIATES]
        else []
    )
    hyperparameter_sets = {
        db_columns.ST_LAMBDA: params[parameters.ST_LAMBDA],
        db_columns.ST_OMEGA: params[parameters.ST_OMEGA],
        db_columns.ST_ZETA: params[parameters.ST_ZETA],
        db_columns.GPR_SCALE: params[parameters.GPR_SCALE],
    }
    return db_stgpr.create_stgpr_version(
        params[parameters.DENSITY_CUTOFFS],
        covariate_info,
        hyperparameter_sets,
        output_path_format,
        session=stgpr_session,
        values={
            db_columns.BUNDLE_ID: params[parameters.BUNDLE_ID],
            db_columns.CROSSWALK_VERSION_ID: params[parameters.CROSSWALK_VERSION_ID],
            db_columns.MODELABLE_ENTITY_ID: params[parameters.MODELABLE_ENTITY_ID],
            db_columns.COVARIATE_ID: params[parameters.COVARIATE_ID],
            db_columns.GBD_ROUND_ID: params[parameters.GBD_ROUND_ID],
            db_columns.DECOMP_STEP_ID: params[parameters.DECOMP_STEP_ID],
            db_columns.RELEASE_ID: params[parameters.RELEASE_ID],
            db_columns.MODEL_TYPE_ID: ModelType[params[parameters.MODEL_TYPE]].value,
            db_columns.MODEL_STATUS_ID: ModelStatus.registration.value,
            db_columns.CUSTOM_STAGE_1: params[parameters.CUSTOM_STAGE_1],
            db_columns.DESCRIPTION: params[parameters.DESCRIPTION],
            db_columns.PREDICTION_UNITS: params[parameters.PREDICTION_UNITS],
            db_columns.PREDICTION_LOCATION_SET_VERSION_ID: params[
                parameters.PREDICTION_LOCATION_SET_VERSION_ID
            ],
            db_columns.STANDARD_LOCATION_SET_VERSION_ID: params[
                parameters.STANDARD_LOCATION_SET_VERSION_ID
            ],
            db_columns.PREDICTION_YEAR_IDS: general_utils.join_or_none(
                params[parameters.PREDICTION_YEAR_IDS]
            ),
            db_columns.PREDICTION_SEX_IDS: general_utils.join_or_none(
                params[parameters.PREDICTION_SEX_IDS]
            ),
            db_columns.PREDICTION_AGE_GROUP_IDS: general_utils.join_or_none(
                params[parameters.PREDICTION_AGE_GROUP_IDS]
            ),
            db_columns.TRANSFORM_TYPE_ID: TransformType[
                params[parameters.DATA_TRANSFORM]
            ].value,
            db_columns.TRANSFORM_OFFSET: params[parameters.TRANSFORM_OFFSET],
            db_columns.IS_CUSTOM_OFFSET: params[parameters.IS_CUSTOM_OFFSET],
            db_columns.ST_VERSION_TYPE_ID: Version[params[parameters.ST_VERSION]].value,
            db_columns.ADD_NSV: params[parameters.ADD_NSV],
            db_columns.RAKE_LOGIT: params[parameters.RAKE_LOGIT],
            db_columns.AGG_LEVEL_4_TO_3: general_utils.join_or_none(
                params[parameters.LEVEL_4_TO_3_AGGREGATE]
            ),
            db_columns.AGG_LEVEL_5_TO_4: general_utils.join_or_none(
                params[parameters.LEVEL_5_TO_4_AGGREGATE]
            ),
            db_columns.AGG_LEVEL_6_TO_5: general_utils.join_or_none(
                params[parameters.LEVEL_6_TO_5_AGGREGATE]
            ),
            db_columns.STAGE_1_MODEL_FORMULA: params[parameters.STAGE_1_MODEL_FORMULA],
            db_columns.PREDICT_RE: params[parameters.PREDICT_RE],
            db_columns.ST_CUSTOM_AGE_VECTOR: general_utils.join_or_none(
                params[parameters.ST_CUSTOM_AGE_VECTOR]
            ),
            db_columns.GPR_AMP_FACTOR: params[parameters.GPR_AMP_FACTOR],
            db_columns.GPR_AMP_METHOD_ID: AmplitudeMethod[
                params[parameters.GPR_AMP_METHOD]
            ].value,
            db_columns.CUSTOM_GPR_AMP_CUTOFF: params[parameters.CUSTOM_GPR_AMP_CUTOFF],
            db_columns.GPR_AMP_CUTOFF: params[parameters.GPR_AMP_CUTOFF],
            db_columns.GPR_DRAWS: params[parameters.GPR_DRAWS],
            db_columns.HOLDOUTS: params[parameters.HOLDOUTS],
            db_columns.RANDOM_SEED: params[parameters.RANDOM_SEED],
            db_columns.ME_NAME: params[parameters.ME_NAME],
            db_columns.NOTES: params[parameters.NOTES],
            db_columns.USER: params[parameters.USER],
        },
    )
