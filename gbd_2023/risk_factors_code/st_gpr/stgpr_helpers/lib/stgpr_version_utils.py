"""Helpers for creating and managing ST-GPR versions."""

import getpass
import json
import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from sqlalchemy import orm

import db_stgpr
import ihme_cc_rules_client
import stgpr_schema
from db_stgpr import columns as db_columns
from gbd.estimation_years import estimation_years_from_release_id
from ihme_cc_rules_client import ResearchAreas, Tools

from stgpr_helpers.lib import (
    covariate_utils,
    file_utils,
    general_utils,
    location_utils,
    model_type_utils,
    parameter_utils,
)
from stgpr_helpers.lib.constants import location, parameters
from stgpr_helpers.lib.validation import model_quota as model_quota_validation
from stgpr_helpers.lib.validation import parameter as parameter_validation
from stgpr_helpers.lib.validation import pregnancy as pregnancy_validation
from stgpr_helpers.lib.validation import rules as rules_validation

_MODEL_TYPE_STR: Dict[stgpr_schema.ModelType, str] = {
    stgpr_schema.ModelType.base: "standard",
    stgpr_schema.ModelType.dd: "density cutoffs",
    stgpr_schema.ModelType.in_sample_selection: "in-sample selection",
    stgpr_schema.ModelType.oos_evaluation: "out-of-sample evaluation",
    stgpr_schema.ModelType.oos_selection: "out-of-sample selection",
}


def create_stgpr_version(
    path_to_config: Optional[str],
    model_index_id: Optional[int],
    parameters_dict: Optional[Dict[str, Any]],
    user: Optional[str],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
    covariates_session: orm.Session,
    shared_vip_session: orm.Session,
) -> int:
    """Creates a new ST-GPR version and adds it to the database."""
    if parameters_dict:
        params_df = pd.DataFrame(parameters_dict, index=[0])
    elif path_to_config:
        logging.info("Reading parameters from config")
        params_df = parameter_utils.read_parameters_from_config(
            path_to_config, model_index_id
        )
    else:
        raise RuntimeError("Must pass either path_to_config or parameters")
    logging.info("Processing parameters")
    params = parameter_utils.process_parameters(params_df)
    # save whether ME is pregnancy type
    params[parameters.IS_PREGNANCY_ME] = pregnancy_validation.is_pregnancy_modelable_entity(
        modelable_entity_id=params[parameters.MODELABLE_ENTITY_ID], session=shared_vip_session
    )

    logging.info("Validating parameters")
    _validate_parameters(
        params, stgpr_session, epi_session, covariates_session, shared_vip_session
    )

    # Density cutoffs can be passed as e.g. either [5, 10] or [0, 5, 10], and get_parameters
    # returns them in the [5, 10] format. Standardize on [0, 5, 10] for data prep.
    # TODO: standardize density cutoff format across the entire model.
    params[parameters.DENSITY_CUTOFFS].insert(0, 0)  # pytype: disable=attribute-error

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
    prediction_location_set_version_id = (
        location_utils.get_prediction_location_set_version_id(
            params[parameters.LOCATION_SET_ID],
            params[parameters.RELEASE_ID],
            session=stgpr_session,
        )
    )
    standard_location_set_version_id = location_utils.get_location_set_version_id(
        location.STANDARD_LOCATION_SET_ID,
        params[parameters.RELEASE_ID],
        session=stgpr_session,
    )
    aggregation_location_set_version_id = location_utils.get_location_set_version_id(
        params[parameters.LOCATION_SET_ID],
        params[parameters.RELEASE_ID],
        session=stgpr_session,
    )

    logging.info("Creating ST-GPR version")
    random_seed = np.random.randint(0, np.iinfo(np.int32).max - 1)
    _update_parameters(
        params,
        prediction_location_set_version_id,
        standard_location_set_version_id,
        aggregation_location_set_version_id,
        random_seed,
        user,
    )
    settings = stgpr_schema.get_settings()
    stgpr_version_id = _create_stgpr_version(params, stgpr_session)
    logging.info(f"Created ST-GPR version with ID {stgpr_version_id}")

    logging.info("Caching parameters")
    params[parameters.OUTPUT_PATH] = settings.output_root_format.format(
        stgpr_version_id=stgpr_version_id
    )
    file_utility = file_utils.StgprFileUtility(stgpr_version_id)

    file_utility.make_root_parameters_directory()
    file_utility.cache_parameters(params)

    return stgpr_version_id


def _validate_parameters(
    params: Dict[str, Any],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
    covariates_session: orm.Session,
    shared_vip_session: orm.Session,
) -> None:
    """Checks that parameters are valid."""
    parameter_validation.run_validations(
        params,
        raise_on_failure=True,
        stgpr_session=stgpr_session,
        epi_session=epi_session,
        covariates_session=covariates_session,
        shared_vip_session=shared_vip_session,
    )

    # Instantiate rules manager after we validate proper set of GBD release
    rules_manager = ihme_cc_rules_client.RulesManager(
        release_id=params[parameters.RELEASE_ID],
        research_area=ResearchAreas.EPI,
        tool=Tools.STGPR,
    )
    rules_validation.validate_model_can_run(
        rules_manager=rules_manager, release_id=params[parameters.RELEASE_ID]
    )
    model_quota_validation.validate_model_quota_not_met(
        params[parameters.MODELABLE_ENTITY_ID],
        params[parameters.GPR_DRAWS],
        params[parameters.RELEASE_ID],
        stgpr_session,
    )


def _update_parameters(
    params: Dict[str, Any],
    prediction_location_set_version_id: int,
    standard_location_set_version_id: int,
    aggregation_location_set_version_id: int,
    random_seed: int,
    user: Optional[str],
) -> None:
    """Updates parameters to unify cached and db parameters before adding to db.

    Updates happen in place.
    """
    # If amplitude not provided, check if other GPR amp params are None and fill in w/ default
    if not params[parameters.CUSTOM_AMPLITUDE]:
        if not params[parameters.GPR_AMP_FACTOR]:
            params[parameters.GPR_AMP_FACTOR] = 1
        if not params[parameters.GPR_AMP_METHOD]:
            params[parameters.GPR_AMP_METHOD] = (
                stgpr_schema.AmplitudeMethod.global_above_cutoff.name
            )

    # If year_end is not provided, update to last estimation year of passed release_id
    if not params[parameters.YEAR_END]:
        params[parameters.YEAR_END] = estimation_years_from_release_id(
            params[parameters.RELEASE_ID]
        )[-1]
        logging.info(
            f"Setting year_end to last estimation year for release_id "
            f"{params[parameters.RELEASE_ID]}: {params[parameters.YEAR_END]}"
        )

    params[parameters.CUSTOM_STAGE_1] = bool(params[parameters.PATH_TO_CUSTOM_STAGE_1])
    params[parameters.PREDICTION_YEAR_IDS] = list(
        range(params[parameters.YEAR_START], params[parameters.YEAR_END] + 1)
    )
    params[parameters.USER] = user or getpass.getuser()
    params[parameters.PREDICTION_LOCATION_SET_VERSION_ID] = prediction_location_set_version_id
    params[parameters.STANDARD_LOCATION_SET_VERSION_ID] = standard_location_set_version_id
    params[parameters.AGGREGATION_LOCATION_SET_VERSION_ID] = (
        aggregation_location_set_version_id
    )
    params[parameters.IS_CUSTOM_OFFSET] = params[parameters.TRANSFORM_OFFSET] is not None
    params[parameters.RANDOM_SEED] = random_seed
    params[parameters.CUSTOM_GPR_AMP_CUTOFF] = params[parameters.GPR_AMP_CUTOFF] is not None
    _update_expansion_column_parameters(params)


def _update_expansion_column_parameters(params: Dict[str, Any]) -> None:
    """After expand columns are validated, format map of aggregate to expansion values.

    At this point, both expansion columns and prediction ids have been validated.
    """
    if params[parameters.AGE_EXPAND]:
        age_dict_to_upload = json.dumps(
            {params[parameters.PREDICTION_AGE_GROUP_IDS][0]: params[parameters.AGE_EXPAND]}
        )
        params[parameters.AGE_EXPAND] = str(age_dict_to_upload)

    if params[parameters.SEX_EXPAND]:
        sex_dict_to_upload = json.dumps(
            {params[parameters.PREDICTION_SEX_IDS][0]: params[parameters.SEX_EXPAND]}
        )
        params[parameters.SEX_EXPAND] = str(sex_dict_to_upload)


def _create_stgpr_version(params: Dict[str, Any], stgpr_session: orm.Session) -> int:
    """Creates a new ST-GPR version in the database. Maps parameters to database columns."""
    covariate_info = (
        covariate_utils.get_gbd_covariate_info(
            params[parameters.GBD_COVARIATES],
            params[parameters.GBD_COVARIATE_MODEL_VERSION_IDS],
            params[parameters.RELEASE_ID],
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
    settings = stgpr_schema.get_settings()
    return db_stgpr.create_stgpr_version(
        params[parameters.DENSITY_CUTOFFS],
        covariate_info,
        hyperparameter_sets,
        settings.output_root_format,
        session=stgpr_session,
        values={
            db_columns.BUNDLE_ID: params[parameters.BUNDLE_ID],
            db_columns.CROSSWALK_VERSION_ID: params[parameters.CROSSWALK_VERSION_ID],
            db_columns.MODELABLE_ENTITY_ID: params[parameters.MODELABLE_ENTITY_ID],
            db_columns.COVARIATE_ID: params[parameters.COVARIATE_ID],
            db_columns.RELEASE_ID: params[parameters.RELEASE_ID],
            db_columns.METRIC_ID: params[parameters.METRIC_ID],
            db_columns.MODEL_TYPE_ID: stgpr_schema.ModelType[
                params[parameters.MODEL_TYPE]
            ].value,
            db_columns.MODEL_STATUS_ID: stgpr_schema.ModelStatus.registration.value,
            db_columns.CUSTOM_STAGE_1: params[parameters.CUSTOM_STAGE_1],
            db_columns.DESCRIPTION: params[parameters.DESCRIPTION],
            db_columns.PREDICTION_UNITS: params[parameters.PREDICTION_UNITS],
            db_columns.PREDICTION_LOCATION_SET_VERSION_ID: params[
                parameters.PREDICTION_LOCATION_SET_VERSION_ID
            ],
            db_columns.STANDARD_LOCATION_SET_VERSION_ID: params[
                parameters.STANDARD_LOCATION_SET_VERSION_ID
            ],
            db_columns.AGGREGATION_LOCATION_SET_VERSION_ID: params[
                parameters.AGGREGATION_LOCATION_SET_VERSION_ID
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
            db_columns.AGE_EXPAND: (
                params[parameters.AGE_EXPAND] if params[parameters.AGE_EXPAND] else None
            ),
            db_columns.SEX_EXPAND: (
                params[parameters.SEX_EXPAND] if params[parameters.SEX_EXPAND] else None
            ),
            db_columns.TRANSFORM_TYPE_ID: stgpr_schema.TransformType[
                params[parameters.DATA_TRANSFORM]
            ].value,
            db_columns.TRANSFORM_OFFSET: params[parameters.TRANSFORM_OFFSET],
            db_columns.IS_CUSTOM_OFFSET: params[parameters.IS_CUSTOM_OFFSET],
            db_columns.ST_VERSION_TYPE_ID: stgpr_schema.Version[
                params[parameters.ST_VERSION]
            ].value,
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
            db_columns.GPR_AMP_METHOD_ID: (
                stgpr_schema.AmplitudeMethod[params[parameters.GPR_AMP_METHOD]].value
                if params[parameters.GPR_AMP_METHOD] is not None
                else None
            ),
            db_columns.CUSTOM_GPR_AMP_CUTOFF: params[parameters.CUSTOM_GPR_AMP_CUTOFF],
            db_columns.GPR_AMP_CUTOFF: params[parameters.GPR_AMP_CUTOFF],
            db_columns.CUSTOM_AMPLITUDE: params[parameters.CUSTOM_AMPLITUDE],
            db_columns.GPR_DRAWS: params[parameters.GPR_DRAWS],
            db_columns.HOLDOUTS: params[parameters.HOLDOUTS],
            db_columns.RANDOM_SEED: params[parameters.RANDOM_SEED],
            db_columns.ME_NAME: params[parameters.ME_NAME],
            db_columns.USER: params[parameters.USER],
        },
    )
