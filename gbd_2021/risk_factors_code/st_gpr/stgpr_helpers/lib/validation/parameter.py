import logging
import os
from typing import Any, Dict, List, Optional

from sqlalchemy import orm

import db_queries
import db_stgpr
import gbd
from db_stgpr.api.enums import ModelType, TransformType

from stgpr_helpers.lib import model_type_utils
from stgpr_helpers.lib.constants import columns, draws
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.constants import modelable_entities, parameters


def validate_description(description: str) -> None:
    """Checks that the description is 140 characters or less."""
    if len(description) > 140:
        raise exc.DescriptionTooLong(
            f"Description must be 140 characters or less, got {len(description)}"
        )


def validate_holdouts(holdouts: int) -> None:
    """Checks that holdouts are between 0 and 10."""
    if holdouts < 0 or holdouts > 10:
        raise exc.InvalidHoldoutNumber(f"Holdouts must be within 0-10, got {holdouts}")


def validate_draws(num_draws: int) -> None:
    """Checks that there are 0, 100, or 1000 draws."""
    if num_draws not in draws.ALLOWED_GPR_DRAWS:
        raise exc.InvalidDrawNumber(f"Draws must be 0, 100, or 1000, got {num_draws}")


def validate_density_cutoffs(density_cutoffs: List[int]) -> None:
    """Checks that density cutoffs are unique."""
    if len(density_cutoffs) != len(set(density_cutoffs)):
        raise exc.NonUniqueDensityCutoffs("Density cutoffs must be unique")


def validate_cross_validation(density_cutoffs: List[int], holdouts: int) -> None:
    """Checks that cross validation is not run with density cutoffs."""
    if density_cutoffs and holdouts:
        raise exc.CrossValidationWithDensityCutoffs(
            "Running cross validation with density cutoffs is not allowed."
        )


def validate_rake_logit(rake_logit: int, transform: str) -> None:
    """Checks that logit raking is only used with logit models."""
    if rake_logit and transform != TransformType.logit.name:
        raise exc.InvalidTransformWithLogitRaking(
            f"Cannot specify logit raking with transform {transform}"
        )


def validate_path_to_data_location(path_to_data: Optional[str]) -> None:
    """Checks that `path_to_data` exists."""
    if path_to_data and not os.path.exists(path_to_data):
        raise exc.InvalidPathToData(f"File {path_to_data} does not exist")


def validate_data_specification(
    crosswalk_version_id: Optional[int], bundle_id: Optional[int], path_to_data: Optional[str]
) -> None:
    """Checks that one of `path_to_data`, `crosswalk_version_id` is specified."""
    if not ((crosswalk_version_id and bundle_id) or path_to_data):
        raise exc.InvalidDataSpecification(
            f"Must specify either (crosswalk_version_id and bundle_id) or path_to_data. Got "
            f"crosswalk_version_id {crosswalk_version_id}, bundle_id {bundle_id}, "
            f"path_to_data {path_to_data}"
        )


def validate_single_custom_input(
    path_to_custom_stage_1: Optional[str], path_to_custom_covariates: Optional[str]
) -> None:
    """Checks that only one of custom stage 1 or custom covariates is present."""
    if path_to_custom_stage_1 and path_to_custom_covariates:
        raise exc.TwoCustomInputs("Cannot use both custom covariates and custom stage 1")


def validate_custom_stage_1(
    path_to_custom_stage_1: Optional[str],
    stage_1_model_formula: Optional[str],
) -> None:
    """Checks that exactly one of custom stage 1 and stage 1 model formula is provided."""
    if not path_to_custom_stage_1 and not stage_1_model_formula:
        raise exc.BothOrNeitherCustomStage1AndFormulaProvided(
            "You must provide either a stage 1 model formula or path to custom stage 1 "
            "inputs. Found neither"
        )
    if path_to_custom_stage_1 and stage_1_model_formula:
        raise exc.BothOrNeitherCustomStage1AndFormulaProvided(
            "You must provide either a stage 1 model formula or path to custom stage 1 "
            "inputs. Found both"
        )


def validate_hyperparameter_count(
    density_cutoffs: List[int],
    st_lambda: List[float],
    st_omega: List[float],
    st_zeta: List[float],
    gpr_scale: List[float],
) -> None:
    """Checks that hyperparameters have length density_cutoffs + 1."""
    if not density_cutoffs:
        return

    hyperparameters_to_validate = {
        parameters.ST_LAMBDA: st_lambda,
        parameters.ST_OMEGA: st_omega,
        parameters.ST_ZETA: st_zeta,
        parameters.GPR_SCALE: gpr_scale,
    }
    bad_hyperparameters = [
        param
        for param, param_value in hyperparameters_to_validate.items()
        if len(param_value) != len(density_cutoffs) + 1
    ]
    if bad_hyperparameters:
        hyperparam_names = ", ".join(hyperparameters_to_validate.keys())
        bad_hyperparam_names = ", ".join(bad_hyperparameters)
        raise exc.InvalidHyperparameterCount(
            f"{hyperparam_names} must all have length {len(density_cutoffs) + 1} (number of "
            "density cutoffs + 1). Found an invalid number of hyperparameters for the "
            f"following: {bad_hyperparam_names}"
        )


def validate_age_omega(
    density_cutoffs: List[int],
    holdouts: int,
    st_lambda: List[float],
    st_omega: List[float],
    st_zeta: List[float],
    gpr_scale: List[float],
    age_group_ids: List[int],
) -> None:
    """Checks that multiple omegas are not cross-validated in a single-age model."""
    model_type = model_type_utils.determine_model_type(
        density_cutoffs, holdouts, st_lambda, st_omega, st_zeta, gpr_scale
    )
    if (
        model_type in (ModelType.oos_selection, ModelType.in_sample_selection)
        and len(age_group_ids) == 1
        and len(st_omega) != 1
    ):
        raise exc.InvalidOmegaCrossValidation(
            "You are running a model with only one age group, but you're trying to run "
            "cross validation for different omegas. This is extremely inefficient for no "
            "added benefit"
        )


def validate_me(modelable_entity_id: int, stgpr_session: orm.Session) -> None:
    """Checks that ME exists and is not the generic ST-GPR ME."""
    if not db_stgpr.modelable_entity_exists(modelable_entity_id, stgpr_session):
        raise exc.NoModelableEntityFound(
            f"Modelable entity ID {modelable_entity_id} does not exist"
        )

    if modelable_entity_id == modelable_entities.GENERIC_STGPR_ME_ID:
        logging.warning(
            f"Generic ST-GPR ME {modelable_entity_id} was passed. This ME is only for "
            "one-off analyses, test models, or models that also use a covariate ID. It is "
            "not possible to save results for this ME, and it should be used with care"
        )


def validate_covariate_id(covariate_id: Optional[int], stgpr_session: orm.Session) -> None:
    """Checks that covariate ID exists."""
    if covariate_id and not db_stgpr.covariate_exists(covariate_id, stgpr_session):
        raise exc.NoCovariateFound(f"Covariate ID {covariate_id} does not exist")


def validate_covariate_name_shorts(
    covariate_name_shorts: List[str], stgpr_session: orm.Session
) -> None:
    """Checks that covariate name shorts exist."""
    for covariate_name_short in covariate_name_shorts:
        if not db_stgpr.covariate_name_short_exists(covariate_name_short, stgpr_session):
            raise exc.NoCovariateNameShortFound(
                f"Covariate {covariate_name_short} does not exist"
            )


def validate_crosswalk_version(
    crosswalk_version_id: Optional[int], epi_session: orm.Session
) -> None:
    """Checks that crosswalk version exists."""
    if crosswalk_version_id and not db_stgpr.crosswalk_version_exists(
        crosswalk_version_id, epi_session
    ):
        raise exc.NoCrosswalkVersionFound(
            f"Crosswalk version {crosswalk_version_id} does not exist"
        )


def validate_linked_bundle(
    crosswalk_version_id: Optional[int], bundle_id: Optional[int], epi_session: orm.Session
) -> None:
    """Checks that passed bundle is linked to passed crosswalk."""
    if not crosswalk_version_id:
        return

    associated_bundle_id = db_stgpr.get_linked_bundle_id(crosswalk_version_id, epi_session)
    if bundle_id != associated_bundle_id:
        raise exc.BundleDoesNotMatchCrosswalkVersion(
            f"Bundle ID {bundle_id} provided in the config does not match bundle ID "
            f"{associated_bundle_id} associated with crosswalk version ID "
            f"{crosswalk_version_id}"
        )


def validate_age_groups(prediction_age_group_ids: List[int], gbd_round_id: int) -> None:
    """For GBD 2020, checks that age groups are part of GBD 2020 age group set."""
    if gbd_round_id != 7:
        return

    active_age_group_ids = db_queries.get_age_metadata(gbd_round_id=gbd_round_id)[
        columns.AGE_GROUP_ID
    ].tolist()
    active_age_group_ids.append(gbd.constants.age.ALL_AGES)

    difference = set(prediction_age_group_ids).difference(active_age_group_ids)
    if difference:
        logging.warning(
            f"Using non-standard prediction age group IDs {difference} for GBD round ID "
            f"{gbd_round_id}. Standard age group set contains IDs: "
            f"{sorted(active_age_group_ids)}"
        )


def validate_parameters(
    params: Dict[str, Any],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
) -> None:
    """Validates all parameters."""
    _validate_base_parameters(params)
    _validate_hyperparameters(params)
    _validate_db_parameters(params, stgpr_session, epi_session)


def _validate_base_parameters(params: Dict[str, Any]) -> None:
    """Validates parameters that are not hyperparameters and that do not hit the database."""
    validate_description(params[parameters.DESCRIPTION])
    validate_holdouts(params[parameters.HOLDOUTS])
    validate_draws(params[parameters.GPR_DRAWS])
    validate_density_cutoffs(params[parameters.DENSITY_CUTOFFS])
    validate_cross_validation(params[parameters.DENSITY_CUTOFFS], params[parameters.HOLDOUTS])
    validate_rake_logit(params[parameters.RAKE_LOGIT], params[parameters.DATA_TRANSFORM])
    validate_path_to_data_location(params[parameters.PATH_TO_DATA])
    validate_data_specification(
        params[parameters.CROSSWALK_VERSION_ID],
        params[parameters.BUNDLE_ID],
        params[parameters.PATH_TO_DATA],
    )
    validate_single_custom_input(
        params[parameters.PATH_TO_CUSTOM_STAGE_1],
        params[parameters.PATH_TO_CUSTOM_COVARIATES],
    )
    validate_custom_stage_1(
        params[parameters.PATH_TO_CUSTOM_STAGE_1], params[parameters.STAGE_1_MODEL_FORMULA]
    )


def _validate_hyperparameters(params: Dict[str, Any]) -> None:
    """Validates hyperparameters."""
    validate_hyperparameter_count(
        params[parameters.DENSITY_CUTOFFS],
        params[parameters.ST_LAMBDA],
        params[parameters.ST_OMEGA],
        params[parameters.ST_ZETA],
        params[parameters.GPR_SCALE],
    )
    validate_age_omega(
        params[parameters.DENSITY_CUTOFFS],
        params[parameters.HOLDOUTS],
        params[parameters.ST_LAMBDA],
        params[parameters.ST_OMEGA],
        params[parameters.ST_ZETA],
        params[parameters.GPR_SCALE],
        params[parameters.PREDICTION_AGE_GROUP_IDS],
    )


def _validate_db_parameters(
    params: Dict[str, Any],
    stgpr_session: orm.Session,
    epi_session: orm.Session,
) -> None:
    validate_me(params[parameters.MODELABLE_ENTITY_ID], stgpr_session)
    validate_covariate_id(params[parameters.COVARIATE_ID], stgpr_session)
    validate_crosswalk_version(params[parameters.CROSSWALK_VERSION_ID], epi_session)
    validate_linked_bundle(
        params[parameters.CROSSWALK_VERSION_ID], params[parameters.BUNDLE_ID], epi_session
    )
    validate_age_groups(
        params[parameters.PREDICTION_AGE_GROUP_IDS], params[parameters.GBD_ROUND_ID]
    )
    validate_covariate_name_shorts(params[parameters.GBD_COVARIATES], stgpr_session)
