import logging
import os
from typing import Any, Dict, List, Optional
import warnings

import numpy as np
from sqlalchemy import orm

import db_queries
from elmo.util import (
    validate as elmo_validate,
    common as elmo_common,
    step_control as elmo_control
)
import gbd
from gbd.constants import decomp_step as ds
from gbd.decomp_step import (
    decomp_step_from_decomp_step_id,
    decomp_step_id_from_decomp_step
)
from rules import RulesManager, enums as rules_enums

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib import parameters as param_funcs
from orm_stgpr.lib.constants import (
    amp_method,
    columns,
    draws,
    parameters,
    modelable_entities
)
from orm_stgpr.lib.util import helpers, query


def validate_rules(
        rules_manager: Optional[RulesManager.RulesManager],
        params: Dict[str, Any],
        session: orm.Session
) -> None:
    """
    Verifies applicable rules:
        - decomp step is active
        - ST-GPR models can run
        - cause or rei is allowed to run for this step
          (ex: is part of current year for 3 year rotation)

    If rules_manager is None, implying that path_to_data has been provided,
    no rules are validated.

    Raises:
        ValueError: the rules are not being followed
    """
    gbd_round_id: Optional[int] = params[parameters.GBD_ROUND_ID]
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    if not rules_manager:
        return

    if not all(rules_manager.get_rule_values([
            rules_enums.Rules.STEP_ACTIVE,
            rules_enums.Rules.MODEL_CAN_RUN
    ])):
        raise ValueError(
            f'ST-GPR modeling is not allowed for decomp step '
            f'{decomp_step} and gbd round ID {gbd_round_id}'
        )

    modelable_entity_id: int = params[parameters.MODELABLE_ENTITY_ID]
    if modelable_entity_id == modelable_entities.GENERIC_STGPR_ME_ID:
        return

    id_of_interest, id_type = query.get_linked_cause_or_rei(
        modelable_entity_id, session)

    # covariate MEs don't have a base id they're associated with
    if not id_of_interest:
        return

    # Validate cause/rei is allowed to run for this round + step
    can_run_model = helpers.cause_or_rei_can_run_models(
        id_of_interest, id_type, gbd_round_id=gbd_round_id,
        decomp_step=decomp_step, rules_manager=rules_manager)

    if not can_run_model:
        raise ValueError(
            f'Modelable entity ID {modelable_entity_id} linked to {id_type} '
            f'ID {id_of_interest} is not in the list of {id_type}s that can '
            f'run for GBD round ID {gbd_round_id}, decomp step {decomp_step}. '
            f'If you believe this is a mistake, please submit a Help ticket.')


def validate_parameter_changes(
        rules_manager: RulesManager.RulesManager,
        best_model_id: Optional[int],
        params: Dict[str, Any],
        session: orm.Session
) -> None:
    """
    Validates that only allowed parameters changed between rounds.

    Args:
        rules_manager: RulesManager instance. Can be null
        best_model_id: best ST-GPR version ID from the previous decomp step.
            Has already been validated for existence, so a lack of
            best_model_id indicates an iterative run.
        params: parameters dictionary
        session: session with the epi database

    Raises:
        ValueError: if a parameter illegally changed values between decomp
            steps
    """
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    if not best_model_id:
        return

    # Get the parameters associated with the best model ID, and query the Rules
    # API to get a list of parameters that are (and aren't) allowed to change
    # for this decomp step.
    previous_step_params = param_funcs.get_parameters(session, best_model_id)
    params_can_change = set(rules_manager.get_rule_value(
        rules_enums.Rules.MODEL_PARAMETERS_CAN_CHANGE_ST_GPR
    ))
    params_cant_change = set(parameters.PARAMETER_SKELETON.keys()) - \
        params_can_change

    # Compare parameters that aren't allowed to change against the values
    # of those parameters for the previous decomp step.
    for param in params_cant_change:
        if param in params and previous_step_params[param] != params[param]:
            # If ST-GPR previously calculated an offset, then is_custom_offset
            # will be false. In this case, transform_offset must be None (i.e.
            # telling ST-GPR to calculate an offset in this run, too).
            if param == parameters.TRANSFORM_OFFSET and \
                    not previous_step_params[parameters.IS_CUSTOM_OFFSET] and \
                    params[parameters.TRANSFORM_OFFSET] is None:
                continue

            # For float parameters, check that parameters are close rather
            # than exactly the same.
            if isinstance(params[param], float) and \
                    isinstance(previous_step_params[param], float) and \
                    np.isclose(params[param], previous_step_params[param]):
                continue

            raise ValueError(
                f'Parameter {param} cannot have different values between '
                f'decomp step {decomp_step} and the previous decomp step. '
                f'Value changed from {previous_step_params[param]} to '
                f'{params[param]}'
            )


def validate_parameters(params: Dict[str, Any], session: orm.Session) -> None:
    """Validates config parameters and hyperparameters"""
    _validate_base_parameters(params, session)
    _validate_hyperparameters(params)
    _validate_age_omega(params)


def _validate_base_parameters(
        params: Dict[str, Any],
        session: orm.Session
) -> None:
    """Validates all parameters except hyperparameters"""
    logging.info('Validating parameters')

    # Validate parameters that don't require external calls.
    _validate_holdouts(params[parameters.HOLDOUTS])
    _validate_draws(params[parameters.GPR_DRAWS])
    _validate_offset(
        params[parameters.TRANSFORM_OFFSET],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP]
    )
    _validate_density_cutoffs(params[parameters.DENSITY_CUTOFFS])
    _validate_amp_method(
        params[parameters.GPR_AMP_METHOD],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP]
    )
    _validate_rake_logit(
        params[parameters.RAKE_LOGIT],
        params[parameters.DATA_TRANSFORM]
    )
    _validate_path_to_data_decomp_step(
        params[parameters.PATH_TO_DATA],
        params[parameters.DECOMP_STEP]
    )
    _validate_path_to_data_or_crosswalk_version_id(
        params[parameters.CROSSWALK_VERSION_ID],
        params[parameters.BUNDLE_ID],
        params[parameters.PATH_TO_DATA]
    )

    # Validate parameters that need to hit the database.
    _validate_me_id_covariate_id(
        params[parameters.MODELABLE_ENTITY_ID],
        params[parameters.COVARIATE_ID],
        session,
    )
    _validate_crosswalk_version_id(
        params[parameters.CROSSWALK_VERSION_ID],
        params[parameters.BUNDLE_ID],
        params[parameters.PATH_TO_DATA],
        session
    )

    _validate_age_groups(
        params[parameters.PREDICTION_AGE_GROUP_IDS],
        params[parameters.GBD_ROUND_ID]
    )
    _validate_in_sync(
        params[parameters.BUNDLE_ID],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        params[parameters.CROSSWALK_VERSION_ID],
        session
    )


def _validate_holdouts(holdouts: int) -> None:
    if holdouts < 0 or holdouts > 10:
        raise ValueError(f'Holdouts must be within 0-10, got {holdouts}')


def _validate_draws(num_draws: int) -> None:
    if num_draws not in draws.ALLOWED_GPR_DRAWS:
        raise ValueError(f'Draws must be 0, 100, or 1000, got {num_draws}')


def _validate_offset(offset: Optional[float], gbd_round_id: int,
                     decomp_step: str) -> None:
    """ In early GBD 2019, offset had to be specified.
    Now it can be imputed within ST-GPR (as this was a methods change)
    or specified by modeler.
    """
    if offset is None and helpers.use_old_methods(gbd_round_id, decomp_step) \
            and gbd_round_id == 6:
        raise ValueError(
            f'Must specify an offset for GBD round ID {gbd_round_id}, '
            f'decomp step {decomp_step}.'
        )


def _validate_density_cutoffs(density_cutoffs: List[int]) -> None:
    if len(density_cutoffs) != len(set(density_cutoffs)):
        raise ValueError('Density cutoffs must be unique')


def _validate_amp_method(gpr_amp_method: str, gbd_round_id: int, decomp_step: str) -> None:
    """ amp_method.PROD was phased out in the middle of GBD 2019 (6) and so should
    be invalid for all later rounds.
    """
    if gpr_amp_method == amp_method.PROD and \
        (not helpers.use_old_methods(gbd_round_id, decomp_step)
         or gbd_round_id > 6):
        raise ValueError(
            f'Amplitude method {amp_method.PROD} is no longer available. '
            f'Please choose {amp_method.GLOBAL_ABOVE_CUTOFF} or '
            f'{amp_method.BROKEN_STICK} instead'
        )


def _validate_rake_logit(rake_logit: int, transform: str) -> None:
    if rake_logit and transform != lookup_tables.TransformType.logit.name:
        raise ValueError(
            f'Cannot specify logit raking with transform {transform}'
        )


def _validate_path_to_data_decomp_step(
        path_to_data: Optional[str],
        decomp_step: str
) -> None:
    if path_to_data and decomp_step != gbd.constants.decomp_step.ITERATIVE:
        raise ValueError(
            'path_to_data can only be used with decomp_step '
            f'{gbd.constants.decomp_step.ITERATIVE}'
        )


def _validate_path_to_data_or_crosswalk_version_id(
        crosswalk_version_id: Optional[int],
        bundle_id: Optional[int],
        path_to_data: Optional[str]
) -> None:
    if not ((crosswalk_version_id and bundle_id) or path_to_data):
        raise ValueError(
            f'Must specify either (crosswalk_version_id and bundle_id) or '
            f'path_to_data. Got crosswalk_version_id {crosswalk_version_id}, '
            f'bundle_id {bundle_id}, path_to_data {path_to_data}'
        )

    if path_to_data and not os.path.exists(path_to_data):
        raise FileNotFoundError(f'File {path_to_data} does not exist')


def _validate_me_id_covariate_id(
        modelable_entity_id: int,
        covariate_id: Optional[int],
        session: orm.Session
) -> None:
    if not query.modelable_entity_id_exists(modelable_entity_id, session):
        raise ValueError(
            f'Modelable entity ID {modelable_entity_id} not found in the '
            'database'
        )
    if covariate_id and not \
            query.covariate_id_exists(covariate_id, session):
        raise ValueError(
            f'Covariate ID {covariate_id} not found in the database'
        )
    if modelable_entity_id == modelable_entities.GENERIC_STGPR_ME_ID:
        warnings.warn(
            f'Generic ST-GPR ME {modelable_entity_id} was passed. This ME is '
            'only for one-off analyses, test models, or models that also use '
            'a covariate ID. It is not possible to save results for this ME, '
            'and it should be used with care'
        )


def _validate_crosswalk_version_id(
        crosswalk_version_id: Optional[int],
        bundle_id: Optional[int],
        path_to_data: Optional[str],
        session: orm.Session
) -> None:
    if path_to_data:
        return

    associated_bundle_id = query.get_bundle_id_from_crosswalk(
        crosswalk_version_id, session
    )
    if bundle_id != associated_bundle_id:
        raise ValueError(
            f'Bundle ID {bundle_id} provided in the config does not match '
            f'bundle ID {associated_bundle_id} associated with crosswalk '
            f'version ID {crosswalk_version_id}'
        )


def _validate_age_groups(
        prediction_age_group_ids: List[int],
        gbd_round_id: int
) -> None:
    """Raises a warning for age groups that aren't part of the GBD 2020
    age group set or age group 22 (all ages), but only if running for
    GBD 2020."""
    if gbd_round_id == 7:
        gbd_round_to_age_group_set = {
            7: 19
        }

        active_age_group_ids = db_queries.get_age_metadata(
            age_group_set_id=gbd_round_to_age_group_set[gbd_round_id],
            gbd_round_id=gbd_round_id
        )[columns.AGE_GROUP_ID].tolist()

        active_age_group_ids.append(gbd.constants.age.ALL_AGES)
        difference = set(prediction_age_group_ids).difference(
            active_age_group_ids)

        if difference:
            warnings.warn(
                f'Using non-standard prediction age group IDs {difference} for '
                f'GBD round ID {gbd_round_id}. Standard age group set contains '
                f'IDs: {sorted(active_age_group_ids)}'
            )


def _validate_in_sync(
        bundle_id: int,
        gbd_round_id: int,
        decomp_step: str,
        crosswalk_version_id: Optional[int],
        session: orm.Session
) -> None:
    """
    Thin wrapper for elmo's validate_in_sync.
    We only want to call the function when the crosswalk version exists.
    """
    if not crosswalk_version_id:
        return

    try:
        elmo_validate.validate_in_sync(
            bundle_id=bundle_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            session=session,
            crosswalk_version_id=crosswalk_version_id
        )
    except ValueError as e:
        # No previous best model found
        if decomp_step != ds.TWO:
            previous_step = decomp_step_from_decomp_step_id(
                helpers.get_previous_decomp_step_id(
                    decomp_step_id_from_decomp_step(
                        decomp_step, gbd_round_id)))
            previous_step_str = ' AND the previous decomp step (GBD round ' \
                f'ID {gbd_round_id}, {previous_step})'
        else:
            previous_step_str = ''
        raise ValueError(
            str(e) +
            '\n\nYou must have a best model with an '
            f'associated crosswalk version from the previous round '
            f'(GBD round ID {gbd_round_id - 1}){previous_step_str}. '
            'If this is a custom bundle, don\'t pass in a crosswalk version.')


def _validate_hyperparameters(params: Dict[str, Any]) -> None:
    """
    Validates that hyperparameters have length density_cutoffs + 1.

    Density cutoffs are used to assign different hyperparameters to each
    location based on the number of country-years of data available in that
    location. Specifying N density_cutoffs implies N + 1 different
    hyperparameters to assign. For example, density_cutoffs = 5,10,15 means
    different hyperparameters for locations with 0-4 country-years of data,
    5-9 country-years of data, 10-14 country-years of data, and 15+ country-years
    of data.
    """
    logging.info('Validating hyperparameters')
    density_cutoffs: List[int] = params[parameters.DENSITY_CUTOFFS]
    holdouts: int = params[parameters.HOLDOUTS]

    if not density_cutoffs:
        return

    if holdouts:
        raise ValueError(
            'Running cross validation with density cutoffs is not allowed.'
        )

    hyperparameters_to_validate = {
        parameters.ST_LAMBDA: params[parameters.ST_LAMBDA],
        parameters.ST_OMEGA: params[parameters.ST_OMEGA],
        parameters.ST_ZETA: params[parameters.ST_ZETA],
        parameters.GPR_SCALE: params[parameters.GPR_SCALE]
    }
    bad_hyperparameters = [
        param for param, param_value in hyperparameters_to_validate.items()
        if len(param_value) != len(density_cutoffs) + 1
    ]
    if bad_hyperparameters:
        raise ValueError(
            f'{", ".join(hyperparameters_to_validate.keys())} must all have '
            f'length {len(density_cutoffs) + 1} (number of density cutoffs '
            f'+ 1). Found an invalid number of hyperparameters for the '
            f'following: {", ".join(bad_hyperparameters)}'
        )


def _validate_age_omega(params: Dict[str, Any]) -> None:
    age_group_ids: List[int] = params[parameters.PREDICTION_AGE_GROUP_IDS]
    st_omega: List[float] = params[parameters.ST_OMEGA]

    run_type = helpers.determine_run_type(params)
    if run_type in (
            lookup_tables.RunType.oos_selection,
            lookup_tables.RunType.in_sample_selection
    ) and len(age_group_ids) == 1 and len(st_omega) != 1:
        raise ValueError(
            'You are running a model with only one age group, but you\'re '
            'trying to run crossval for different omegas. This is wildly '
            'computationally inefficient for no added benefit.'
        )
