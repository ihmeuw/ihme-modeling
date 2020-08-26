from typing import Dict, Set

from gbd.constants import decomp_step as ds
from gbd.decomp_step import gbd_2019_decomp_step_id

##### PREVIOUS STEP BEST MODEL RULES #####
# Maps GBD round ID -> a set of (current) decomp steps
# that do not require a best model from the previous step.
_DECOMP_STEPS_TO_EXCLUDE_BEST_MODEL: Dict[int, Set[str]] = {
    5: {
        ds.ITERATIVE
    },
    6: {
        # Steps 1 and 2 were included for GBD 2019 because
        # decomp wasn't fully ready for ST-GPR until step 3.
        ds.ITERATIVE,
        ds.USA_HEALTH_DISPARITIES,
        ds.ONE,
        ds.TWO
    },
    7: {
        # Each GBD 2020 step is dependent on the previous step.
        # Health disparities is treated like iterative.
        ds.ITERATIVE,
        ds.USA_HEALTH_DISPARITIES
    }
}

# Actions to take if a model doesn't have a model marked best from the
# previous decomp step.
WARN: str = 'warn'  # for new MEs that wouldn't have previous models.
PASS: str = 'pass'  # for GBD 2019 step4 models that have either step 2/3 best.
ERROR: str = 'error'  # default action.

# Maps GBD round ID -> (current) decomp step -> action.
# Default action is ERROR, meaning round - decomp step
# pairs not in the table will throw errors.
_NO_PREVIOUS_MODEL_ACTION: Dict[int, Dict[str, str]] = {
    6: {
        ds.ONE: ERROR,
        ds.TWO: ERROR,
        ds.THREE: WARN,
        ds.FOUR: WARN
    },
    7: {
        # New MEs for existing (or new) causes are considered
        # methods updates and shouldn't occur until step 3.
        ds.TWO: ERROR,
        ds.THREE: WARN
    }
}

# Maps the decomp step ID of the final, offical step ->
# the final modeling step.
# EX: GBD 2019's last official step was step 5, but that
# step was really just for CoDCorrect. Modeler's real last step
# was step 4
FINAL_STEP_ID_TO_FINAL_MODELING_STEP = {
    gbd_2019_decomp_step_id[ds.FIVE]: ds.FOUR
}

##### CUSTOM COVARIATES RULES #####
# Actions that can be taken on custom covariates between decomp steps.
ADD_DROP = 'add_drop'
MODIFY_DATA = 'modify_data'

# Maps GBD round ID -> (current) decomp step -> action.
_COVARIATE_ACTIONS: Dict[int, Dict[str, Set[str]]] = {
    5: {
        ds.ITERATIVE: {ADD_DROP, MODIFY_DATA}
    },
    6: {
        ds.ONE: {ADD_DROP, MODIFY_DATA},
        ds.TWO: {ADD_DROP, MODIFY_DATA},
        ds.THREE: {ADD_DROP, MODIFY_DATA},
        ds.FOUR: {ADD_DROP, MODIFY_DATA}
    },
    7: {
        ds.TWO: set(),
        ds.THREE: {ADD_DROP, MODIFY_DATA},
    }
}

# Maps GBD round ID -> set of decomp steps for which filled covariates should
# be used
_COVARIATE_FILL_STEPS: Dict[int, Set[str]] = {
    5: set(),
    6: set(),
    7: {ds.TWO}
}

##### LOOKUP FUNCTIONS #####


def exclude_from_best_model_requirement(
        gbd_round_id: int,
        decomp_step: str
) -> bool:
    """
    Returns True iff a best model from the previous decomp
    step is not required for the given GBD round - decomp step pair.

    Returns:
        True iff decomp_step is in the set of steps that are excluded
        from the requirement that a previous model must be bested.
        False otherwise.

    Raises:
        RuntimeError: if GBD round ID is not in the map
    """
    if gbd_round_id not in _DECOMP_STEPS_TO_EXCLUDE_BEST_MODEL:
        raise RuntimeError(
            f'This request cannot be executed for GBD round id {gbd_round_id}'
        )
    return decomp_step in _DECOMP_STEPS_TO_EXCLUDE_BEST_MODEL[gbd_round_id]


def get_no_previous_model_action(
        gbd_round_id: int,
        decomp_step: str
) -> str:
    """
    Looks up which action should be performed if there are no models for
    a modelable entity in the given GBD round and previous decomp step.

    Returns:
        An action (WARN, PASS, or ERROR) to take for a model.

    Raises:
        RuntimeError: if the GBD round ID is not in the previous model
        actions map
    """
    if gbd_round_id not in _NO_PREVIOUS_MODEL_ACTION:
        raise RuntimeError(
            f'This request cannot be executed for GBD round id {gbd_round_id}'
        )
    gbd_round_action_map = _NO_PREVIOUS_MODEL_ACTION[gbd_round_id]
    if decomp_step not in gbd_round_action_map:
        raise RuntimeError(f'Valid gbd_round_id ({gbd_round_id}) but invalid '
                           f'decomp step (\'{decomp_step}\').')
    return gbd_round_action_map[decomp_step]


def get_custom_covariate_actions(
        gbd_round_id: int,
        decomp_step: str
) -> Set[str]:
    """
    Looks up which actions are allowed to be taken on custom covariates in the
    given GBD round and decomp step.

    Returns:
        A set of allowed actions (ADD_DROP, MODIFY_DATA, both, or neither)

    Raises:
        RuntimeError: if GBD round ID or decomp step is not present in the
            covariate actions map
    """
    if gbd_round_id not in _COVARIATE_ACTIONS:
        raise RuntimeError(
            f'Cannot pull custom covariates for GBD round {gbd_round_id}'
        )
    if decomp_step not in _COVARIATE_ACTIONS[gbd_round_id]:
        raise RuntimeError(
            f'Cannot pull custom covariates for GBD round {gbd_round_id} '
            f'decomp step {decomp_step}'
        )
    return _COVARIATE_ACTIONS[gbd_round_id][decomp_step]


def get_covariate_fill_steps(gbd_round_id: int) -> Set[str]:
    """
    Looks up which decomp steps use filled covariate estimates.

    Returns:
        A set of decomp steps in which filled covariates estimates should be
        used

    Raises:
        RuntimeError: if GBD round ID is not present in the covariate fill step
            lookup map
    """
    if gbd_round_id not in _COVARIATE_FILL_STEPS:
        raise RuntimeError(
            f'Cannot pull custom covariates for GBD round {gbd_round_id}'
        )
    return _COVARIATE_FILL_STEPS[gbd_round_id]
