"""General helper functions."""

import itertools
from typing import Any, Dict, Generator, List, Optional, Callable

import numpy as np
import pandas as pd

import gbd
from gbd.constants import decomp_step as ds
from rules import RulesManager, enums as rules_enums

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import (
    columns, parameters, methods, modelable_entities
)


def get_previous_decomp_step_id(decomp_step_id: int) -> int:
    """
    Gets the decomp step ID associated with the previous decomp step.
    Returns exactly the previous decomp step, agnostic of what's relevant
    for modeling purposes.

    Args:
        decomp_step_id: the decomp step ID for which to get the previous
            step's decomp step ID

    Returns:
        Decomp step ID of the previous decomp step
    Raises:
        KeyError if decomp_step_id cannot be mapped to a previous decomp step
    """
    if decomp_step_id not in gbd.constants.previous_decomp_step_id:
        raise KeyError(f'Decomp step id \'{decomp_step_id}\' does not exist '
                       f'in the previous decomp step map. Keys: '
                       f'{gbd.constants.previous_decomp_step_id.keys()}')

    return gbd.constants.previous_decomp_step_id[decomp_step_id]


def determine_run_type(params: Dict[str, Any]) -> lookup_tables.RunType:
    """
    Determines which type of ST-GPR model is running.
    See the RunType enum docstring for a description of different run types.

    Args:
        holdouts: number of holdouts specified in config
        density_cutoffs: list of density cutoffs specified in config
        st_params: spacetime parameters specified in config
        gpr_params: GPR parameters specified in config

    Returns:
        The type of ST-GPR model being run

    Raises:
        ValueError: if the parameters don't indicate a valid run type
    """
    density_cutoffs: List[int] = params[parameters.DENSITY_CUTOFFS]
    holdouts: int = params[parameters.HOLDOUTS]

    max_num_params = max(
        len(params[parameters.ST_LAMBDA]),
        len(params[parameters.ST_OMEGA]),
        len(params[parameters.ST_ZETA]),
        len(params[parameters.GPR_SCALE])
    )
    has_cutoffs = density_cutoffs and density_cutoffs != [0]
    if not has_cutoffs and not holdouts and max_num_params == 1:
        return lookup_tables.RunType.base
    elif not has_cutoffs and not holdouts and max_num_params > 1:
        return lookup_tables.RunType.in_sample_selection
    elif not has_cutoffs and holdouts and max_num_params == 1:
        return lookup_tables.RunType.oos_evaluation
    elif not has_cutoffs and holdouts and max_num_params > 1:
        return lookup_tables.RunType.oos_selection
    elif has_cutoffs and not holdouts and max_num_params > 1:
        return lookup_tables.RunType.dd

    raise ValueError((
        'You CANNOT specify a model with density cutoffs for a crossval run. '
        'You will end up with so many parameter configurations that this will '
        'basically never finish. Leave density_cutoffs blank.'
    ))


def param_sets_cartesian(
        table: Any,
        params: Dict[str, Any]
) -> Generator[Any, None, None]:
    cartesian_params = table.crossval_params
    other_params = [
        param for param in params
        if param in table.single_params or param in table.list_params
    ]
    cartesian = itertools.product(
        *[np.unique(params[cp]) for cp in cartesian_params]
    )
    for param_set in cartesian:
        row_vals = {cp: param_set[i] for i, cp in enumerate(cartesian_params)}
        row_vals.update({op: params[op] for op in other_params})
        yield table(**row_vals)


def param_sets_density_cutoffs(
        table: Any,
        params: Dict[str, Any],
        density_cutoffs: List[int]
) -> Generator[Any, None, None]:
    for i in range(len(density_cutoffs)):
        param_set = {
            param_name: param_value[i]
            for param_name, param_value in params.items()
            if param_name in table.crossval_params
        }
        param_set.update({p: params[p] for p in table.list_params})
        param_set.update({p: params[p] for p in table.single_params})
        yield table(**param_set)


def nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where((pd.notnull(df)), None)


def join_or_none(to_join: List[Any]) -> Optional[str]:
    return ','.join([str(item) for item in to_join]) if to_join else None


def sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts columns first by demographics then alphabetically"""
    return df[columns.DEMOGRAPHICS + sorted([
        col for col in df if col not in columns.DEMOGRAPHICS
    ])]


def use_old_methods(gbd_round_id: int, decomposition_step: str) -> bool:
    """Whether to use old or updated ST-GPR methods from given GBD round
    for the decomp step. Returns true iff the old methods should be used."""
    if gbd_round_id not in methods.USE_OLD_METHODS_MAP:
        raise RuntimeError(f'GBD round ID {gbd_round_id} is not currently '
                           f'in methods.USE_OLD_METHODS_MAP.')

    return decomposition_step in\
        methods.USE_OLD_METHODS_MAP[gbd_round_id]


def lookup_method(method: str, gbd_round_id: int,
                  decomposition_step: str) -> Callable:
    """ Lookup the proper implementation of a method given a GBD round and
    decomp step. This allows methods changes for a round to be pulled in
    automatically when allowed.

    Returns:
        method function appropriate for GBD round and decomp step.
        If the method does not have an entry for the given GBD round ID,
        the most recently updated method function is returned

    Raises:
        RuntimeError if method is not in map
    """
    if method not in methods.LOOKUP_METHODS_MAP:
        raise RuntimeError(f'Method \'{method}\' does not exist in ',
                           f'methods.LOOKUP_METHODS_MAP. Available methods: '
                           f'{list(methods.LOOKUP_METHODS_MAP.keys())}')

    methods_map = methods.LOOKUP_METHODS_MAP[method]
    old_methods = use_old_methods(gbd_round_id, decomposition_step)

    if gbd_round_id not in methods_map:
        # Method has not been changed this round, so look up
        # most recently updated method version and return it
        gbd_round_id = sorted(list(methods_map.keys()))[-1]
        old_methods = False

    return methods_map[gbd_round_id][old_methods]


def subset_data_by_demographics(
        data_df: pd.DataFrame,
        location_ids: List[int],
        year_ids: List[int],
        age_group_ids: List[int],
        sex_ids: List[int]
) -> None:
    return data_df[
        (data_df[columns.LOCATION_ID].isin(location_ids)) &
        (data_df[columns.YEAR_ID].isin(year_ids)) &
        (data_df[columns.AGE_GROUP_ID].isin(age_group_ids)) &
        (data_df[columns.SEX_ID].isin(sex_ids))
    ]


def cause_or_rei_can_run_models(
        id_of_interest: Optional[int],
        id_type: str,
        gbd_round_id: Optional[int] = None,
        decomp_step: Optional[str] = None,
        rules_manager: Optional[RulesManager.RulesManager] = None
) -> bool:
    """
    Returns True iff the cause/rei id is allowed to run models
    for the given GBD round and decomp step (or according to
    the rules manager, whichever is not None). If rules_manager
    is passed in, that overrides GBD round and decomp step.

    Note:
        Causes/REIs are always allowed to run in iterative,
        no matter what.

    Args:
        id_of_interest: either cause of rei id
        id_type: 'cause' or 'rei'

    Raises:
        ValueError if id_type is not in allowed list
    """
    allowed_id_types = [modelable_entities.CAUSE, modelable_entities.REI]
    if id_type not in allowed_id_types:
        raise ValueError(
            f'Invalid id_type: {id_type}. Must be in {allowed_id_types}')

    if decomp_step == ds.ITERATIVE:
        return True

    if not rules_manager:
        if gbd_round_id and decomp_step:
            rules_manager = RulesManager.RulesManager(
                rules_enums.ResearchAreas.EPI,
                rules_enums.Tools.STGPR,
                decomp_step,
                gbd_round_id=gbd_round_id
            )
        else:
            raise ValueError(
                "Must pass in (gbd_round_id and decomp_step) OR rules_manager.")

    can_run_model_rule = (
        rules_enums.Rules.CAUSES_CAN_RUN_MODELS
        if id_type == modelable_entities.CAUSE
        else rules_enums.Rules.REIS_CAN_RUN_MODELS
    )
    can_run_model = rules_manager.get_rule_value(can_run_model_rule) or []
    return id_of_interest in can_run_model
