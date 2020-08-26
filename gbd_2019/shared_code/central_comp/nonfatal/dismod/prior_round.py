"""
Functionality for reasoning about 'last round best model'. Used in decomp
specific logic (ie what csmr to use)
"""
from typing import Optional

import gbd
from gbd.constants import decomp_step as ds
from gbd.decomp_step import decomp_step_id_from_decomp_step
from rules.enums import ResearchAreas, Rules, Tools
from rules.RulesManager import RulesManager

from cascade_ode.constants import Queries
from cascade_ode.db import execute_select


def get_last_round_best_model(model_version_id: int) -> Optional[int]:
    """
    Given a model version id, return the model version id of the
    best model from the prior round.

    """
    res = execute_select(
        Queries.GET_CAUSE_OR_REI_ID,
        params={'model_version_id': model_version_id})

    rei_missing = res.rei_id.isna().iat[0]
    cause_missing = res.cause_id.isna().iat[0]

    if rei_missing and cause_missing:
        raise RuntimeError("Could not find cause or rei associated with model")

    if not rei_missing and not cause_missing:
        raise RuntimeError(
            "Found both cause and rei associated with model: "
            f"{res.to_dict('list')}")

    cause_id = res.cause_id.get(0)
    rei_id = res.rei_id.get(0)
    current_gbd_round_id = res.gbd_round_id.get(0)
    last_gbd_round_id = current_gbd_round_id - 1
    step_id_for_rule = decomp_step_id_from_decomp_step(
        gbd.constants.decomp_step.TWO, current_gbd_round_id)

    can_run_model_rule, id_of_interest = (
        (Rules.CAUSES_CAN_RUN_MODELS, cause_id)
        if cause_id
        else (Rules.REIS_CAN_RUN_MODELS, rei_id)
    )

    rules_manager = RulesManager(
        ResearchAreas.EPI, Tools.ELMO, decomp_step_id=step_id_for_rule)
    can_run_model = rules_manager.get_rule_value(can_run_model_rule) or []

    final_step_id = decomp_step_id_from_decomp_step(
        final_step_per_round[last_gbd_round_id], last_gbd_round_id)

    step_id_to_check = (
        final_step_id
        if id_of_interest in can_run_model
        else decomp_step_id_from_decomp_step(
            # iterative is the fall back
            gbd.constants.decomp_step.ITERATIVE, last_gbd_round_id)
    )
    possible_step_ids_to_check = [decomp_step_id_from_decomp_step(
        step, last_gbd_round_id) for step in [ds.FOUR, ds.ITERATIVE]]

    actual_step_ids_to_check = possible_step_ids_to_check[
        possible_step_ids_to_check.index(step_id_to_check):]

    for step_id in actual_step_ids_to_check:
        best_model_version_id = execute_select(
            Queries.BEST_MODEL_LAST_STEP,
            params={
                "model_version_id": model_version_id,
                "decomp_step_id": step_id
            }
        )
        if best_model_version_id.empty:
            continue
        break
    return best_model_version_id.model_version_id.get(0)


final_step_per_round = {
    6: gbd.constants.decomp_step.FOUR
}
