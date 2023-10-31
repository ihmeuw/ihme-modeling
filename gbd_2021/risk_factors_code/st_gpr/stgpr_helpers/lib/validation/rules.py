from typing import Optional

from rules import enums as rules_enums
from rules.RulesManager import RulesManager

from stgpr_helpers.lib.constants import exceptions as exc


def validate_model_can_run(
    rules_manager: Optional[RulesManager], gbd_round_id: int, decomp_step: str
) -> None:
    """Checks that a model is allowed to run in a given step.

    Args:
        rules_manager: an instantiated Rules Manager.
        gbd_round_id: ID of the GBD round to check.
        decomp_step: step to check.

    Raises:
        ValueError: if the step is inactive or models are not allowed to run.
    """
    if not rules_manager:
        return

    if not all(
        rules_manager.get_rule_values(
            [rules_enums.Rules.STEP_ACTIVE, rules_enums.Rules.MODEL_CAN_RUN]
        )
    ):
        raise exc.ModelCantRun(
            f"ST-GPR modeling is not allowed for decomp step {decomp_step} and gbd round ID "
            f"{gbd_round_id}"
        )
