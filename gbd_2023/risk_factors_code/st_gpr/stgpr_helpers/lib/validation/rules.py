from typing import Optional

import ihme_cc_rules_client
from ihme_cc_rules_client import Rules

from stgpr_helpers.lib.constants import exceptions as exc


def validate_model_can_run(
    rules_manager: Optional[ihme_cc_rules_client.RulesManager], release_id: int
) -> None:
    """Checks that a model is allowed to run in a given release.

    Args:
        rules_manager: an instantiated Rules Manager.
        release_id: ID of the release to check

    Raises:
        ModelCantRun: if the release is inactive or models are not allowed to run.
    """
    if not rules_manager:
        return

    if not all(rules_manager.get_rule_values([Rules.STEP_ACTIVE, Rules.MODEL_CAN_RUN])):
        raise exc.ModelCantRun(f"ST-GPR modeling is not allowed for release id {release_id}")
