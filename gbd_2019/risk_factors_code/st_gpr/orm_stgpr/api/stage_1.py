import pandas as pd

from orm_stgpr.db import session_management
from orm_stgpr.lib import stage_1
from orm_stgpr.lib.validation import stgpr_version_validation


def get_custom_stage_1_estimates(stgpr_version_id: int) -> pd.DataFrame:
    """
    Pulls custom stage 1 data associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull custom stage 1 estimates

    Returns:
        Dataframe of custom stage 1 estimates for an ST-GPR run or None if
        there are no custom stage 1 estimates for the given ST-GPR version ID

    Raises:
        ValueError: if stgpr_version_id is not in the database
    """
    with session_management.session_scope() as session:
        # Validate input.
        stgpr_version_validation.validate_stgpr_version_exists(
            stgpr_version_id, session
        )

        # Call library function to get parameters.
        return stage_1.get_custom_stage_1_estimates(session, stgpr_version_id)
