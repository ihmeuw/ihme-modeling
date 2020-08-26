from typing import Any, Dict

from orm_stgpr.db import session_management
from orm_stgpr.lib import parameters
from orm_stgpr.lib.validation import stgpr_version_validation


def get_parameters(stgpr_version_id: int) -> Dict[str, Any]:
    """
    Pulls parameters associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull parameters

    Returns:
        Dictionary of parameters for an ST-GPR run

    Raises:
        ValueError: if stgpr_version_id is not in the database
    """
    with session_management.session_scope() as session:
        # Validate input.
        stgpr_version_validation.validate_stgpr_version_exists(
            stgpr_version_id, session
        )

        # Call library function to get parameters.
        return parameters.get_parameters(session, stgpr_version_id)
