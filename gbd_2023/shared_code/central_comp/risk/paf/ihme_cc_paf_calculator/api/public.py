"""Top-level public ihme_cc_paf_calculator functions.

These functions are a facade over library code. Typically, they would start a session
with the appropriate database, initialize HTTP clients to external services, set up logging
and metrics, etc.

This structure allows us to change library code as needed while maintaining the same top-level
interface. It also ensures a clean separation of interface from implementation details.
"""

from typing import Optional

from sqlalchemy import orm

import db_tools_core
from gbd import conn_defs

from ihme_cc_paf_calculator.lib import monitor


def get_paf_model_status(model_version_id: int, session: Optional[orm.Session] = None) -> int:
    """Get PAF model status. Either success (0), running (1), or failed (2).

    Constants for PAF model status can be imported via
    `ihme_cc_paf_calculator.PafModelStatus.`

    It is not recommended to call this function more than once a minute to avoid
    unnecessary database calls.

    Args:
        model_version_id: PAF Calculator model_version_id, returned when a PAF model is
            launched
        session: optional session with the epi database

    Raises:
        ValueError: if no PAF model metadata is found for given model_version_id
        RuntimeError: if more than one row of PAF model metadata is found for
            given metadata. Internal error. Submit a help desk ticket if occurs

    Returns:
        0 if PAF model finished successfully; 1 if the model is still running, or 2
            if the model failed.
    """
    with db_tools_core.session_scope(conn_defs.EPI, session=session) as scoped_session:
        return monitor.get_paf_model_status(model_version_id, scoped_session)
