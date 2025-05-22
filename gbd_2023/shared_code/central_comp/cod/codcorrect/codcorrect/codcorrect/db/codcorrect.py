"""Utility functions for pulling info from the CoDCorrect database"""

import getpass
from typing import Optional

from sqlalchemy import orm

import db_tools_core

from codcorrect.legacy.utils.constants import Columns, ConnectionDefinitions
from codcorrect.lib.db.queries import Diagnostics


def create_new_diagnostic_version_row(
    version_id: int, session: Optional[orm.Session] = None
) -> None:
    """Creates a new row in TABLE for the CodCorrect run.

    Args:
        version_id: CodCorrect version id.
        session: Session with the CodCorrect database.
    """
    username = getpass.getuser()

    with db_tools_core.session_scope(
        ConnectionDefinitions.CODCORRECT, session=session
    ) as scoped_session:
        scoped_session.execute(
            Diagnostics.INSERT_DIAGNOSTIC_ROW,
            params={
                Columns.CODCORRECT_VERSION_ID: version_id,
                Columns.IS_BEST: 0,
                "inserted_by": username,
                "last_updated_by": username,
            },
        )


def delete_diagnostics(version_id: int, session: orm.Session) -> int:
    """Delete diagnostics rows in TABLE.

    This should be run when db summaries are deleted. Returns the number of rows
    deleted.

    Args:
        version_id: CodCorrect version id.
        session: Session with the CodCorrect database.
    """
    # Count rows
    row_count = int(
        session.execute(
            Diagnostics.ROW_COUNT, params={Columns.CODCORRECT_VERSION_ID: version_id}
        ).scalar()
    )

    # Delete all diagnostic rows, if there are any
    if row_count:
        session.execute(
            Diagnostics.DELETE_DIAGNOSTICS, params={Columns.CODCORRECT_VERSION_ID: version_id}
        )

    return row_count
