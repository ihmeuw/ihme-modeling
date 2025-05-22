"""Utility functions for pulling info from the CoD database"""

from typing import Optional

from sqlalchemy import orm

import db_tools_core
from gbd import conn_defs

from codcorrect.legacy.utils.constants import COD, Best, Columns, Status
from codcorrect.lib.db.queries import CoDCorrectVersion


def create_new_cod_output_version_row(
    cod_output_version_id: int,
    description: str,
    release_id: int,
    code_version: str,
    env_version: int,
    session: Optional[orm.Session] = None,
) -> None:
    """Creates a new row for the CodCorrect run in TABLE.

    Note:
        cod_output_version_id is not necessarily the same value as the CodCorrect version.

    Args:
        cod_output_version_id: CoD output version id, the version ID of the CodCorrect
            run for the CoD output version table.
        description: description of the CodCorrect run for the CoD database.
        release_id: release ID for the run
        code_version: git hash of the code version.
        env_version: Version of the non-shock mortality envelope used.
        session: Session with the CoD database.
    """
    with db_tools_core.session_scope(conn_defs.COD, session=session) as scoped_session:
        scoped_session.execute(
            CoDCorrectVersion.INSERT_NEW_VERSION_ROW,
            params={
                Columns.OUTPUT_VERSION_ID: cod_output_version_id,
                Columns.OUTPUT_PROCESS_ID: COD.DataBase.OUTPUT_PROCESS_ID,
                Columns.TOOL_TYPE_ID: COD.DataBase.TOOL_TYPE_ID,
                Columns.RELEASE_ID: release_id,
                Columns.DESCRIPTION: description,
                Columns.CODE_VERSION: code_version,
                Columns.ENV_VERSION: env_version,
                Columns.STATUS: Status.SUBMITTED,
                Columns.IS_BEST: Best.NOT_BEST,
            },
        )


def create_cod_output_version_metadata(
    cod_output_version_id: int,
    codcorrect_version_id: int,
    session: Optional[orm.Session] = None,
) -> None:
    """Creates metadata row linking COD output version to CodCorrect version.

    Args:
        cod_output_version_id: CoD output version id, the version ID of the CodCorrect
            run for the CoD output version table.
        codcorrect_version_id: CoDCorrect version ID
        session: Session with the CoD database.
    """
    with db_tools_core.session_scope(conn_defs.COD, session=session) as scoped_session:
        scoped_session.execute(
            CoDCorrectVersion.INSERT_NEW_OUTPUT_VERSION_METADATA,
            params={
                Columns.OUTPUT_VERSION_ID: cod_output_version_id,
                Columns.METADATA_TYPE_ID: COD.DataBase.METADATA_TYPE_ID,
                Columns.VALUE: codcorrect_version_id,
            },
        )


def get_new_cod_output_version_id(session: Optional[orm.Session] = None) -> int:
    """Gets a new COD output version for the CodCorrect run.

    Takes the largest output_version_id from cod.output_version and adds 1.

    Args:
        session: Session with the COD database.
    """
    with db_tools_core.session_scope(conn_defs.COD, session=session) as scoped_session:
        return int(
            scoped_session.execute(CoDCorrectVersion.GET_NEW_COD_OUTPUT_VERSION_ID).scalar()
        )


def update_status(
    cod_output_version_id: int, status: int, session: Optional[orm.Session] = None
) -> None:
    """Updates the status of the CodCorrect run in the CoD database (TABLE).

    Args:
        cod_output_version_id: CoD output version id, the version ID of the CodCorrect
            run for the CoD output version table.
        status: Status id to update the CodCorrect run to. 0 (running), 1 (complete),
            2 (submitted), 3 (deleted).
        session: Session with the CoD database.
    """
    with db_tools_core.session_scope(conn_defs.COD, session=session) as scoped_session:
        scoped_session.execute(
            CoDCorrectVersion.UPDATE_STATUS,
            params={Columns.STATUS: status, Columns.OUTPUT_VERSION_ID: cod_output_version_id},
        )


def create_cod_output_table(cod_output_version_id: int, session: orm.Session) -> str:
    """Create output table to upload to in cod database.

    The sproc will fail if data already exists for the cod_output_version_id.

    Args:
        cod_output_version_id: CoD output version id, the version ID of the CodCorrect
            run for the CoD output version table.
        session: Session with the CoD database.

    Returns:
        name of the output upload table
    """
    session.execute(
        """SPROC CALL""",
        params={"cod_output_version_id": cod_output_version_id},
    )

    return "TABLE"


def finalize_cod_output_table(cod_output_version_id: int, session: orm.Session) -> None:
    """Finalize output table in cod database.

    Upkeep after creating a table for a CodCorrect version and then uploading.
    Should be called immediately after uploading to the cod db.

    Args:
        cod_output_version_id: CoD output version id, the version ID of the CodCorrect
            run for the CoD output version table.
        session: Session with the CoD database.
    """
    session.execute(
        """SPROC CALL""",
        params={"cod_output_version_id": cod_output_version_id},
    )
