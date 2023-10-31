import getpass
from typing import Optional, Tuple

from sqlalchemy import orm
import pandas as pd

from db_tools import ezfuncs, query_tools
from gbd import decomp_step

from fauxcorrect.queries.queries import CoDCorrectVersion, Diagnostics
from fauxcorrect.utils.constants import (
    Best, COD, Columns, ConnectionDefinitions, GBD, Status
)
from fauxcorrect.utils.exceptions import MoreThanOneBestVersion


def create_new_cod_output_version_row(
        cod_output_version_id: int,
        description: str,
        decomp_step_id: int,
        code_version: str,
        env_version: int
) -> None:
    """Creates a new row for the CodCorrect run in cod.output_version."""
    query_tools.exec_query(
        CoDCorrectVersion.INSERT_NEW_VERSION_ROW,
        session=ezfuncs.get_session(ConnectionDefinitions.COD),
        close=True,
        parameters={
            Columns.OUTPUT_VERSION_ID: cod_output_version_id,
            Columns.TOOL_TYPE_ID: COD.DataBase.TOOL_TYPE_ID,
            Columns.DECOMP_STEP_ID: decomp_step_id,
            Columns.DESCRIPTION: description,
            Columns.CODE_VERSION: code_version,
            Columns.ENV_VERSION: env_version,
            Columns.STATUS: Status.SUBMITTED,
            Columns.IS_BEST: Best.NOT_BEST
        }
    )


def create_output_partition(cod_output_version_id) -> None:
    query_tools.exec_query(
        CoDCorrectVersion.CREATE_OUTPUT_PARTITION,
        session=ezfuncs.get_session(ConnectionDefinitions.COD),
        close=True,
        parameters={
            'schema': COD.DataBase.SCHEMA,
            'table': COD.DataBase.TABLE,
            Columns.OUTPUT_VERSION_ID: cod_output_version_id
        }
    )


def get_new_codcorrect_version_id() -> int:
    """Gets a new CodCorrect version for the CodCorrect run.

    Takes the largest CodCorrect version from gbd.gbd_process_version_metadata and adds 1.
    """
    return int(ezfuncs.query(
        CoDCorrectVersion.GET_NEW_CODCORRECT_VERSION_ID,
        conn_def=ConnectionDefinitions.GBD
    )[Columns.CODCORRECT_VERSION_ID].iat[0])


def get_new_cod_output_version_id() -> int:
    """Gets a new COD output version for the CodCorrect run.

    Takes the largest output_version_id from cod.output_version and adds 1.
    """
    return int(ezfuncs.query(
        CoDCorrectVersion.GET_NEW_COD_OUTPUT_VERSION_ID,
        conn_def=ConnectionDefinitions.COD
    )[Columns.OUTPUT_VERSION_ID].iat[0])


def update_status(codcorrect_version_id: int, status: int) -> None:
    ezfuncs.query(
        CoDCorrectVersion.UPDATE_STATUS,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.STATUS: status,
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id
        }
    )


def update_best(codcorrect_version_id: int, is_best: int) -> None:
    ezfuncs.query(
        CoDCorrectVersion.UPDATE_BEST,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id,
            Columns.IS_BEST: is_best
        }
    )


def get_current_best(decomp_step_id: int) -> Optional[int]:
    q = ezfuncs.query(
        CoDCorrectVersion.GET_CURRENT_BEST_VERSION,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.TOOL_TYPE_ID: COD.DataBase.TOOL_TYPE_ID,
            Columns.DECOMP_STEP_ID: decomp_step_id
        }
    )
    if q.empty:
        return None
    elif len(q) > 1:
        raise MoreThanOneBestVersion(
            f"There is more than one best {GBD.Process.Name.CODCORRECT} "
            f"version in the database. Please investigate."
        )
    else:
        return q.output_version_id.iat[0]


def unmark_current_best(decomp_step_id: int) -> None:
    old_version = get_current_best(
        decomp_step_id=decomp_step_id
    )
    if not pd.isnull(old_version):
        update_best(old_version, Best.PREVIOUS_BEST)


def mark_best(codcorrect_version_id: int) -> None:
    update_best(codcorrect_version_id, Best.BEST)


def get_codcorrect_version_metadata(version_id: Optional[int]) -> Tuple[Optional[int]]:
    """Get CodCorrect version metadata.

    Returns a tuple of gbd_process_version_id,
    gbd_round_id, and decomp_step corresponding with the given
    CodCorrect version id. If version_id is None, returns None
    for all three.

    Args:
        version_id: id of a CodCorrect version.

    Raises:
        ValueError: if not metadata can be found, for instance if the version doesn't exist
        RuntimeError: more than one row of metadata was found
    """
    if version_id is None:
        return None, None, None

    with ezfuncs.session_scope(ConnectionDefinitions.GBD) as session:
        metadata = query_tools.query_2_df(
            CoDCorrectVersion.GET_CODCORRECT_VERSION_METADATA,
            session=session,
            parameters={"version_id": version_id}
        )

    if metadata.empty:
        raise ValueError(f"No metadata found for CodCorrect version {version_id}")
    elif len(metadata) > 1:
        raise RuntimeError(
            f"More than one row of metadata returned for CodCorrect version {version_id}"
        )

    metadata["decomp_step"] = decomp_step.decomp_step_from_decomp_step_id(
        metadata["decomp_step_id"].iat[0]
    )
    return (
        metadata["gbd_process_version_id"].iat[0],
        metadata["gbd_round_id"].iat[0],
        metadata["decomp_step"].iat[0]
    )


def create_new_diagnostic_version_row(
    version_id: int,
    session: Optional[orm.Session] = None
) -> None:
    """Creates a new row in codcorrect.diagnostics_version for the CodCorrect run.

    For now, never mark it best as that column is meaningless
    in regards to the diagnostic viz.
    """
    username = getpass.getuser()

    with ezfuncs.session_scope(ConnectionDefinitions.CODCORRECT, session=session) as scoped_session:
        query_tools.exec_query(
            Diagnostics.INSERT_DIAGNOSTIC_ROW,
            parameters={
                Columns.CODCORRECT_VERSION_ID: version_id,
                Columns.IS_BEST: 0,
                "inserted_by": username,
                "last_updated_by": username
            },
            session=scoped_session
        )
