from pandas import isnull

from db_tools.ezfuncs import get_session, query
from db_tools.query_tools import exec_query

from fauxcorrect.queries.queries import CoDCorrectVersion
from fauxcorrect.utils.constants import (
    Best, COD, Columns, ConnectionDefinitions, GBD, Status
)


def create_new_version_row(
        codcorrect_version_id: int,
        description: str,
        decomp_step_id: int,
        code_version: str,
        env_version: int
) -> None:
    exec_query(
        CoDCorrectVersion.INSERT_NEW_VERSION_ROW,
        session=get_session(ConnectionDefinitions.COD),
        close=True,
        parameters = {
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id,
            Columns.TOOL_TYPE_ID: COD.DataBase.TOOL_TYPE_ID,
            Columns.DECOMP_STEP_ID: decomp_step_id,
            Columns.DESCRIPTION: description,
            Columns.CODE_VERSION: code_version,
            Columns.ENV_VERSION: env_version,
            Columns.STATUS: Status.SUBMITTED,
            Columns.IS_BEST: Best.NOT_BEST
        }
    )


def create_output_partition(codcorrect_version_id) -> None:
    exec_query(
        CoDCorrectVersion.CREATE_OUTPUT_PARTITION,
        session=get_session(ConnectionDefinitions.COD),
        close=True,
        parameters = {
            'schema': COD.DataBase.SCHEMA,
            'table': COD.DataBase.TABLE,
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id
        }
    )


def get_new_version_id() -> int:
    return int(query(
        CoDCorrectVersion.GET_NEW_VERSION_ID,
        conn_def=ConnectionDefinitions.COD
    ).output_version_id.item())


def get_last_inserted_version_id() -> int:
    return query(
        CoDCorrectVersion.GET_LAST_INSERT_ID,
        conn_def=ConnectionDefinitions.COD
    ).output_version_id.item()


def update_status(codcorrect_version_id: int, status: int) -> None:
    query(
        CoDCorrectVersion.UPDATE_STATUS,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.STATUS: status,
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id
        }
    )


def update_best(codcorrect_version_id: int, is_best: int) -> None:
    query(
        CoDCorrectVersion.UPDATE_BEST,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.OUTPUT_VERSION_ID: codcorrect_version_id,
            Columns.IS_BEST: is_best
        }
    )


def get_current_best(decomp_step_id: int) -> int:
    q = query(
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
        return q.output_version_id.item()


def unmark_current_best(decomp_step_id: int) -> None:
    old_version = get_current_best(
        decomp_step_id=decomp_step_id
    )
    if not isnull(old_version):
        update_best(old_version, Best.PREVIOUS_BEST)


def mark_best(codcorrect_version_id: int) -> None:
    update_best(codcorrect_version_id, Best.BEST)
