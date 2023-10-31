from db_tools.ezfuncs import get_session, query
from db_tools.query_tools import exec_query

from fauxcorrect.queries.queries import FauxCorrectVersion
from fauxcorrect.utils.constants import (
    Best, Columns, ConnectionDefinitions, Status
)


def create_new_version_row(
        fauxcorrect_version_id: int,
        description: str,
        code_version: str,
        scalar_version_id: int,
        gbd_round_id: int,
        decomp_step_id: int
) -> None:
    exec_query(
        query=FauxCorrectVersion.INSERT_NEW_VERSION_ROW.format(
            fauxcorrect_version_id=fauxcorrect_version_id,
            description=description,
            code_version=code_version,
            scalar_version_id=scalar_version_id,
            gbd_round_id=gbd_round_id,
            decomp_step_id=decomp_step_id,
            status=Status.SUBMITTED,
            is_best=Best.NOT_BEST
        ),
        session=get_session(ConnectionDefinitions.COD),
        close=True
    )


def get_new_version_id() -> int:
    return int(query(
        FauxCorrectVersion.GET_NEW_VERSION_ID,
        conn_def=ConnectionDefinitions.COD
    ).fauxcorrect_version_id.iat[0])


def get_last_inserted_version_id() -> int:
    return query(
        FauxCorrectVersion.GET_LAST_INSERT_ID,
        conn_def=ConnectionDefinitions.COD
    ).fauxcorrect_version_id.iat[0]


def update_status(fauxcorrect_version_id: int, status: int) -> None:
    query(
        FauxCorrectVersion.UPDATE_STATUS,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.STATUS: status,
            Columns.FAUXCORRECT_VERSION_ID: fauxcorrect_version_id
        }
    )


def update_best(fauxcorrect_version_id: int, is_best: int) -> None:
    query(
        FauxCorrectVersion.UPDATE_BEST,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.FAUXCORRECT_VERSION_ID: fauxcorrect_version_id,
            Columns.IS_BEST: is_best
        }
    )


def get_current_best(gbd_round_id: int, decomp_step_id: int) -> int:
    return query(
        FauxCorrectVersion.GET_CURRENT_BEST_VERSION,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            Columns.GBD_ROUND_ID: gbd_round_id,
            Columns.DECOMP_STEP_ID: decomp_step_id
        }
    ).fauxcorrect_version_id.iat[0]


def unmark_current_best(gbd_round_id: int, decomp_step_id: int) -> None:
    old_version = get_current_best(
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id
    )
    update_best(old_version, Best.PREVIOUS_BEST)


def mark_best(fauxcorrect_version_id: int) -> None:
    update_best(fauxcorrect_version_id, Best.BEST)


def get_scalar_version_id(gbd_round_id: int) -> int:
    return query(
        FauxCorrectVersion.GET_SCALAR_VERSION_ID,
        conn_def=ConnectionDefinitions.COD,
        parameters={Columns.GBD_ROUND_ID: gbd_round_id - 1}  # last round's id
    ).scalar_version_id.iat[0]
