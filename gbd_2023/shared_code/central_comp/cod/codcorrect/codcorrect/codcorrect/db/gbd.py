"""Utility functions for pulling info from the GBD database"""

from typing import List, Optional, Tuple, Union

from sqlalchemy import orm

import db_tools_core
import gbd_outputs_versions
from gbd.constants import gbd_metadata_type, gbd_process

from codcorrect.legacy.utils.constants import GBD, ConnectionDefinitions
from codcorrect.lib.db.queries import CoDCorrectVersion, GbdDatabase


def get_new_codcorrect_version_id(session: Optional[orm.Session] = None) -> int:
    """Gets a new CodCorrect version for the CodCorrect run.

    Takes the largest CodCorrect version from TABLE and adds 1.

    Args:
        session: Session with the GBD database.
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.GBD, session=session
    ) as scoped_session:
        return int(
            scoped_session.execute(CoDCorrectVersion.GET_NEW_CODCORRECT_VERSION_ID).scalar()
        )


def get_codcorrect_version_metadata(
    version_id: Optional[int],
) -> Tuple[Optional[int], Optional[int]]:
    """Gets CodCorrect version metadata.

    Returns a tuple of gbd_process_version_id, release_id corresponding
    with the given CodCorrect version id. If version_id is None, returns None
    for both.

    Args:
        version_id: ID of a CodCorrect version.
    """
    if version_id is None:
        return None, None

    gbd_process_version_id = gbd_outputs_versions.internal_to_process_version(
        version_id, gbd_process.COD
    )
    process_version = gbd_outputs_versions.GBDProcessVersion(gbd_process_version_id)

    return gbd_process_version_id, process_version.release_id


def get_gbd_process_version_id(
    version_id: int,
    session: orm.Session,
    raise_if_multiple: bool = False,
    return_all: bool = False,
) -> Union[List[int], int]:
    """
    Returns the (non-deleted) GBD process version id associated with the
    CoDCorrect version. Defaults to the most recent GBD process
    version if there is more than one.

    Args:
        version_id: the internal version of the process. Ie. 135
            for CoDCorrect v135
        session: SQLAlchemy session with the GBD db
        raise_if_multiple: whether to throw an error if there are more than
            one associated GBD process versions. Defaults to False
        return_all: whether to return a list of all associated GBD process
            version ids if multiple or just the most recent.

    Returns:
        The most recent, non-deleted GBD process version id associated with
        the CoDCorrect run. Returns all as a list if return-all=True

    Raises:
        ValueError if there are no non-deleted GBD process version ids
            associated;
        RuntimeError if there are multiple associated process version ids
            and raise_if_multiple=True
    """
    undeleted_version_ids = (
        session.execute(
            GbdDatabase.GET_GBD_PROCESS_VERSION_ID,
            params={
                "gbd_process_id": GBD.Process.Id.CODCORRECT,
                "metadata_type_id": gbd_metadata_type.CODCORRECT,
                "internal_version_id": version_id,
            },
        )
        .scalars()
        .all()
    )

    if not undeleted_version_ids:
        raise ValueError(
            f"There are no GBD process version ids for CodCorrect v"
            f"{version_id} that have not been deleted."
        )

    if raise_if_multiple and len(undeleted_version_ids) > 1:
        raise RuntimeError(
            "There is more than one GBD process version id associated with "
            f"CodCorrect v{version_id} and raise_if_multiple=True. GBD process "
            f" versions: {undeleted_version_ids}."
        )

    if return_all:
        return undeleted_version_ids
    else:
        return undeleted_version_ids[0]
