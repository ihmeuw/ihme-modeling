from sqlalchemy import orm

from orm_stgpr.db import models


def validate_stgpr_version_exists(
        stgpr_version_id: int,
        session: orm.Session
) -> None:
    """
    Validates that an ST-GPR version ID is present in the database and has
    only one entry.

    Args:
        stgpr_version_id: the ST-GPR version ID to check
        session: the database session

    Raises:
        ValueError: if ST-GPR version ID is not present in the database, or if
            more than one ST-GPR version ID is present
    """
    count = session\
        .query(models.StgprVersion)\
        .filter_by(stgpr_version_id=stgpr_version_id)\
        .count()
    if not count:
        raise ValueError(
            f'Could not find run ID {stgpr_version_id} in the database'
        )
    elif count > 1:
        raise ValueError(
            f'Found {count} entries in the database for run ID '
            f'{stgpr_version_id}'
        )
