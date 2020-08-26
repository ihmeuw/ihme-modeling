import contextlib
from typing import Generator

from sqlalchemy import orm

from db_tools import ezfuncs


@contextlib.contextmanager
def session_scope(conn_def: str) -> Generator[orm.Session, None, None]:
    """Provides a transactional scope around a series of operations."""
    session = ezfuncs.get_session(conn_def)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
