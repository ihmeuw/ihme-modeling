import logging
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import orm

import db_tools_core

logger = logging.getLogger(__name__)

def select_to_df(
    query: str,
    conn_def: str,
    parameters: Optional[Dict[str, Any]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Executes a query and returns a dataframe, using db_tools_core."""
    with db_tools_core.session_scope(conn_def=conn_def, session=session) as scoped_session:
        return db_tools_core.query_2_df(query, session=scoped_session, parameters=parameters)
