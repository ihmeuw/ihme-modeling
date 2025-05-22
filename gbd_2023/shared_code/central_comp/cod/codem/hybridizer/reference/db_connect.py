import logging
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import orm

import db_tools.loaders as loaders
import db_tools_core
from db_tools import query_tools

# create logger
logger = logging.getLogger(__name__)


def execute_select(
    query: str,
    conn_def: str,
    parameters: Optional[Dict[str, Any]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Executes a query and returns a dataframe, calls db_tools_core."""
    with db_tools_core.session_scope(conn_def=conn_def, session=session) as scoped_session:
        return db_tools_core.query_2_df(query, session=scoped_session, parameters=parameters)


def execute_call(
    call: str, conn_def: str, parameters: Optional[Dict[str, Any]] = None
) -> None:
    """Executes an update query with db_tools_core."""
    with db_tools_core.session_scope(conn_def=conn_def) as scoped_session:
        scoped_session.execute(call, params=parameters)


def clean_string(string: str) -> str:
    """Cleans a string to be inserted."""
    string = string.rsplit("\n")
    string = " ".join(string)
    return string


def write_data(df: pd.DataFrame, db: str, table: str, conn_def: str) -> None:
    """Writes data in bulk as a pd.DataFrame to a specified table/database."""
    with db_tools_core.session_scope(conn_def) as scoped_session:
        loaders.Inserts(table=table, schema=db, insert_df=df).insert(scoped_session)


def write_metadata(df: pd.DataFrame, db: str, table: str, conn_def: str) -> list:
    """
    Writes data by row to SQL and returns primary keys from the insert.
    :return list of primary keys
    """
    primary_keys = []
    for index, _row in df.iterrows():
        insert_string = loaders.Inserts.instring(
            insert_df=df.loc[[index]], table=table, schema=db
        )
        insert_string = clean_string(insert_string)

        with db_tools_core.session_scope(conn_def) as scoped_session:
            key = query_tools.exec_query(insert_string, session=scoped_session).lastrowid
        primary_keys.append(key)
    return primary_keys


def wipe_database_upload(model_version_id: int, conn_def: str) -> pd.DataFrame:
    """
    Delete summaries from cod.model in case this hybrid needs to be run again.
    :param model_version_id: int
    :param conn_def: str
    :return:
    """
    with db_tools_core.session_scope(conn_def) as scoped_session:
        count_rows_query = """
            SELECT COUNT(1) AS count
            FROM cod.model
            WHERE model_version_id = :model_version_id
        """
        count = scoped_session.execute(
            count_rows_query, params={"model_version_id": model_version_id}
        ).scalar()
        if count > 0:
            call = "CALL cod.delete_from_model_tables_by_request(:model_version_id);"
            return query_tools.exec_query(
                call,
                session=scoped_session,
                parameters={"model_version_id": model_version_id},
            )
