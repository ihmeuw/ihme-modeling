import logging

import db_tools.loaders as loaders
from db_tools import ezfuncs, query_tools

# create logger
logger = logging.getLogger(__name__)


def clean_string(string):
    """
    Cleans a string to be inserted.
    """
    string = string.rsplit('\n')
    string = " ".join(string)
    return string


def write_data(df, db, table, conn_def):
    """
    Writes data in bulk as a pd.DataFrame to a specified table/database.
    """
    with ezfuncs.session_scope(conn_def) as scoped_session:
        loaders.Inserts(table=table, schema=db, insert_df=df).insert(scoped_session)


def write_metadata(df, db, table, conn_def):
    """
    Writes data by row to SQL and returns primary keys from the insert.
    :return list of primary keys
    """
    primary_keys = []
    for index, row in df.iterrows():
        insert_string = loaders.Inserts.instring(insert_df=df.loc[[index]],
                                                 table=table,
                                                 schema=db)
        insert_string = clean_string(insert_string)

        with ezfuncs.session_scope(conn_def) as scoped_session:
            key = query_tools.exec_query(
                insert_string,
                session=scoped_session
            ).lastrowid
        primary_keys.append(key)
    return primary_keys


def wipe_database_upload(model_version_id, conn_def):
    """
    Delete summaries from cod.model_version in case this hybrid needs to be run again.
    :param model_version_id: int
    :param conn_def: str
    :return:
    """
    call = "CALL cod.delete_from_model_tables_by_request(:model_version_id);"
    call = clean_string(call)
    with ezfuncs.session_scope(conn_def) as scoped_session:
        return query_tools.exec_query(
            call, session=scoped_session, parameters={'model_version_id': model_version_id}
        )
