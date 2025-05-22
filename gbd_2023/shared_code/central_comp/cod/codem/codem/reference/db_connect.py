import logging
import re
from typing import Any, Dict, List

import pandas as pd
import pymysql
import sqlalchemy as sql

import db_tools_core

# create logger
logger = logging.getLogger(__name__)


def execute_select(
    query: str,
    conn_def: str,
    parameters: Dict[str, Any] = None,
    session: sql.orm.Session = None,
) -> pd.DataFrame:
    """Executes a query and returns a dataframe, calls db_tools_core."""
    with db_tools_core.session_scope(conn_def=conn_def, session=session) as scoped_session:
        return db_tools_core.query_2_df(query, session=scoped_session, parameters=parameters)


def call_stored_procedure(name: str, args: List[Any], conn_def: str) -> None:
    """
    Call a stored procedure
    :param name: (str) name of stored procedure
    :param args: (list) arguments for stored procedure
    :param conn_def: (str) connection definition to database
    """
    logger.info(f"Running stored procedure {name} with {args}.")
    eng = db_tools_core.get_engine(conn_def=conn_def)
    connection = eng.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.callproc(name, args)
        cursor.close()
        connection.commit()
    except (pymysql.err.InternalError, pymysql.err.OperationalError) as e:
        logger.info(f"Hit an error with procedure: {name}.")
        raise e
    finally:
        connection.close()


# @retry(tries=5, wait=60)
def write_df_to_sql(
    df: pd.DataFrame, table: str, conn_def: str, return_key: bool = False
) -> None:
    """
    Writes a pandas df to a table in the database and returns a list of
    the primary keys, usually the model version id

    :param df: pandas DataFrame
        the dataframe to write to the database
    :param table: str
        which table in the db to write to (example: model_version)
    :param conn_def: str
        which server to connect to (example: cod-db-t01)
    :param return_key: boolean
        whether or not to return the inserted primary keys

    :return: list or None
        list of integers (inserted primary keys) if return_key is True,
        None otherwise
    """
    engine = db_tools_core.get_engine(conn_def=conn_def)

    if return_key:  # need to use sqlalchemy core to get keys
        metadata = sql.MetaData()
        db_table = sql.Table(table, metadata, autoload=True, autoload_with=engine)
        ins = db_table.insert()
        con = engine.connect()
        df = df.where((pd.notnull(df)), None)
        models = df.to_dict("records")
        primary_keys = []
        for model in models:  # only one primary key can be retrieved at once
            key = con.execute(ins, model).inserted_primary_key
            primary_keys.append(key)
        con.close()
        return [item for sublist in primary_keys for item in sublist]
    else:  # otherwise we can just use the pandas to_sql function
        df.to_sql(table, engine, if_exists="append", index=False, chunksize=15000)
        return


def countPartition(db: str, table: str, version_id: int, conn_def: str) -> int:
    """
    Counts the partitions to see if the partition list contains the versionID specified.

    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'
    :versionID (int): versionID that partition is based off of, for example for cod.submodel
                      it is based off of model_version_id
    :conn_def (str): server to connect to, e.g. 'codem'

    :return (int): 0 or 1
    """
    call = """
            SELECT COUNT(*)
            FROM information_schema.partitions
            WHERE table_schema = :db
                AND table_name = :table
                AND partition_method = 'RANGE'
                AND CAST(partition_description as unsigned int) > :version_id
            """
    count = execute_select(
        query=call,
        conn_def=conn_def,
        parameters={"db": db, "table": table, "version_id": version_id},
    ).iloc[0][0]
    return 1 if count > 0 else 0


def increase_partitions(conn_def: str, db: str, table: str) -> None:
    """
    Increases the partitions in a table. This problem has been encountered with the
    cod.submodel table in GBD 2016 and GBD 2017. This has only been used for the
    cod.submodel table.

    :conn_def (str): server to connect to, e.g. 'codem'
    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'

    :return: None
    """
    get_partition = """
    SELECT MAX(CAST(PARTITION_DESCRIPTION AS UNSIGNED)) AS max
    FROM information_schema.partitions
    WHERE TABLE_SCHEMA = :db
    AND TABLE_NAME = :table
    """

    add_partition = """
    ALTER TABLE {db}.{t} ADD PARTITION (PARTITION {string} VALUES LESS THAN ({less_than}));
    """
    max = execute_select(
        get_partition, conn_def=conn_def, parameters={"db": db, "table": table}
    )["max"][0]

    start = max
    end = max + 1000 - 1

    string = "p{}_{}".format(start, end)
    less_than = end + 1

    # Added try/except for GBD 2023 Codem Central Run 2 - CCPURPLE-1741
    # Our instinct is that it's from a race condition
    try:
        with db_tools_core.session_scope(conn_def=conn_def) as session:
            session.execute(
                add_partition.format(db=db, t=table, string=string, less_than=less_than)
            )
    except pymysql.err.OperationalError as e:
        code, msg = e.args
        if re.search("Duplicate partition", str(msg)):
            logger.info(f"Got {e} with {add_partition} for string: {string}.")
            pass
        else:
            raise e
