import time
import logging

import db_tools.ezfuncs as ezfuncs
import db_tools.query_tools as query_tools
import db_tools.loaders as loaders

# create logger
logger = logging.getLogger(__name__)


def retry(tries, wait):
    def real_decorator(function):
        def wrapper(*args, **kwargs):
            iterate = 0
            while iterate < tries:
                try:
                    return function(*args, **kwargs)
                except Exception:
                    logger.info("You failed to connect to the "
                                "database on iteration {}".format(iterate))
                    iterate += 1
                    time.sleep(wait)
            return function(*args, **kwargs)

        return wrapper

    return real_decorator


def query(call, conn_def):
    """
    Performs a SQL query with db_tools
    """
    df = ezfuncs.query(call, conn_def=conn_def)
    return df


def clean_string(string):
    """
    Cleans a string to be inserted.
    """
    string = string.rsplit('\n')
    string = " ".join(string)
    return string


def exec_query(call, conn_def):
    """
    Executes a raw SQL statement with db_tools
    """
    session = ezfuncs.get_session(conn_def)
    call = clean_string(call)
    return query_tools.exec_query(call, session=session, close=True)


def write_data(df, db, table, conn_def):
    """
    Writes data in bulk as a pd.DataFrame to a specified table/database.
    """
    session = ezfuncs.get_session(conn_def)
    loaders.Inserts(table=table, schema=db, insert_df=df).insert(session, commit=True)


def write_metadata(df, db, table, conn_def):
    """
    Writes data by row to SQL and returns primary keys from the insert.
    :return list of primary keys
    """
    primary_keys = []
    for index, row in df.iterrows():
        insert_string = loaders.Inserts.instring(insert_df=df.loc[[index]], table=table, schema=db)
        insert_string = clean_string(insert_string)
        key = exec_query(insert_string, conn_def=conn_def).lastrowid
        primary_keys.append(key)
    return primary_keys


def count_partition(db, table, version_id, conn_def):
    """
    Counts the partitions to see if the partition list contains the versionID specified.

    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'
    :version_id (int): versionID that partition is based off of, for example for cod.submodel
                      it is based off of model_version_id
    :conn_def (str): connection definition to use, codem or codem-test

    :return (int): 0 or 1
    """
    call = '''
           SELECT count(*)
           FROM information_schema.partitions
           WHERE table_schema = \'{db}\'
           AND table_name = \'{t}\'
           AND partition_method = \'RANGE\'
           AND cast(partition_description as unsigned int) > {vid}
           AND cast(substr(partition_name, instr(partition_name, \'p\')+1, instr(partition_name, \'_\')-2) as unsigned int) <= {vid}
           AND cast(substr(partition_name, instr(partition_name, \'_\')+1, length(partition_name))       as unsigned int) >= {vid}
           '''.format(db=db, t=table, vid=version_id)

    count = query(call, conn_def).iloc[0][0]
    return count


def increase_partitions(conn_def, db, table):
    """
    Increases the partitions in the cod.submodel table.

    :conn_def (str): connection definition, either ADDRESS or ADDRESS
    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'

    :return: None
    """
    get_partition = '''
                    SELECT MAX(CAST(PARTITION_DESCRIPTION AS UNSIGNED)) AS max
                    FROM information_schema.partitions
                    WHERE TABLE_SCHEMA = '{db}'
                    AND TABLE_NAME = '{t}'
                    '''

    add_partition = '''
                    ALTER TABLE {db}.{t} ADD PARTITION (PARTITION {string} VALUES LESS THAN ({less_than}));
                    '''

    maximum = query(get_partition.format(db=db, t=table), conn_def)['max'][0]

    start = maximum
    end = maximum + 1000 - 1

    string = "p{}_{}".format(start, end)
    less_than = end + 1

    exec_query(add_partition.format(db=db, t=table, string=string, less_than=less_than), conn_def)


def wipe_database_upload(model_version_id, conn_def):
    """
    Delete summaries from ADDRESS in case this hybrid needs to be run again.
    :param model_version_id: int
    :param conn_def: str
    :return:
    """
    call = f"CALL cod.delete_from_model_tables_by_request({model_version_id});"
    exec_query(call, conn_def=conn_def)
