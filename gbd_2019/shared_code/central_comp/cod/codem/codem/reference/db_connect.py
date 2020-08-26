import sqlalchemy as sql
import pandas as pd
import time
import json
import logging

# create logger
logger = logging.getLogger(__name__)


def read_creds():
    """
    Read the password credentials from a file on the cluster.
    """
    with open('FILEPATH', 'r') as infile:
        creds = "USERNAME:" + json.load(infile)["password"]
    return creds


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


#@retry(tries=5, wait=60)
def query(call, connection, creds=None):
    """
    Make a call to a database, handles all the overhead

    :param call: str
        query that you wish to run
    :param connection: str
        database that you wish to query from
    :param creds: str
        credentials to use
    :return: dataframe
        pandas data frame
    """
    if creds is None:
        creds = read_creds()
    DB = 'mysql+pymysql://{creds}@{connection}.ihme.washington.edu:3306'.\
                                    format(creds=creds, connection=connection)
    engine = sql.create_engine(DB)
    try:
        df = pd.read_sql_query(call, engine)
    except sql.exc.ResourceClosedError:
        df = pd.DataFrame()
    return df


def call_stored_procedure(name, args, connection):
    """
    Call a stored procedure
    :param name: (str) name of stored procedure
    :param args: (list) arguments for stored procedure
    :param connection: (str) database
    :return:
    """
    logger.info(f'Running stored procedure {name} with {args}.')
    creds = read_creds()
    db = 'mysql+pymysql://{creds}@{connection}.ihme.washington.edu:3306'.format(creds=creds, connection=connection)
    engine = sql.create_engine(db)
    connect = engine.raw_connection()
    cursor = connect.cursor()
    cursor.callproc(name, args)
    cursor.close()
    connect.commit()


#@retry(tries=5, wait=60)
def write_df_to_sql(df, db, table, connection, creds=None, return_key=False):
    """
    Writes a pandas df to a table in the database and returns a list of
    the primary keys, usually the model version id

    :param df: pandas DataFrame
        the dataframe to write to the database
    :param db: str
        which database to write to (example: cod, shared)
    :param table: str
        which table in the db to write to (example: model_version)
    :param connection: str
        which server to connect to (example: ADDRESS)
    :param creds: str
        the credentials to use to connect to the db
    :param return_key: boolean
        whether or not to return the inserted primary keys

    :return: list or None
        list of integers (inserted primary keys) if return_key is True,
        None otherwise
    """
    if creds is None:
        creds = read_creds()
    DB = "mysql+pymysql://{creds}@{connection}.ihme.washington.edu:3306/{db}".\
                            format(creds=creds, connection=connection, db=db)
    engine = sql.create_engine(DB)

    if return_key:  #  need to use sqlalchemy core to get keys
        metadata = sql.MetaData()
        db_table = sql.Table(table, metadata, autoload=True,
                                        autoload_with=engine)
        ins = db_table.insert()
        con = engine.connect()
        df = df.where((pd.notnull(df)), None)
        models = df.to_dict('records')
        primary_keys = []
        for model in models:  # only one primary key can be retrieved at once
            key = con.execute(ins, model).inserted_primary_key
            primary_keys.append(key)
        con.close()
        return [item for sublist in primary_keys for item in sublist]
    else:  # otherwise we can just use the pandas to_sql function
        df.to_sql(table, engine, if_exists="append", index=False, chunksize=15000)
        return


def countPartition(db, table, versionID, db_connection):
    '''
    Counts the partitions to see if the partition list contains the versionID specified.

    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'
    :versionID (int): versionID that partition is based off of, for example for cod.submodel
                      it is based off of model_version_id
    :db_connection (str): server to connect to, e.g. 'ADDRESS'

    :return (int): 0 or 1
    '''
    call =  '''
            SELECT count(*)
            FROM information_schema.partitions
            WHERE table_schema = \'{db}\'
            AND table_name = \'{t}\'
            AND partition_method = \'RANGE\'
            AND cast(partition_description as unsigned int) > {vid}
            AND cast(substr(partition_name, instr(partition_name, \'p\')+1, instr(partition_name, \'_\')-2) as unsigned int) <= {vid}
            AND cast(substr(partition_name, instr(partition_name, \'_\')+1, length(partition_name))       as unsigned int) >= {vid}
            '''.format(db=db, t=table, vid=versionID)

    count = query(call, db_connection).iloc[0][0]
    return count


def increase_partitions(db_connection, db, table):
    """
    Increases the partitions in a table. This problem has been encountered with the cod.submodel table
    in GBD 2016 and GBD 2017. This has only been used for the cod.submodel table.

    :db_connection (str): server to connect to, e.g. 'ADDRESS'
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

    max = query(get_partition.format(db=db, t=table), db_connection)['max'][0]
    
    start = max
    end = max + 1000 - 1
    
    string = "p{}_{}".format(start, end)
    less_than = end + 1
    
    query(add_partition.format(db=db, t=table, string=string, less_than=less_than), db_connection)
