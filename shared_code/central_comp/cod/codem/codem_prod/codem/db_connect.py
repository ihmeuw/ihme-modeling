import sqlalchemy as sql
import pandas as pd
import numpy as np
import json
from sqlalchemy.orm import sessionmaker


def read_creds():
    """
    Read the password credentials from a file on the cluster.
    """
    with open('FILEPATH', 'r') as infile:
        creds = "USERNAME:" + json.load(infile)["password"]
    return creds


def query(call, connection, creds=None):
    """
    Make a call to a database, handles all the overhead

    :param call: str
        querty that you wish to run
    :param db: str
        database that you wish to query from
    :param creds: str
        credentials to use
    :return: dataframe
        pandas data frame
    """
    if creds is None:
        creds = read_creds()
    DB = "DB_NAME"
    engine = sql.create_engine(DB)
    try:
        df = pd.read_sql_query(call, engine)
    except sql.exc.ResourceClosedError:
        df = pd.DataFrame()
    return df


def write_df_to_sql(df, db, table, connection, creds=None, return_key=False):
    """
    Writes a pandas df to a table in the database and returns a list of
    the primary keys, usually the model version id

    :param df: pandas DataFrame
        the dataframe to write to the database
    :param db: str
        which database to write to
    :param table: str
        which table in the db to write to
    :param connection: str
        which server to connect to
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
    DB = "DB_PATH"
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
