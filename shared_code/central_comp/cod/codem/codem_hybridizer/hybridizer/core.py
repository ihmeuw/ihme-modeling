import sqlalchemy as sql
import pandas as pd
import json
import logging
import json

def run_query(sql_statement, server="SERVER"):
    """
    Executes and returns the results of a SQL query

    :param sql_statement: str
        SQL query to execute
    :param server: str
        DB server to connect to 
    :param database: str
        database to access
    :return: dataframe
        pandas dataframe with the results of the SQL query
    """
    DB = "DATABASE"
    engine = sql.create_engine(DB)
    return pd.read_sql_query(sql_statement, engine)

def read_creds():
    """
    Read the password credentials from a file on the cluster.
    """
    with open('FILEPATH', 'r') as infile:
        creds = "codem:" + json.load(infile)["password"]
    return creds

def insert_row(insert_object, engine):
    """
    Inserts a row or rows into a db in SQL

    :param insert_object: sqlalchemy Insert object
        an object containing the insert statement for the input rows
    :param engine: sqlalchemy engine
        engine with which to connect to the DB
    :return: int
        the primary key from the first row inserted, typically model_version_id
    """
    connection = engine.connect()
    result = connection.execute(insert_object)
    connection.close()
    return int(result.inserted_primary_key[0])

def execute_statement(sql_statement, server="SERVER"):
    """
    Executes an input SQL statement for the user-specified database and server

    :param sql_statement: str
        SQL query to execute
    :param server: str
        server to connect to
    :param database: str
        database to read from or write to
    """
    engine = sql.create_engine('SERVER')
    conn = engine.connect()
    conn.execute(sql_statement)
    conn.close()

