import sqlalchemy as sql
import pandas as pd
import smtplib
import json


def run_query(sql_statement, server="DATABASE"):
    """
    Executes and returns the results of a SQL query

    :param sql_statement: str
        SQL query to execute
    :param server: str
        DB server to connect to ('DATABASE' or 'DATABASE')
    :param database: str
        database to access
    :return: dataframe
        pandas dataframe with the results of the SQL query
    """
    DB = 'DATABASE'.format(creds=read_creds(),
                           server=server)
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


def execute_statement(sql_statement, server="DATABASE"):
    """
    Executes an input SQL statement for the user-specified database and server

    :param sql_statement: str
        SQL query to execute
    :param server: str
        server to connect to ('DATABASE' or 'DATABASE')
    :param database: str
        database to read from or write to
    """
    engine = sql.create_engine('DATABASE'.
                               format(creds=read_creds(), server=server))
    conn = engine.connect()
    conn.execute(sql_statement)
    conn.close()


def read_creds():
    """
    Read the password credentials from a file on the cluster.
    """
    with open('FILEPATH', 'r') as infile:
        creds = "codem:" + json.load(infile)["password"]
    return creds


def send_email(recipients, subject, msg_body):
    """
    Sends an email to user-specified recipients with a given subject and
    message body

    :param recipients: list of strings
        users to send the results to
    :param subject: str
        the subject of the email
    :param msg_body: str
        the content of the email
    """
    SMTP_SERVER = 'EMAIL'
    SMTP_PORT = 587
    sender_name = "CODEm Hybridizer"
    sender = 'EMAIL'
    password = 'PASSWORD'

    headers = ["From: " + sender_name + "<" + sender + ">",
               "Subject: " + subject,
               "To: " + ', '.join(recipients),
               "MIME-Version: 1.0",
               "Content-Type: text/html"]
    headers = "\r\n".join(headers)

    msg_body = headers + "\r\n\r\n" + msg_body

    session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)

    session.ehlo()
    session.starttls()
    session.ehlo
    session.login(sender, password)

    session.sendmail(sender, recipients, msg_body)
    session.quit()
