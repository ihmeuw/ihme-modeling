"""
AUTHOR:
DATE: April 2019

This file contains a wrapper around mysqlalchemy to make it more pythonic
and easier to use. You can either use an db_credentials.json file or an 
.odbc.ini file to store your database credentials. 

Examples:
1) Using the Database object to make queries:

db = Database()
db.load_odbc('some_odbc_profile')
db.query("SELECT * FROM schema.table")

2) Using the DBManager:

DBopen = DBManager('some_odbc_profile')
with DBopen("SELECT * FROM schema.table") as table:
    print(table)

"""

from collections import namedtuple
import os
import traceback
import sys

import pandas as pd
import sqlalchemy
import sqlalchemy.orm as orm

from db_tools.loaders import Infiles

from .odbc import ODBC


Credentials = namedtuple("Credentials", ["username", "password", "host", "port"])


class DatabaseException(Exception):
    pass


class Database:
    def __init__(self):
        self.credentials = None

    def load_db_credentials(self):
        """
        Helper function of db_connection.
        Per the documentation on /database of the hospital repo, store your
        credentials to access the sandbox dB in a json file. This functions
        pulls your username, password and the sandbox host address
        """
        creds_path = os.path.expanduser("FILEPATH")
        try:
            creds = pd.read_json(creds_path).reset_index()
        except ValueError:
            raise DatabaseException(
                f"You must store your clinical data sandbox credentials "
                f"in {creds_path}. Please see the database directory of the "
                f"hospital repo for more info"
            )

        user = creds.loc[creds["index"] == "user", "credentials"].iloc[0]
        psswrd = creds.loc[creds["index"] == "password", "credentials"].iloc[0]
        host = creds.loc[creds["index"] == "name", "host"].iloc[0]
        port = creds.loc[creds["index"] == "port", "host"].iloc[0]

        self.credentials = Credentials(user, psswrd, host, port)

    def load_odbc(self, odbc_profile, path="FILEPATH"):
        """ loads credentials from an odbc profile from a provided file into the database"""
        odbc = ODBC()
        odbc.load(path)
        server = odbc[odbc_profile]

        user = server["user"]
        password = server["password"]
        host = server["server"]
        port = server["port"]

        self.credentials = Credentials(user, password, host, port)

    @property
    def engine(self):
        """Returns a mysqlalchemy.engine"""
        if not self.credentials:
            raise DatabaseException("Credentials have not been loaded")

        return sqlalchemy.create_engine(
            "mysql://{username}:{password}@{host}:{port}/{db}".format(
                username=self.credentials.username,
                password=self.credentials.password,
                host=self.credentials.host,
                port=self.credentials.port,
                db="FILEPATH",
            )
        )
        return engine

    @property
    def session(self):
        """returns a mysqlalchemy.session"""
        SessionMaker = orm.sessionmaker(bind=self.engine)
        return SessionMaker()

    def query(self, query_str, verbose=False):
        """ Runs a provided query on the database that was loaded"""
        try:
            return pd.read_sql(sql=query_str, con=self.engine)

        # Invalid SQL syntax - These tracebacks are ridiculously long.
        # This block of code aims to cut that down.
        except sqlalchemy.exc.ProgrammingError as err:
            code, message = err.orig.args
            if not verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(
                    exc_type, exc_value, exc_traceback, limit=0, file=sys.stdout
                )
        raise DatabaseException(f"SQL Error {code}: {message}")

    def write(self, path, schema, table, with_replace=True, commit=True):
        """ Inlines a dataframe into the database that was loaded"""
        # upload
        inf = Infiles(table=table, schema=schema, session=self.session)
        inf.infile(path=path, with_replace=with_replace, commit=commit)


class DBManager:
    def __init__(self, odbc_profile):
        self.odbc_profile = odbc_profile

    def __call__(self, query):
        self.query = query
        return self

    def __enter__(self):
        db = Database()
        db.load_odbc(self.odbc_profile)
        return db.query(self.query)

    def __exit__(self, type, value, traceback):
        pass

