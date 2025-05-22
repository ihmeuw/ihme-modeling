"""
AUTHOR: USERNAME
DATE: April 2019

This file contains a wrapper around mysqlalchemy to make it more pythonic
and easier to use. You must use an .odbc.ini file to store your database credentials.
This also contains a separate database connection class for the CMS database. For this class
you must have a person .odbc.ini as well as read privs for the heavily restricted CMS data.

Examples:
1) Using the Database object to make queries:

db = Database()
db.load_odbc('some_odbc_profile')
db.query("QUERY")

2) Using the DBManager:

DBopen = DBManager('some_odbc_profile')
with DBopen("QUERY") as table:
    print(table)

"""

import getpass
import sys
import traceback
from collections import namedtuple

import mysql.connector as sql
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

    def load_odbc(self, odbc_profile, path="FILEPATH"):
        """loads credentials from an odbc profile from a provided file into the database"""
        odbc = ODBC()
        odbc.load(path)
        server = odbc[odbc_profile]

        user = server["USER"]
        password = server["PASSWORD"]
        host = server["SERVER"]
        port = server["PORT"]

        self.credentials = Credentials(user, password, host, port)

    @property
    def engine(self):
        """Returns a mysqlalchemy.engine"""
        if not self.credentials:
            raise DatabaseException("Credentials have not been loaded")

        return sqlalchemy.create_engine(
            "mysql://URL"
        )

    @property
    def session(self):
        """returns a mysqlalchemy.session"""
        SessionMaker = orm.sessionmaker(bind=self.engine)
        return SessionMaker()

    def query(self, query_str, verbose=False):
        """Runs a provided query on the database that was loaded"""
        try:
            return pd.read_sql(sql=query_str, con=self.engine)

        except sqlalchemy.exc.ProgrammingError as err:
            code, message = err.orig.args
            if not verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(
                    exc_type, exc_value, exc_traceback, limit=0, file=sys.stdout
                )
            raise DatabaseException(f"SQL Error {code}: {message}")

    def write(self, path, schema, table, with_replace=True, commit=True):
        """Inlines a dataframe into the database that was loaded"""
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


class CmsDatabase:
    """Class to use to connect to the CMS database. There are some slight but important
    differences between this and the class above. Going forward we should either synthesize
    these two classes or perhaps remove this class entirely if we decide to remove all
    dependencies on the CMS database."""

    def __init__(self):
        self.credentials = None

    def load_odbc(self, odbc_profile="CONN_DEF", path="FILEPATH"):
        """loads credentials from an odbc profile from a provided file into the database"""

        odbc = ODBC()
        odbc.load(path)
        server = odbc[odbc_profile]

        user = server["USER"]
        password = server["PASSWORD"]
        host = server["SERVER"]
        database = server["DATABASE"]

        Credentials = namedtuple("Credentials", ["username", "password", "host", "database"])
        self.credentials = Credentials(user, password, host, database)

    def engine(self):
        if not self.credentials:
            raise Exception("Load Credentials")

        config = {
            "host": self.credentials.host,
            "database": self.credentials.database,
            "user": self.credentials.username,
            "password": self.credentials.password,
            "use_pure": True,
        }

        return sql.connect(**config)

    def read_table(self, dql):

        try:
            eng = self.engine()
            df = pd.read_sql(sql=dql, con=eng)
            eng.close()
            return df

        except sql.Error as e:
            print(e)
