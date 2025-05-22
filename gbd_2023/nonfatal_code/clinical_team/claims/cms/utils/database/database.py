import getpass
from collections import namedtuple

import mysql.connector as sql
import pandas as pd
from db_connector.odbc import ODBC


class CmsDatabase:
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
