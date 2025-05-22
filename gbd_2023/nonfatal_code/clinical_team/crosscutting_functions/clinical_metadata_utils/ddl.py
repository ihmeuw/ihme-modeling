"""
Author: USERNAME
Date: March 29, 2021
Contents: DDL class
"""

import re
import warnings
from typing import Any, List, Tuple, Type, Union

import mysql.connector
import numpy as np
import pandas as pd
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection, MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

from crosscutting_functions.clinical_metadata_utils.odbc import ODBC


class DDL:
    """Supports different types of CRUD operations to meet the Clinical team's requirements.

    Attributes:
        odbc_profile: Any valid connection definition in your odbc file,
                      which is appropriate for the tasks, eg: if
                      a task needs write perms.
        odbc_path: Path to an odbc.ini file. Defaults to the clinical team's shared odbc.
        host: Address to connect to. Automatically pulled from the odbc file/profile.
        database: Database within the provided host to connect to.
                  Automatically pulled from the odbc file/profile.
        user: Username to use when connecting to the db. Automatically pulled from
              the odbc file/profile.
        password: Password to use when connecting to the db. Automatically
                  pulled from the odbc file/profile.
        connection: Connection to a mysql database for the above properties.
    """

    def __init__(
        self, odbc_profile: str, odbc_path: str = "FILEPATH.odbc.ini"
    ) -> None:
        self.odbc_profile = odbc_profile
        self.odbc_path = odbc_path
        self.host = None
        self.database = None
        self.user = None
        self.password = None
        self._pull_odbc()

        self.connection = self._create_connection()

    def _pull_odbc(self) -> None:
        """Function to load ODBC and set attributes."""
        odbc = ODBC()
        odbc.load(self.odbc_path)
        odbc = odbc[self.odbc_profile]
        self.host = odbc["SERVER"]
        self.database = odbc["DATABASE"]
        self.user = odbc["USER"]
        self.password = odbc["PASSWORD"]

    def _type_check_error(self, obj_to_check: Any, type_to_verify: Type) -> None:
        """Validate that the obj_to_check is the expected type_to_verify.

        Raises:
            TypeError if the object to check does not match it's expected type.
        """
        if not isinstance(obj_to_check, type_to_verify):
            raise TypeError("Supplied obj_to_check is not the type you expected.")

    def _table_exists_check_error(self, table: str) -> None:
        """Validate that a given table exists.

        Raises:
            ValueError if the input table does not exist in the database.
        """
        curr_tables = self._pull_present_tables()
        if table not in curr_tables:
            raise ValueError(
                f"Supplied table {table} does not exist in {self.database} database."
            )

    def _cols_exist_check_error(self, table: str, cols_to_check: List[str]) -> None:
        """Validate that the columns provided exist in the table.

        Raises:
            ValueError if the provided columns to not exist in the table.
        """
        valid_cols = pd.read_sql(
            QUERY
        ).columns
        if any([col not in valid_cols for col in cols_to_check]):
            raise ValueError(
                f"One or more supplied columns in does not exist in {self.database}.{table}."
            )

    def _create_connection(self) -> Union[PooledMySQLConnection, MySQLConnection, Any]:
        """Return a connection to the database.

        Returns:
            A mysql.connector connection to the database.
        """
        return mysql.connector.connect(
            user=self.user, password=self.password, host=self.host, database=self.database
        )

    def _generate_cursor(self) -> MySQLCursor:
        """Return a cursor on the database connection."""
        return self.connection.cursor()  # type: ignore

    def _pull_present_tables(self) -> List[str]:
        """Return a list of present tables in self.database.

        Returns:
            List of all tables present in the database.
        """
        tables = pd.read_sql("SHOW TABLES;", self.connection)

        tables_list = tables[f"Tables_in_{self.database}"].tolist()
        return tables_list

    def _drop_if_exists(self, cursor: MySQLCursor, table: str) -> None:
        """Drop a table if it already exists in the database."""
        cursor.execute(f"DROP TABLE IF EXISTS {self.database}.{table}")

    def _copy_table(self, cursor: MySQLCursor, table: str, new_table: str) -> None:
        """Make an exact copy of an existing table."""
        # Define the SQL command to copy the table structure
        init_sql = f"CREATE TABLE {self.database}.{new_table} LIKE {self.database}.{table};"

        # Execute the command
        cursor.execute(init_sql)

    def _copy_data(self, cursor: MySQLCursor, table: str, new_table: str) -> None:
        """Copy all data from a given table into a new table."""
        # Define the SQL command to copy data from the existing table
        copy_sql = (
            f"INSERT INTO {self.database}.{new_table} "
            QUERY"
        )

        # Try executing & commiting, and rollback if necessary
        self._commit_to_db(cursor, copy_sql)

    def _commit_to_db(self, cursor: MySQLCursor, command_str: str) -> None:
        """Try inserting & commiting changes, rollback on failure.

        Raises:
            Error on failures to commit.
        """
        # Try executing the command
        try:
            cursor.execute(command_str)
            self.connection.commit()

        # Rolling back in case of error
        except Error:
            self.connection.rollback()
            raise Error

    def _alter_table(self, cursor: MySQLCursor, table: str, sql_command: str) -> None:
        """Modify a given table using a given SQL command."""
        # Define alter table command
        alter_sql = f"ALTER TABLE {self.database}.{table} {sql_command};"

        # Execute the command
        cursor.execute(alter_sql)

    def _update_table(
        self, cursor: MySQLCursor, table: str, set_clause: str, where_clause: str
    ) -> None:
        """Pass an Update command to be inserted into the database.

        Arguments:
            cursor: A mysql.connector cursor.
            table: Table to update data.
            set_clause: Values to be updated.
            where_clause: A filter to identify subset of rows to update.
        """
        # Define update SQL command
        update_sql = (
            f"UPDATE {self.database}.{table} " f"SET {set_clause} " f"WHERE {where_clause};"
        )

        # Send it to be inserted
        self._commit_to_db(cursor, update_sql)

    def _create_table(self, cursor: MySQLCursor, table: str, sql_command: str) -> None:
        """Create a new table.

        Arguments:
            cursor: A mysql.connector cursor.
            table: Table name to create.
            sql_command: Columns to add to the table.
        """
        # Define create table SQL
        create_tbl_sql = f"CREATE TABLE {self.database}.{table} ({sql_command});"

        # Execute it
        cursor.execute(create_tbl_sql)

    def _rename_col(
        self, cursor: MySQLCursor, table: str, curr_name: str, new_name: str, data_type: str
    ) -> None:
        """Internal method to rename a column in a table.

        Arguments:
            cursor: A mysql.connector cursor.
            table: Table where column to alter lives.
            curr_name: The original name of the column.
            new_name: The updated name for the column.
            data_type: The data type to assign to the column.
        """
        # Define rename SQL
        rename_sql = (
            f"ALTER TABLE {self.database}.{table} CHANGE COLUMN "
            f"{curr_name} {new_name} {data_type};"
        )

        # Execute
        cursor.execute(rename_sql)

    def _rename_table(self, cursor: MySQLCursor, curr_name: str, new_name: str) -> None:
        """Internal function to rename table.

        Arguments:
            cursor: A mysql.connector cursor.
            curr_name: The original name of the table.
            new_name: The updated name for the table.
        """
        # Define rename SQL
        rename_sql = f"ALTER TABLE {self.database}.{curr_name} RENAME TO {new_name};"

        # Execute
        cursor.execute(rename_sql)

    def make_duplicate_table(self, table: str, new_table: str) -> None:
        """Make exact copy of existing table and call it {self.database}.{new_table}.

        Arguments:
            table: Original table to be copied.
            new_table: The new table to create.
        """
        print(f"Making temporary table in {self.database} named {new_table}.")

        # Generate cursor
        cur = self._generate_cursor()

        # Drop temp_{table} if it already exists
        print(f"Dropping {new_table} if it already exists.")
        self._drop_if_exists(cur, new_table)

        # Copy table
        print(f"Making exact copy of {self.database}.{table} attributes.")
        self._copy_table(cur, table, new_table)

        # Fill values
        print(f"Copying data from {self.database}.{table}.")
        self._copy_data(cur, table, new_table)

        print(f"Duplicate table {self.database}.{new_table} created successfully.")

    def add_columns(self, table: str, new_column_sql: str, dtype: str) -> None:
        """Tack on a series of columns defined in new_column_sql to an existing table.

        Arguments:
            table: The table to add columns to.
            new_column_sql: The columns to add.
            dtype: Data types for the columns.
        """
        print(f"Adding columns to {self.database}.{table}.")

        # Generate cursor
        cur = self._generate_cursor()

        # Ensure columns don't already exist
        print("Checking if provided columns already exist.")
        existing_cols = pd.read_sql(
            QUERY, self.connection
        ).columns.tolist()
        provided_cols = new_column_sql.split(" ")
        matches = np.in1d(existing_cols, provided_cols)
        if any(m for m in matches):
            raise ValueError(
                f"One or more columns specified already exist in {self.database}.{table}."
            )

        # Alter the table
        print(f"Updating {self.database}.{table}.")
        sql_command = f"ADD COLUMN {new_column_sql} {dtype}"
        self._alter_table(cur, table, sql_command)

        print(f"Columns in {self.database}.{table} updated successfully.")

    def backfill_observations(
        self, table: str, new_vals_sql: str, where_clause_sql: str
    ) -> None:
        """Update existing rows of a table with new values.

        Arguments:
            table: The table with rows to update.
            new_vals_sql: The new values to update to.
            where_clause_sql: Any filters to apply to the update.
        """
        print(f"Backfilling columns in {self.database}.{table}.")
        print("Note: any present observations will be overwritten.")

        # Generate cursor
        cur = self._generate_cursor()

        # Send to update table
        print("Updating observations.")
        self._update_table(cur, table, new_vals_sql, where_clause_sql)

        print(f"Values in {self.database}.{table} updated successfully.")

    def table_switcheroo(self, existing_table: str, new_table: str) -> None:
        """Delete the specified existing table and replace it with the new_table.

        Arguments:
            existing_table: The table to be copied and deleted.
            new_table: The new table to create.
        """
        print(
            f"Setting up {self.database}.{new_table} as the replacement "
            f"for {self.database}.{existing_table}."
        )

        # Generate cursor
        cur = self._generate_cursor()

        # Ensure there's the same number of rows between the existing
        # and new table
        print("Performing sanity checks between existing and new table.")
        exist = pd.read_sql(
            QUERY, self.connection
        )
        new = pd.read_sql(QUERY, self.connection)
        if len(exist) != len(new):
            raise ValueError(
                "There are an incongruent number of rows between "
                f"{self.database}.{existing_table} and {self.database}.{new_table}."
            )

        # Delete existing_table
        print(f"Dropping existing {existing_table}.")
        self._drop_if_exists(cur, existing_table)

        # Make copy of new_table using existing_table name
        print(f"Making copy of {new_table} as {existing_table}.")
        self._copy_table(cur, new_table, existing_table)

        # Copy all the new_table into (new) existing_table
        print("Filling data appropriately.")
        self._copy_data(cur, new_table, existing_table)

        # Delete the new_table now that existing_table has been recreated
        print(f"Dropping {new_table} now that {existing_table} exists.")
        self._drop_if_exists(cur, new_table)

        print("Table switcheroo completed successfully.")

    def create_table(self, table: str, column_sql: str) -> None:
        """Create new table inside database.

        Arguments:
            table: The table name to create.
            column_sql: The in the new table columns to create.

        """
        print(f"Getting started adding {table} table to {self.database} database.")

        # Generate cursor
        cur = self._generate_cursor()

        # Ensure table doesn't already exist
        print(f"Checking that {table} table doesn't already exist.")
        curr_tables = self._pull_present_tables()
        if table in curr_tables:
            raise ValueError(f"{table} table already exists in {self.database} database.")
        del curr_tables

        # Send to _create_table
        print("Creating table.")
        self._create_table(cur, table, column_sql)

        print(f"{table} table created successfully.")

    def insert_row(
        self, table: str, insert_columns: List[str], insert_values: List[Any]
    ) -> None:
        """Insert new row into existing table.

        Arguments:
            table: The table to insert data into.
            insert_columns: The columns to insert into.
            insert_values: The values to insert.
        """
        print(f"Getting started inserting into {self.database}.{table}.")

        # Generate cursor
        cur = self._generate_cursor()

        print("Performing data validations before insert.")

        # Ensure insert_columns is a list
        if not isinstance(insert_columns, list):
            warnings.warn(
                "Parameter insert_columns was not supplied as a list but was cast as such."
            )
            insert_columns = [insert_columns]

        # Ensure insert_values is a list
        if not isinstance(insert_values, list):
            warnings.warn(
                "Parameter insert_values was not supplied as a " "list but was cast as such."
            )
            insert_values = [insert_values]

        # Ensure the table exists
        self._table_exists_check_error(table)

        # Ensure the columns exist to insert into
        self._cols_exist_check_error(table, insert_columns)

        # Format the command
        insert_columns = ", ".join(insert_columns)  # type: ignore
        insert_values = str(insert_values)[1:-1]  # type: ignore
        insert_sql = (
            f"INSERT INTO {self.database}.{table} "
            f"({insert_columns}) "
            f"VALUES ({insert_values});"
        )

        # Send to the _insert function
        print("Inserting into table.")
        self._commit_to_db(cur, insert_sql)

        print(f"New row successfully inserted into {self.database}.{table}.")

    def insert_rows(self, table: str, rows_to_insert: Union[dict, pd.DataFrame]) -> None:
        """Function to insert multiple rows into specified database.

        Arguments:
            table: The table to insert new rows into.
            rows_to_insert: The rows to insert into the table.
        """

        # Ensure that the table exists
        self._table_exists_check_error(table)

        # Ensure rows_to_insert is a dictionary or Pandas DF
        if not (
            (isinstance(rows_to_insert, dict)) | (isinstance(rows_to_insert, pd.DataFrame))
        ):
            raise TypeError("Supplied rows_to_insert is not a dictionary or Pandas DataFrame.")

        # Cast rows_to_insert as a Pandas DF if not already
        if not isinstance(rows_to_insert, pd.DataFrame):
            rows_to_insert = pd.DataFrame.from_dict(rows_to_insert)

        # Use DDL to insert rows
        for idx, row in rows_to_insert.iterrows():
            insert_cols = row.index.tolist()
            insert_vals = row.values.tolist()
            self.insert_row(table, insert_cols, insert_vals)

    def update_row(
        self,
        table: str,
        update_cols_and_vals: List[Tuple[Any]],
        update_merging_keys: List[Tuple[Any]],
    ) -> None:
        """Update a row in the specified table.

        Arguments:
            table: The table to be updated.
            update_cols_and_vals: The columns and their values to be updated to.
            update_merging_keys: The column and where clause to identify row to update.
        """

        print(f"Getting started updating {self.database}.{table}")

        # Generate cursor
        cur = self._generate_cursor()

        print("Performing data validations before update.")

        # Ensure update_cols_and_vals is a list of tuples
        if not isinstance(update_cols_and_vals, list):
            warnings.warn(
                "Parameter update_cols_and_vals was not supplied as a "
                "list but was cast as such."
            )
            update_cols_and_vals = [update_cols_and_vals]
        self._type_check_error(update_cols_and_vals[0], tuple)

        # Ensure update_merging_keys is a list of tuples
        if not isinstance(update_merging_keys, list):
            warnings.warn(
                "Parameter update_merging_keys was not supplied as a "
                "list but was cast as such."
            )
            update_merging_keys = [update_merging_keys]
        self._type_check_error(update_merging_keys[0], tuple)

        # Ensure the table exists
        self._table_exists_check_error(table)

        # Ensure all update cols exist
        self._cols_exist_check_error(table, [col[0] for col in update_cols_and_vals])

        # Ensure all update keys exist
        self._cols_exist_check_error(table, [col[0] for col in update_merging_keys])

        # Ensure string types are kept
        internal_cv: List[Any] = []
        for i in range(len(update_cols_and_vals)):
            cv = list(update_cols_and_vals[i])
            if isinstance(cv[1], str):
                internal_cv.append((cv[0], '"' + cv[1] + '"'))
            else:
                internal_cv.append(update_cols_and_vals[i])
        internal_k: List[Any] = []
        for i in range(len(update_merging_keys)):
            cv = list(update_merging_keys[i])
            if isinstance(cv[1], str):
                internal_k.append((cv[0], '"' + cv[1] + '"'))
            else:
                internal_k.append(update_merging_keys[i])

        # Setup set and where clauses
        set_clause = []
        for i in range(len(internal_cv)):
            set_clause.append(f"{internal_cv[i][0]} = {internal_cv[i][1]}")
        set_clause_str = ", ".join(set_clause)
        where_clause = []
        for i in range(len(internal_k)):
            where_clause.append(f"{internal_k[i][0]} = {internal_k[i][1]}")
        where_clause_str = " AND ".join(where_clause)

        print("Updating table.")
        self._update_table(cur, table, set_clause_str, where_clause_str)

        print(f"{self.database}.{table} table successfully updated.")

    def update_rows(
        self,
        table: str,
        rows_to_update: Union[dict, pd.DataFrame],
        update_key_columns: List[str],
    ) -> None:
        """Function to update multiple rows in the specified database.

        Arguments:
            table: The table to update rows within.
            rows_to_update: With a DataFrame, the database columns to update into
                            must be the column names in the DF. With a
                            dictionary, the database columns must be the keys of the dict.
            update_key_column: A list of columns/keys  present in the `rows_to_update`
                               which will isolate the rows to update in the database
                               when pasted onto a WHERE clause.
        """

        # Ensure that the table exists
        self._table_exists_check_error(table)

        # Ensure rows_to_update is a dictionary or Pandas DF
        if not (
            (isinstance(rows_to_update, dict)) | (isinstance(rows_to_update, pd.DataFrame))
        ):
            raise TypeError("Supplied rows_to_update is not a dictionary or Pandas DataFrame.")

        # Ensure update_key_columns is a list
        if not isinstance(update_key_columns, list):
            warnings.warn(
                "Supplied update_key_columns not of type list but will be cast as such."
            )
            update_key_columns = [update_key_columns]

        # Cast rows_to_update as Pandas DF if not already
        if not isinstance(rows_to_update, pd.DataFrame):
            rows_to_update = pd.DataFrame.from_dict(rows_to_update)

        # Ensure all key columns are in the rows_to_update
        if any(col not in rows_to_update.columns for col in update_key_columns):
            raise ValueError(
                "Supplied rows_to_update do not include observations "
                "for (all of) the specified update_key_columns."
            )

        # Use DDL to update rows
        for idx, row in rows_to_update.iterrows():
            update_cv = []
            update_k = []
            for item in row.iteritems():
                if item[0] in update_key_columns:
                    update_k.append(item)
                else:
                    update_cv.append(item)
            self.update_row(table, update_cv, update_k)

    def rename_column(
        self, table: str, existing_col_name: str, new_col_name: str, data_type: str
    ) -> None:
        """Rename a column in an existing table.

        Arguments:
            table: The table the column to rename exists in.
            existing_col_name: The original column name.
            new_col_name: The updated name for the original column.
            data_type: The data type for the updated column.
        """

        print("Getting started updating a column name.")

        # Generate cursor
        cur = self._generate_cursor()

        print("Performing data validations before renaming.")

        # Ensure table exists
        self._table_exists_check_error(table)

        # Ensure existing column name exists
        self._cols_exist_check_error(table, [existing_col_name])

        # Ensure new column name doesn't already exist
        curr_cols = pd.read_sql(
            QUERY, self.connection
        ).columns
        if new_col_name in curr_cols:
            raise ValueError(f"Supplied column name already exists in {self.database}.{table}")
        del curr_cols

        # Pass params into _rename()
        print(f"Updating column name from {existing_col_name} to {new_col_name}.")
        self._rename_col(cur, table, existing_col_name, new_col_name, data_type)

        print("Update performed successfully.")

    def rename_table(self, existing_table_name: str, new_table_name: str) -> None:
        """Rename an existing table.

        Arguments:
            existing_table_name: The original table name.
            new_table_name: The updated name for the original table.
        """

        print("Getting started renaming a table.")

        # Generate cursor
        cur = self._generate_cursor()

        print("Performing data validations before renaming.")

        # Ensure existing table exists
        self._table_exists_check_error(existing_table_name)

        # Ensure new table name doesn't already exist
        curr_tables = self._pull_present_tables()
        if new_table_name in curr_tables:
            raise ValueError(f"Supplied new_table_name already exists in {self.database}.")

        # Call _rename_table
        print("Performing rename.")
        self._rename_table(cur, existing_table_name, new_table_name)

        print(f"Table successfully renamed from {existing_table_name} to {new_table_name}.")

    def delete_from_where(self, table: str, where_expression: str) -> None:
        """Delete rows from a table where results match the where_expression.

        Arguments:
            table: The table to delete rows from.
            where_expression: A where clause to identify rows to delete.
        """
        print("Getting started with the deletion.")

        # Generate cursor
        cur = self._generate_cursor()

        # Validate that the table exists
        self._table_exists_check_error(table)

        # Remove the word where (ignoring case) if contained in the where_expression
        where_expression = re.sub("WHERE ", "", where_expression, flags=re.I)

        # Select the rows and print out a message
        impacted_rows = pd.read_sql(
            QUERY,
            self.connection,
        )
        print(
            f"Note: There are {len(impacted_rows)} row(s) which will be impacted by "
            "this deletion."
        )

        # Pass the statement for deletion
        delete_sql = f"DELETE FROM {self.database}.{table} " f"WHERE {where_expression};"
        self._commit_to_db(cur, delete_sql)

        print("Row(s) successfully deleted!")

    def __str__(self):
        return (
            "DDL object:\n"
            f"\todbc_profile: {self.odbc_profile}\n"
            f"\todbc_path: {self.odbc_path}\n"
            f"\thost: {self.host}\n"
            f"\tdatabase: {self.database}\n"
            f"\tuser: {self.user}\n"
            f"\tpassword: {'*' * len(self.password)}"
        )

    def __repr__(self):
        return (
            "DDL object:\n"
            f"\todbc_profile: {self.odbc_profile}\n"
            f"\todbc_path: {self.odbc_path}\n"
            f"\thost: {self.host}\n"
            f"\tdatabase: {self.database}\n"
            f"\tuser: {self.user}\n"
            f"\tpassword: {'*' * len(self.password)}"
        )

    def __del__(self) -> None:
        """Close connection to database upon class deletion"""
        self.connection.close()