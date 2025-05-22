"""
Author: USERNAME
Date: April 2, 2021
Contents: PipelineWrappers class and pipeline-specific subclasses
Edited: USERNAME
"""

import warnings
from functools import lru_cache
from typing import List, Optional, Union

import pandas as pd

# Imports
from crosscutting_functions.clinical_metadata_utils.ddl import DDL
from crosscutting_functions.clinical_metadata_utils.version_directory_manager import (
    DataDirManager,
)

# Setup column name + description character limits
# This comes from the column specifications in the clinical database
name_desc_limits = {
    "adhoc_methods": {"name": 49, "description": 249},
    "age_sex_weights_version": {"name": 49, "description": 249},
    "inj_cf_input_version": {"name": 49, "description": 249},
    "inj_cf_version": {"name": 49, "description": 249},
    "cf_version_set": {"name": 49, "description": 499},
    "clinical_data_universe": {"description": 499},
    "clinical_version": {"name": 49, "description": 249},
    "clinical_methods": {"name": 49, "description": 249},
    "status": {"name": 49},
}


class PipelineWrappers:
    """Parent class to read and write run-based metadata to the clinical database.

    Attributes:
        self.ddl: Instance of the DDL class to use when read/writing data. This is
        what is being "wrapped" to create a friendlier interface for use in clinical
        processing pipelines.
    """

    def __init__(
        self, odbc_profile: str, odbc_path: str = "FILEPATH/.odbc.ini"
    ) -> None:
        self.ddl = DDL(odbc_profile, odbc_path)

    def _table_exists_check_error(self, table: str) -> None:
        """Validate that a given table exists.

        Args:
            table: The table of interest to check.

        Raises:
            ValueError if a table is not present in the database.
        """
        curr_tables = self.ddl._pull_present_tables()
        if table not in curr_tables:
            raise ValueError(
                f"Supplied table {table} does not exist in {self.ddl.database} database."
            )

    def _generate_name_desc_df(
        self,
        column_stubname: str,
        name_vals: Optional[Union[str, List[str]]] = None,
        description_vals: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Internal function to generate a DataFrame for insert into one of the
        Id/Name/Description tables.

        Args:
            column_stubname: The base column to append name or description to. Also used
                             to identify character limits using a global variable.
            name_vals: Names to insert into the database.
            description_vals: Descriptions to insert into the database.
        Returns:
            A DataFrame of names and descriptions for a given base column.
        """

        # Pull allowed number of characters for this table
        # We'll use this as reference for which parameters are expected None
        allowed_chars = name_desc_limits[column_stubname]

        # Init DataFrame
        insert = pd.DataFrame({})

        # Loop over keys (some combination of name +/- description)
        for col in allowed_chars.keys():
            # Cast as list
            insert_vals = eval(f"{col}_vals")
            if isinstance(eval(f"{col}_vals"), str):
                insert_vals = [insert_vals]

            # Truncate to expected number of characters
            insert_vals = [val[0 : allowed_chars[col]] for val in insert_vals]

            # Insert into DataFrame
            insert[f"{column_stubname}_{col}"] = insert_vals

        # Return
        return insert

    def _insert_into_name_desc_tables_wrapper(
        self,
        table_name: str,
        name_vals: Optional[Union[str, List[str]]] = None,
        description_vals: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """Wrapper function to insert into any specified Id/Name/Description table

        Args:
            table_name: Name of the table to insert name and description values into. Uses
                        a global variable to define supported tables and character lengths.
            name_vals: Names to insert into the database.
            description_vals: Descriptions to insert into the database.
        """
        print("Beginning insert(s).")

        # Generate the insert DataFrame
        insert = self._generate_name_desc_df(table_name, name_vals, description_vals)

        # Call insert_rows
        self.ddl.insert_rows(table_name, insert)

        print("Insert(s) complete!")

    @lru_cache(maxsize=15)
    def _define_run_metadata_columns(self) -> List[str]:
        """Internal method to return a list of columns in table

        Returns:
            List of columns present in run metadata, excluding run and clinical data type IDs.
        """
        cols = pd.read_sql(
            QUERY, self.ddl.connection
        ).columns.tolist()

        return [col for col in cols if col not in ["run_id", "clinical_data_type_id"]]

    def _init_base_run_metadata_df(
        self, run_id: Union[int, List[int]], clinical_data_type_id: Union[int, List[int]]
    ) -> pd.DataFrame:
        """Internal method to return a small DataFrame with run_id and clinical_data_type_id
        columns for run_metadata

        Args:
            run_id: Clinical run.
            clinical_data_type_id: Clinical data type.
        Returns:
            A DataFrame containing the input runs and data types.
        """
        return pd.DataFrame(
            {"run_id": [run_id], "clinical_data_type_id": [clinical_data_type_id]}
        )

    def _validate_run_metadata_foreign_key_reference(
        self, fk_column_name: str, fk_value: int
    ) -> None:
        """Validate that for a given run_metadata foreign key the intended insert value
        exists in the foreign table.

        Args:
            fk_column_name: The column and table to check against.
            fk_value: A value to confirm exists in the column and table of interest.

        Raises:
            ValueError if the input foreign key value does not exist in the table.
        """
        # Reference table name and pull existing IDs
        fk_table_name = fk_column_name[0:-3]
        existing_ids = pd.read_sql(
            QUERY,
            self.ddl.connection,
        )[fk_column_name].tolist()

        # Validate that the fk_value exists in the existing_ids
        if fk_value not in existing_ids:
            raise ValueError(QUERY
            )

    def insert_into_adhoc_methods(
        self,
        adhoc_methods_name: Union[str, List[str]],
        adhoc_methods_description: Union[str, List[str]],
    ) -> None:
        """Insert new observation(s) into table"""
        self._insert_into_name_desc_tables_wrapper(
            "adhoc_methods", adhoc_methods_name, adhoc_methods_description
        )

    def insert_into_age_sex_weights_version(
        self,
        age_sex_weights_version_name: Union[str, List[str]],
        age_sex_weights_version_description: Union[str, List[str]],
        run_id: Union[int, List[int]],
    ) -> None:
        """Insert new observation(s) into table. Note: This
        table must contain a run_id as well as the standard name and description.
        """
        insert = self._generate_name_desc_df(
            "age_sex_weights_version",
            age_sex_weights_version_name,
            age_sex_weights_version_description,
        )
        insert["run_id"] = [run_id]

        self.ddl.insert_rows("age_sex_weights_version", insert)

    def insert_into_cf_version_set(
        self,
        cf_version_set_name: Union[str, List[str]],
        cf_version_set_description: Union[str, List[str]],
    ) -> None:
        """Insert new observation(s) into table."""
        self._insert_into_name_desc_tables_wrapper(
            "cf_version_set", cf_version_set_name, cf_version_set_description
        )

    def insert_into_clinical_data_universe(
        self, clinical_data_universe_description: Union[str, List[str]]
    ) -> None:
        """Insert observation(s) into table."""
        self._insert_into_name_desc_tables_wrapper(
            "clinical_data_universe", description_vals=clinical_data_universe_description
        )

    def insert_into_clinical_methods(
        self,
        clinical_methods_name: Union[str, List[str]],
        clinical_methods_description: Union[str, List[str]],
    ) -> None:
        """Insert observaton(s) into table."""
        self._insert_into_name_desc_tables_wrapper(
            "clinical_methods", clinical_methods_name, clinical_methods_description
        )

    def insert_into_clinical_version(
        self,
        clinical_version_name: Union[str, List[str]],
        clinical_version_description: Union[str, List[str]],
    ) -> None:
        """Insert observation(s) into table."""
        self._insert_into_name_desc_tables_wrapper(
            "clinical_version", clinical_version_name, clinical_version_description
        )

    def insert_into_envelope_type(
        self,
        envelope_model_id: Union[int, List[int]],
        envelope_model_type: Union[str, List[str]],
    ) -> None:
        """Insert observation(s) into table."""
        print("Beginning insert(s).")

        # Casting both args as lists
        if isinstance(envelope_model_id, int):
            envelope_model_id = [envelope_model_id]
        if isinstance(envelope_model_type, str):
            envelope_model_type = [envelope_model_type]

        # Ensure envelope_model_type is one of [dismod, stgpr]
        if any([mtype not in ["dismod", "stgpr"] for mtype in envelope_model_type]):
            raise ValueError(
                "Supplied one or more envelope_model_types which do not match: "
                "['dismod', 'stgpr']."
            )

        # Turn into Pandas DF
        insert = pd.DataFrame(
            {
                "envelope_model_id": envelope_model_id,
                "envelope_model_type": envelope_model_type,
            }
        )

        # Ensure that the envelope_model_id + envelope_model_type combo doesn't already
        # exist in the table
        existing_rows = pd.read_sql(
            QUERY, self.ddl.connection
        )
        for idx, row in existing_rows.iterrows():
            merge_check = pd.merge(insert, existing_rows, how="inner")
            if len(merge_check) > 0:
                raise ValueError(
                    f"Supplied {len(merge_check)} envelope_model_id + envelope_model_type "
                    "combination(s) which is/are already present "
                    "Please reference clinical_envelope_id(s) "
                    f"{merge_check.clinical_envelope_id.tolist()} for whatever run you are "
                    "about to launch."
                )

        # Generate a clinical_envelope_id for the insert
        ce_ids = [max(existing_rows.clinical_envelope_id) + 1] * len(insert)
        insert["clinical_envelope_id"] = ce_ids

        # Call insert_rows
        self.ddl.insert_rows("envelope_type", insert)

        print("Insert(s) complete!")

    def insert_into_status(self, status_name: Union[str, List[str]]) -> None:
        """Insert observation(s) into table."""
        self._insert_into_name_desc_tables_wrapper("status", name_vals=status_name)

    def initialize_run_metadata(
        self,
        run_id: int,
        clinical_data_type_id: int,
        release_id: int,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        map_version: Optional[int] = None,
        age_sex_weights_version_id: Optional[int] = None,
        adhoc_methods_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        clinical_methods_id: Optional[int] = None,
        clinical_data_universe_id: Optional[int] = None,
        inj_cf_input_version_id: Optional[int] = None,
        inj_cf_version_id: Optional[int] = None,
    ) -> None:
        """Initialize the table filling as many columns as are provided
        by the user.

        run_id: Standard clinical team run_id. One of the two composite keys of the
                run_metadata table.
        clinical_data_type_id: The type of clinical data to pull metadata for. The other
                               composite key on the run_metadata table.
        release_id: GBD release to associate with a given run and clinical data type.
        cf_version_set_id: Correction factor version.
        clinical_envelope_id:
        map_version: Standard clinical map version.
        age_sex_weights_version_id: A versioned set of weights to use when age and sex
                                    splitting inpatient pipeline data.
        adhoc_methods_id: A catch-all ID that can be used to track one-off changes.
        population_run_id: The run_id to pass when calling get_population.
        haqi_model_version_id: The version of the health access and quality covariate.
        asfr_model_version_id: The version of the age-sex fertility covariate.
        live_births_by_sex_model_version_id: The version of the live birth covariate.
        ifd_coverage_prop_model_version_id: The version of the in-facility delivery covariate.
        clinical_data_universe_id: The version of the clinical data universe to use, eg a set
                                   of all valid NIDs that could potentially be used while
                                   processing clinical data for all pipelines.
        inj_cf_input_version_id:  The version of input data to use when creating injury
                                  correction data.
        inj_cf_version_id: The version of injury correction factors to apply to injury bundles.

        """
        # Ensure the run_id + clinical_data_type_id combination doesn't already exist
        existing_data = pd.read_sql(
            QUERY,
            self.ddl.connection,
        )
        for idx, row in existing_data.iterrows():
            if (row.run_id == run_id) & (row.clinical_data_type_id == clinical_data_type_id):
                raise ValueError(
                    f"This run_id + clinical_data_type_id combination (run_id {run_id}, "
                    f"clinical_data_type_id {clinical_data_type_id}) already exists and "
                    " cannot be re-inserted. If you meant to update "
                    "the observations for this combination, please use update_run_metadata()."
                )
            if (row.run_id == run_id) & (row.release_id != release_id):
                raise ValueError(
                    f"There is a different existing release_id {release_id} associated with "
                    "this run_id. Please consider using a new run_id."
                )

        # Ensure a foreign key error isn't about to happen for each provided parameter
        for col in [
            "cf_version_set_id",
            "age_sex_weights_version_id",
            "adhoc_methods_id",
            "clinical_methods_id",
            "clinical_data_universe_id",
            "inj_cf_input_version_id",
            "inj_cf_version_id",
        ]:
            if eval(col) is not None:
                self._validate_run_metadata_foreign_key_reference(col, eval(col))

        print("Beginning insert.")
        # Initialize insert DataFrame
        insert = self._init_base_run_metadata_df(run_id, clinical_data_type_id)

        # For every present paremeter, add it to the DataFrame
        for col in self._define_run_metadata_columns():
            if eval(col) is not None:
                insert[col] = [eval(col)]

        # Call insert_rows
        for idx, row in insert.iterrows():
            self.ddl.insert_rows("run_metadata", insert)

        print("Insert complete!")

    def update_run_metadata(
        self,
        run_id: int,
        clinical_data_type_id: int,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        map_version: Optional[int] = None,
        age_sex_weights_version_id: Optional[int] = None,
        adhoc_methods_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        clinical_methods_id: Optional[int] = None,
        clinical_data_universe_id: Optional[int] = None,
        inj_cf_version_id: Optional[int] = None,
        inj_cf_input_version_id: Optional[int] = None,
        release_id: Optional[int] = None,
        ignore_overwrite_errors: bool = False,
    ) -> None:
        """Update the table filling as many columns as are provided
        by the user.
        """
        # Ensure that the run_id + clinical_data_type_id combination already exist
        existing_data = pd.read_sql(
            QUERY,
            self.ddl.connection,
        )
        if clinical_data_type_id not in existing_data.clinical_data_type_id.tolist():
            raise ValueError(
                f"Supplied run_id {run_id} does not exist for "
                f"clinical_data_type_id {clinical_data_type_id}."
            )
        existing_data = existing_data.loc[
            existing_data.clinical_data_type_id == clinical_data_type_id
        ].reset_index(drop=True)

        # If the user wants an error raised for overwriting data
        if not ignore_overwrite_errors:
            # Loop through each parameter
            for col in self._define_run_metadata_columns():
                # If the paremeter has been passed in
                if eval(col) is not None:
                    # Check for an existing value
                    if existing_data.loc[0, col] is not None:
                        raise ValueError(
                            f"Value of {existing_data.loc[0, col]} already present in {col} "
                            f"for run_id {run_id} and "
                            f"clinical_data_type_id {clinical_data_type_id}."
                        )

        # Ensure a foreign key error isn't about to happen for each provided parameter
        for col in [
            "cf_version_set_id",
            "age_sex_weights_version_id",
            "adhoc_methods_id",
            "clinical_methods_id",
            "clinical_data_universe_id",
        ]:
            if eval(col) is not None:
                self._validate_run_metadata_foreign_key_reference(col, eval(col))

        # Compile update DataFrame
        update = self._init_base_run_metadata_df(run_id, clinical_data_type_id)
        for col in self._define_run_metadata_columns():
            if eval(col) is not None:
                update[col] = [eval(col)]

        # Call update_rows()
        self.ddl.update_rows("run_metadata", update, ["run_id", "clinical_data_type_id"])

    def pull_run_metadata(
        self, run_id: Union[int, List[int]], clinical_data_type_id: Union[int, List[int]]
    ) -> pd.DataFrame:
        """Function to pull from run_metadata table for a given run_id
        and clinical_data_type.

        Args:
            run_id : Clinical runs of interest.
            clinical_data_type_id : clinical data types of interest.
        Returns:
            A DataFrame of metadata.
        """
        # Cast args as list of not already
        if not isinstance(run_id, list):
            run_id = [run_id]
        if not isinstance(clinical_data_type_id, list):
            clinical_data_type_id = [clinical_data_type_id]

        # Define query
        query = (
            QUERY
        )

        # Execute
        run_metadata = pd.read_sql(query, self.ddl.connection)

        # Return
        return run_metadata

    def pull_envelope_model_id(self, run_id: int, clinical_data_type_id: int) -> Optional[int]:
        """Function to pull the envelope_model_id associated with a given run_id +
        clinical_data_type_id.

        Args:
            run_id: Clinical run.
            clinical_data_type_id: Clinical data type.
        Returns:
            An envelope model ID associated with the given run and data type.
        """
        # Validate that the run_id + clinical_data_type_id exists
        existing_data = pd.read_sql(
            (
                QUERY
            ),
            self.ddl.connection,
        )
        if len(existing_data) == 0:
            raise ValueError(
                f"There is no run_metadata associated with run_id {run_id} and "
                f"clinical_data_type_id {clinical_data_type_id}."
            )

        # Isolate the clinical_envelope_id
        ce_id = existing_data.clinical_envelope_id[0]
        if ce_id is None:
            return ce_id

        # Pull and return the envelope_model_id
        return pd.read_sql(
            QUERY,
            self.ddl.connection,
        ).envelope_model_id[0]

    def __str__(self):
        return "PipelineWrappers object:\n" f"  {self.ddl}"

    def __repr__(self):
        return "PipelineWrappers object:\n" f"  {self.ddl}"

    def __del__(self) -> None:
        del self.ddl


class InpatientWrappers(PipelineWrappers):
    """Child class which contains methods and attributes specific to inpatient pipeline
    process.

    Attributes:
        run_id: A clinical run of interest to read and write metadata.
        odbc_profile: The odbc connection profile to use when instantiating the DDL.
        odbc_path: File path to pull odbc profile from.
        clinical_data_type: Hardcoded to the inpatient type.
    """

    def __init__(
        self,
        run_id: int,
        odbc_profile: str,
        odbc_path: str = "FILEPATH/.odbc.ini",
    ) -> None:
        super().__init__(odbc_profile, odbc_path)
        self.run_id = run_id
        self.clinical_data_type_id = 1

    def initialize_run_metadata(  # type: ignore
        self,
        adhoc_methods_id: int,
        asfr_model_version_id: int,
        clinical_data_universe_id: int,
        clinical_envelope_id: int,
        clinical_methods_id: int,
        haqi_model_version_id: int,
        ifd_coverage_prop_model_version_id: int,
        live_births_by_sex_model_version_id: int,
        map_version: int,
        population_run_id: int,
        release_id: int,
        age_sex_weights_version_id: Optional[int] = None,
        cf_version_set_id: Optional[int] = None,
        inj_cf_input_version_id: Optional[int] = None,
        inj_cf_version_id: Optional[int] = None,
    ) -> None:
        """Convenience function to pass self attributes to superclass's
        initialize_run_metadata. Retains only the arguments relevant to the
        inpatient pipeline.
        """
        # Check that the required parameters have been passed in
        for col in [
            "adhoc_methods_id",
            "asfr_model_version_id",
            "clinical_data_universe_id",
            "clinical_envelope_id",
            "clinical_methods_id",
            "haqi_model_version_id",
            "ifd_coverage_prop_model_version_id",
            "live_births_by_sex_model_version_id",
            "map_version",
            "population_run_id",
        ]:
            if eval(col) is None:
                raise ValueError(f"Value required for column {col} but none supplied.")

        # Call superclass method
        super().initialize_run_metadata(
            run_id=self.run_id,
            clinical_data_type_id=self.clinical_data_type_id,
            adhoc_methods_id=adhoc_methods_id,
            asfr_model_version_id=asfr_model_version_id,
            clinical_data_universe_id=clinical_data_universe_id,
            clinical_envelope_id=clinical_envelope_id,
            clinical_methods_id=clinical_methods_id,
            haqi_model_version_id=haqi_model_version_id,
            ifd_coverage_prop_model_version_id=ifd_coverage_prop_model_version_id,
            live_births_by_sex_model_version_id=live_births_by_sex_model_version_id,
            map_version=map_version,
            population_run_id=population_run_id,
            age_sex_weights_version_id=age_sex_weights_version_id,
            cf_version_set_id=cf_version_set_id,
            inj_cf_input_version_id=inj_cf_input_version_id,
            inj_cf_version_id=inj_cf_version_id,
            release_id=release_id,
        )

    def update_run_metadata(  # type:ignore
        self,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        map_version: Optional[int] = None,
        age_sex_weights_version_id: Optional[int] = None,
        adhoc_methods_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        clinical_methods_id: Optional[int] = None,
        clinical_data_universe_id: Optional[int] = None,
        inj_cf_version_id: Optional[int] = None,
        inj_cf_input_version_id: Optional[int] = None,
        release_id: Optional[int] = None,
        ignore_overwrite_errors: bool = False,
    ) -> None:
        """Convenience function to pass self attributes to superclass's update_run_metadata."""
        # Call superclass method
        super().update_run_metadata(
            self.run_id,
            self.clinical_data_type_id,
            cf_version_set_id,
            clinical_envelope_id,
            map_version,
            age_sex_weights_version_id,
            adhoc_methods_id,
            population_run_id,
            haqi_model_version_id,
            asfr_model_version_id,
            live_births_by_sex_model_version_id,
            ifd_coverage_prop_model_version_id,
            clinical_methods_id,
            clinical_data_universe_id,
            inj_cf_version_id,
            inj_cf_input_version_id,
            release_id,
            ignore_overwrite_errors,
        )

    def pull_run_metadata(self) -> pd.DataFrame:  # type: ignore
        """Convenience function to pass self attributes to superclass's pull_run_metadata."""
        # Call superclass method
        run_metadata = super().pull_run_metadata(self.run_id, self.clinical_data_type_id)

        # Raise warning if user is calling this method before initializing the run
        if len(run_metadata) == 0:
            warnings.warn(
                f"Did not find any run metadata associated with run_id {self.run_id} and "
                f"clinical_data_type_id {self.clinical_data_type_id} - you likely have not "
                "yet initialized this run_id + clinical_data_type_id combination. Please "
                "reference the initialize_run_metadata() method to get started with this run."
            )

        # Always return
        return run_metadata

    def pull_envelope_model_id(self) -> Optional[int]:  # type: ignore
        """Convenience function to pass self attributes to superclass's
        pull_envelope_model_id.
        """
        return super().pull_envelope_model_id(self.run_id, self.clinical_data_type_id)

    def insert_into_age_sex_weights_version(  # type: ignore
        self,
        age_sex_weights_version_name: Union[str, List[str]],
        age_sex_weights_version_description: Union[str, List[str]],
    ) -> None:
        """Convenience function to pass self attributes to superclass's method."""
        super().insert_into_age_sex_weights_version(
            age_sex_weights_version_name, age_sex_weights_version_description, self.run_id
        )

    def insert_into_inj_cf_version(
        self,
        inj_cf_version_name: Union[str, List[str]],
        inj_cf_version_description: Union[str, List[str]],
        run_id: Union[int, List[int]],
    ) -> None:
        """
        Insert new observation(s) into clinical injury correction factor version table.

        Args:
            inj_cf_version_name: Name(s) of the injury correction factors version.
            inj_cf_version_description: Description(s) of the injury correction
                                        factors version.
            run_id: Run ID(s) of interest to populate the injury cf table.
        """
        insert = super()._generate_name_desc_df(
            "inj_cf_version", inj_cf_version_name, inj_cf_version_description
        )
        insert["run_id"] = [run_id]

        self.ddl.insert_rows("inj_cf_version", insert)

    def insert_into_inj_cf_input_version(
        self,
        inj_cf_input_version_name: Union[str, List[str]],
        inj_cf_input_version_description: Union[str, List[str]],
        run_id: Union[int, List[int]],
    ) -> None:
        """Insert new observation(s) into clinical injury correction factor input table.

        Args:
            inj_cf_input_version_name: Name(s) of the injury correction factor input version.
            inj_cf_input_version_description: Description(s) of the injury correction factor
                                              inputs version.
            run_id: Run ID(s) of interest to populate the input table.
        """
        insert = super()._generate_name_desc_df(
            "inj_cf_input_version", inj_cf_input_version_name, inj_cf_input_version_description
        )
        insert["run_id"] = [run_id]

        self.ddl.insert_rows("inj_cf_input_version", insert)

    def pull_version_dir(self, config_key: str) -> str:
        """Returns a directory containing versioned inputs for the provided key.

        Args:
            config_key: The key to identify input data of interest.

        Returns:
            A directory of versioned input data.
        """
        ddm = DataDirManager(config_key)
        # method automatically subsets to clinical_data_type_id 1
        try:
            r_m = self.pull_run_metadata()
        except:  # noqa
            self.ddl.connection = self.ddl._create_connection()
            r_m = self.pull_run_metadata()

        return ddm.pull_version_dir(r_m)

    def create_version_dir(
        self, config_key: str, version_id: int, return_dir=False
    ) -> Optional[str]:
        """Creates a new versioned directory for the input data key type.

        Args:
            config_key: The key to identify input data of interest.
            version_id: Version of the input data.
            return_dir: New directory where versioned inputs will be stored. Defaults to False.

        Returns:
            The newly created directory if return_dir is True.
        """
        ddm = DataDirManager(config_key)
        new_dir = ddm.create_version_dir(version_id)
        if return_dir:
            return new_dir
        else:
            return None

    def __str__(self):
        return (
            "InpatientWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __repr__(self):
        return (
            "InpatientWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __del__(self) -> None:
        del self.ddl


class OutpatientWrappers(PipelineWrappers):
    """Child class which contains methods and attributes specific to outpatient pipeline
    process.

    Attributes:
        run_id: A clinical run of interest to read and write metadata.
        odbc_profile: The odbc connection profile to use when instantiating the DDL.
        odbc_path: File path to pull odbc profile from.
        clinical_data_type: Hardcoded to the outpatient type.
    """

    def __init__(
        self,
        run_id: int,
        odbc_profile: str,
        odbc_path: str = "FILEPATH/.odbc.ini",
    ) -> None:
        super().__init__(odbc_profile, odbc_path)
        self.run_id = run_id
        self.clinical_data_type_id = 2

    def initialize_run_metadata(  # type: ignore
        self,
        adhoc_methods_id: int,
        clinical_data_universe_id: int,
        clinical_methods_id: int,
        map_version: int,
        release_id: int,
        age_sex_weights_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
    ) -> None:
        """Convenience function to pass self attributes to superclass's
        initialize_run_metadata. Retains only the arguments relevant to the
        outpatient pipeline.
        """
        # Check that the required parameters have been passed in
        for col in [
            "adhoc_methods_id",
            "clinical_data_universe_id",
            "clinical_methods_id",
            "map_version",
        ]:
            if eval(col) is None:
                raise ValueError(f"Value required for column {col} but none supplied.")

        # Call superclass method
        super().initialize_run_metadata(
            run_id=self.run_id,
            clinical_data_type_id=self.clinical_data_type_id,
            adhoc_methods_id=adhoc_methods_id,
            asfr_model_version_id=asfr_model_version_id,
            clinical_data_universe_id=clinical_data_universe_id,
            clinical_envelope_id=clinical_envelope_id,
            clinical_methods_id=clinical_methods_id,
            haqi_model_version_id=haqi_model_version_id,
            ifd_coverage_prop_model_version_id=ifd_coverage_prop_model_version_id,
            live_births_by_sex_model_version_id=live_births_by_sex_model_version_id,
            map_version=map_version,
            population_run_id=population_run_id,
            age_sex_weights_version_id=age_sex_weights_version_id,
            cf_version_set_id=cf_version_set_id,
            release_id=release_id,
        )

    def update_run_metadata(  # type: ignore
        self,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        map_version: Optional[int] = None,
        age_sex_weights_version_id: Optional[int] = None,
        adhoc_methods_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        clinical_methods_id: Optional[int] = None,
        clinical_data_universe_id: Optional[int] = None,
        release_id: Optional[int] = None,
        ignore_overwrite_errors: bool = False,
    ) -> None:
        """
        Convenience function to pass self attributes to superclass's update_run_metadata.
        """
        # Call superclass method
        super().update_run_metadata(
            self.run_id,
            self.clinical_data_type_id,
            cf_version_set_id,
            clinical_envelope_id,
            map_version,
            age_sex_weights_version_id,
            adhoc_methods_id,
            population_run_id,
            haqi_model_version_id,
            asfr_model_version_id,
            live_births_by_sex_model_version_id,
            ifd_coverage_prop_model_version_id,
            clinical_methods_id,
            clinical_data_universe_id,
            ignore_overwrite_errors,
            release_id,
        )

    def pull_run_metadata(self) -> pd.DataFrame:  # type: ignore
        """Convenience function to pass self attributes to superclass's pull_run_metadata."""
        # Call superclass method
        run_metadata = super().pull_run_metadata(self.run_id, self.clinical_data_type_id)

        # Raise warning if user is calling this method before initializing the run
        if len(run_metadata) == 0:
            warnings.warn(
                f"Did not find any run metadata associated with run_id {self.run_id} and "
                f"clinical_data_type_id {self.clinical_data_type_id} - you likely have not "
                "yet initialized this run_id + clinical_data_type_id combination. Please "
                "reference the initialize_run_metadata() method to get started with this run."
            )

        # Always return
        return run_metadata

    def pull_envelope_model_id(self) -> Optional[int]:  # type: ignore
        """Convenience function to pass self attributes to superclass's
        pull_envelope_model_id.
        """
        return super().pull_envelope_model_id(self.run_id, self.clinical_data_type_id)

    def insert_into_age_sex_weights_version(  # type: ignore
        self,
        age_sex_weights_version_name: Union[str, List[str]],
        age_sex_weights_version_description: Union[str, List[str]],
    ) -> None:
        # Call superclass method
        super().insert_into_age_sex_weights_version(
            age_sex_weights_version_name, age_sex_weights_version_description, self.run_id
        )

    def __str__(self):
        return (
            "OutpatientWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __repr__(self):
        return (
            "OutpatientWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __del__(self) -> None:
        del self.ddl


class ClaimsWrappers(PipelineWrappers):
    """Child class which contains methods and attributes specific to claims pipeline
    process.

    Attributes:
        run_id: A clinical run of interest to read and write metadata.
        odbc_profile: The odbc connection profile to use when instantiating the DDL.
        odbc_path: File path to pull odbc profile from.
        clinical_data_type: Defaults to the claims (non-flagged) data type.
    """

    def __init__(
        self,
        run_id: int,
        odbc_profile: str,
        odbc_path: str = "FILEPATH/.odbc.ini",
        clinical_data_type_id: int = 3,
    ) -> None:
        super().__init__(odbc_profile, odbc_path)
        self.run_id = run_id
        if clinical_data_type_id not in [3, 4, 5]:
            msg = "Claims clinical_data_type_id must be one of 3, 4, 5"
            ext = f"found '{clinical_data_type_id}'"
            raise ValueError(f"{msg}, {ext}")
        self.clinical_data_type_id = clinical_data_type_id

    def initialize_run_metadata(  # type: ignore
        self,
        adhoc_methods_id: int,
        asfr_model_version_id: int,
        clinical_data_universe_id: int,
        clinical_methods_id: int,
        map_version: int,
        release_id: int,
        age_sex_weights_version_id: Optional[int] = None,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
    ) -> None:
        """Convenience function to pass self attributes to superclass's
        initialize_run_metadata. Retains only the arguments relevant to a
        claims pipeline.
        """
        # Check that the required parameters have been passed in
        for col in [
            "adhoc_methods_id",
            "asfr_model_version_id",
            "clinical_data_universe_id",
            "clinical_methods_id",
            "map_version",
        ]:
            if eval(col) is None:
                raise ValueError(f"Value required for column {col} but none supplied.")

        # Call superclass method
        super().initialize_run_metadata(
            run_id=self.run_id,
            clinical_data_type_id=self.clinical_data_type_id,
            adhoc_methods_id=adhoc_methods_id,
            asfr_model_version_id=asfr_model_version_id,
            clinical_data_universe_id=clinical_data_universe_id,
            clinical_envelope_id=clinical_envelope_id,
            clinical_methods_id=clinical_methods_id,
            haqi_model_version_id=haqi_model_version_id,
            ifd_coverage_prop_model_version_id=ifd_coverage_prop_model_version_id,
            live_births_by_sex_model_version_id=live_births_by_sex_model_version_id,
            map_version=map_version,
            population_run_id=population_run_id,
            age_sex_weights_version_id=age_sex_weights_version_id,
            cf_version_set_id=cf_version_set_id,
            release_id=release_id,
        )

    def update_run_metadata(  # type: ignore
        self,
        cf_version_set_id: Optional[int] = None,
        clinical_envelope_id: Optional[int] = None,
        map_version: Optional[int] = None,
        age_sex_weights_version_id: Optional[int] = None,
        adhoc_methods_id: Optional[int] = None,
        population_run_id: Optional[int] = None,
        haqi_model_version_id: Optional[int] = None,
        asfr_model_version_id: Optional[int] = None,
        live_births_by_sex_model_version_id: Optional[int] = None,
        ifd_coverage_prop_model_version_id: Optional[int] = None,
        clinical_methods_id: Optional[int] = None,
        clinical_data_universe_id: Optional[int] = None,
        release_id: Optional[int] = None,
        ignore_overwrite_errors: bool = False,
    ) -> None:
        """
        Convenience function to pass self attributes to superclass's update_run_metadata.
        """
        # Call superclass method
        super().update_run_metadata(
            self.run_id,
            self.clinical_data_type_id,
            cf_version_set_id,
            clinical_envelope_id,
            map_version,
            age_sex_weights_version_id,
            adhoc_methods_id,
            population_run_id,
            haqi_model_version_id,
            asfr_model_version_id,
            live_births_by_sex_model_version_id,
            ifd_coverage_prop_model_version_id,
            clinical_methods_id,
            clinical_data_universe_id,
            ignore_overwrite_errors,
            release_id,
        )

    def pull_run_metadata(self) -> pd.DataFrame:  # type: ignore
        """Convenience function to pass self attributes to superclass's pull_run_metadata."""
        # Call superclass method
        run_metadata = super().pull_run_metadata(self.run_id, self.clinical_data_type_id)

        # Raise warning if user is calling this method before initializing the run
        if len(run_metadata) == 0:
            warnings.warn(
                f"Did not find any run metadata associated with run_id {self.run_id} and "
                f"clinical_data_type_id {self.clinical_data_type_id} - you likely have not "
                "yet initialized this run_id + clinical_data_type_id combination. Please "
                "reference the initialize_run_metadata() method to get started with this run."
            )

        # Always return
        return run_metadata

    def pull_envelope_model_id(self) -> Optional[int]:  # type: ignore
        """Convenience function to pass self attributes to superclass's
        pull_envelope_model_id.
        """
        return super().pull_envelope_model_id(self.run_id, self.clinical_data_type_id)

    def insert_into_age_sex_weights_version(  # type: ignore
        self,
        age_sex_weights_version_name: Union[str, List[str]],
        age_sex_weights_version_description: Union[str, List[str]],
    ) -> None:
        # Call superclass method
        super().insert_into_age_sex_weights_version(
            age_sex_weights_version_name, age_sex_weights_version_description, self.run_id
        )

    def __str__(self):
        return (
            "ClaimsWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __repr__(self):
        return (
            "ClaimsWrappers object:\n"
            f"  run_id: {self.run_id}\n"
            f"  clinical_data_type_id: {self.clinical_data_type_id}\n"
            f"  {self.ddl}"
        )

    def __del__(self) -> None:
        del self.ddl