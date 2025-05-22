"""Pulls mapping data from the clinical_mapping database.

Contains the functions which pull and validate database tables to retrieve bundle, ICG, and
condition properties, the mappings from cause code to ICG and ICG to bundle, etc.

Example of pulling age/sex restrictions, measures and durations by Clinical bundle:
    get_clinical_process_data(table="bundle_properties", map_version=30, prod=True)
"""

import warnings
from functools import lru_cache
from typing import List, Union

import numpy as np
import pandas as pd
from crosscutting_functions.shared_constants.database import MappingTables
from crosscutting_functions.clinical_metadata_utils.database import Database
from loguru import logger

from crosscutting_functions.mapping import clinical_mapping_validations


def test_and_return_map_version(map_version: Union[str, int], prod: bool = True) -> int:
    """
    Validate that each clinical mapping table returns the same map version when pulled from
    the db via get_clinical_process_data

    Used in inpatient to validate a map_version at run-time before mapping to ICD.

    Args:
        map_version: Identifies which version of the mapping set to use. Must be an int
        or str "current".
        prod: If true the function will break if any tests fail.

    Raises:
        ValueError if more than 1 unique map version across tables is returned.
    """
    tables = (
        MappingTables.CAUSE_CODE_ICG,
        MappingTables.ICG_BUNDLE,
        MappingTables.ICG_PROPERTIES,
        MappingTables.BUNDLE_PROPERTIES,
    )
    version: List[int] = []
    for t in tables:
        tmp = get_clinical_process_data(t, map_version=map_version, prod=prod)
        version = list(set(version + tmp["map_version"].unique().tolist()))
    if len(version) != 1:
        raise ValueError("There are multiple map versions present: {}".format(version))

    return version[0]


@lru_cache(maxsize=10)
def get_current_map_version(odbc_profile: str = "DATABASE") -> int:
    """Gets the current map version under the .odbc.ini profile name 'odbc_profile'.

    Returns:
        An integer of the current (eg largest) map version stored in the ICG to bundle table.
    """
    q = (
        "QUERY"
    )
    db = get_db_conn(odbc=odbc_profile)
    query_result = db.query(q)

    map_version = query_result["map_version"].iloc[0]

    map_version = int(map_version)

    logger.info("The 'current' map version is version {}.".format(map_version))

    return map_version


def check_map_version(map_version: Union[str, int]) -> None:
    """
    Checks that map_version is an int or the string "current"

    Args:
        map_version: Identifies which version of the mapping set to use.

    Raises:
        ValueError if map_version is not an int or the string "current".
        ValueError if map_version is less than or equal zero (if
                    map_version was a number)

    """
    if not ((isinstance(map_version, (int, np.integer))) | (map_version == "current")):
        raise ValueError("map_version must be either an integer or 'current'.")
    if isinstance(map_version, int):
        if map_version <= 0 and map_version != -9999:
            raise ValueError("map_version should be larger than 0.")


@lru_cache(maxsize=10)
def get_clinical_process_data(
    table: str, map_version: Union[str, int], prod: bool = True
) -> pd.DataFrame:
    """
    Connect to the Clinical Mapping database and pull and return the table specified.
    Current options can be found in mapping_constants MappingTables. This function
    will work for any other table present in clinical mapping but the results will not be
    validated.

    Args:
        table: The name of the table you'd like to pull, in full, from the database.
        map_version: Identifies which version of the mapping set to use.
        prod: If true the function will break if any tests fail.

    Returns:
        A DataFrame of the table requested.

    Raises:
        ValueError if an empty DataFrame is returned.
    """

    check_map_version(map_version)
    if map_version == "current":
        map_version = get_current_map_version()

    unversioned = [MappingTables.ICG, MappingTables.CODE_SYSTEM, MappingTables.CONDITION]
    if table in unversioned:
        where_state = ""
    else:
        where_state = f"WHERE map_version = {map_version}"

    # run the query on the dB
    mapping_query = """QUERY"""

    db = get_db_conn()
    df = db.query(mapping_query)

    if df.empty:
        raise ValueError(
            "Unable to retrieve data for given table and map_version. "
            "Please check both values\n"
            "The {} table was requested with map version {}".format(table, map_version)
        )

    # retain essential behavior of including the 'icg_name' column
    if table in (MappingTables.CAUSE_CODE_ICG, MappingTables.ICG_BUNDLE):
        df = df.merge(
            get_clinical_process_data(MappingTables.ICG, map_version), validate="m:1"
        )
    elif table == MappingTables.CAUSE_CODE_CONDITION:
        df = df.merge(
            get_clinical_process_data(MappingTables.CONDITION, map_version)[
                ["condition_id", "condition_name"]
            ],
            validate="m:1",
        )

    # validate the table
    logger.info(clinical_mapping_validations.clinfo_process_validation(df, table, prod=prod))

    return df


@lru_cache(maxsize=10)
def create_bundle_restrictions(map_version: Union[str, int]) -> pd.DataFrame:
    """
    Pull the bundle-level age and sex restrictions from the
    database.

    Args:
        map_version: Identifies which version of the mapping set to use. Must be an int
        or str "current".

    Returns:
        A DataFrame of the age and sex restrictions for all bundles in the specified
        map version.
    """

    check_map_version(map_version)
    if map_version == "current":
        map_version = get_current_map_version()

    q = """QUERY"""

    db = get_db_conn()
    df = db.query(q)

    return df


def get_db_conn(odbc="DATABASE") -> Database:
    """Create a connection to a database given an odbc profile. Defaults to the
     ACCOUNT account on the SERVER host."""
    db = Database()
    db.load_odbc(odbc)
    return db


def get_active_bundles(map_version: Union[str, int], **kwargs) -> pd.DataFrame:
    """
    Pulls data from the Active Bundle Metadata table in the db.
    Defaults to pulling bundle, estimate, and clinical age group IDs from the production
    epi database, but this can be modified using keyword arguments.

    Args:
        map_version: Identifies which version of the mapping set to use.
        kwargs: Features of active_bundle_metadata. additionaly the k
                cols will select that subset of features.

    Returns:
        A DataFrame containing a subset of columns in active bundle metadata for a specified
        map version.
    """
    if "cols" in kwargs.keys():
        cols = kwargs["cols"]
    else:
        cols = ["bundle_id", "estimate_id", "clinical_age_group_set_id", "measure_id"]

    if "odbc" in kwargs.keys():
        db = get_db_conn(kwargs["odbc"])
    else:
        db = get_db_conn()
    if map_version == "current":
        map_version = db.query(
            "QUERY"
        )["mv"].item()

    dql = """QUERY"""
    for k, v in kwargs.items():
        if k == "cols" or k == "odbc":
            continue
        elif k not in cols:
            warnings.warn("You're filtering data on a column that is not being returned")
        elif k in cols:
            dql += f"AND {k} IN ({str(v)[1:-1]})"

    df = db.query(dql)
    return df[list(cols)]


@lru_cache(maxsize=10)
def create_bundle_durations(map_version: Union[str, int]) -> pd.DataFrame:
    """
    Pull the bundle-level incidence durations from the DATABASE table in the
    DATABASE database.

    Args:
        map_version: Identifies which version of the mapping set to use.

    Returns:
        A DataFrame containing bundles and their durations in days for a specific map version.
    """

    check_map_version(map_version)
    if map_version == "current":
        map_version = get_current_map_version()

    q = """QUERY"""

    db = get_db_conn()
    df = db.query(q)

    return df


@lru_cache(maxsize=10)
def get_bundle_measure(map_version: Union[str, int]) -> pd.DataFrame:
    """Pull the bundle-level measures from the DATABASE table in the
    DATABASE database.

    Args:
        map_version: Identifies which version of the mapping set to use.

    Returns:
        A DataFrame containing bundles and their measures for a specified map version.
    """

    check_map_version(map_version)
    if map_version == "current":
        map_version = get_current_map_version()

    q = """QUERY"""
    db = get_db_conn()
    df = db.query(q)

    return df


def get_code_sys_by_bundle(bundle_id: int, map_version: Union[str, int]) -> List[int]:
    """For a given bundle_id and map_version return the code systems where that bundle exists.
    Uses existing clinical mapping functions rather than a direct sql query for consistency

    Args:
        bundle_id: ID of interest to pull the code systems it contains a mapping for.
        map_version: Identifies which version of the mapping set to use.

    Returns:
        A list of code system ids which the bundle can be mapped to.
    """

    # pull the cause code, icg, bundle maps
    bundle_df = get_clinical_process_data(MappingTables.ICG_BUNDLE, map_version=map_version)
    bundle_df = bundle_df[bundle_df["bundle_id"] == bundle_id]
    cause_code_df = get_clinical_process_data(
        MappingTables.CAUSE_CODE_ICG, map_version=map_version
    )
    # merge the cause code map onto the bundle map removing all other bundle codes
    cause_code_df = cause_code_df.merge(
        bundle_df, how="right", on=["icg_id", "map_version"], validate="m:1"
    )

    return cause_code_df["code_system_id"].unique().tolist()
