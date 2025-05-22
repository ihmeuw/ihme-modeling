import functools
from typing import List, Optional

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.values import legacy_cf_estimate_types
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import PipelineWrappers
from crosscutting_functions.clinical_metadata_utils.database import Database


def get_release_id(run_id: int) -> int:
    """Get release_id from run_metadata.

    WILL BREAK when the provided run_id has an invalid release_id.
    The assumption is that there should only be ONE unique release_id tied
    to any given run_id.

    Arguments:
        run_id: The run_id to get the release_id for.

    Raises:
        ValueError: If the run_id is invalid or has multiple release_ids.

    Returns:
        The release_id for the given run_id.
    """
    db = Database()
    db.load_odbc("CONN_DEF")
    if run_id <= 36:
        msg = f"Given run_id {run_id} <= 36. Please provide a run_id with a valid release_id."
        raise ValueError(msg)

    run_metadata = db.query("QUERY")
    if run_metadata.empty:
        raise ValueError(f"Invalid run_id given: {run_id}")

    releases = run_metadata.release_id.unique().tolist()

    if len(releases) == 1:
        release_id = releases[0]
    else:
        # This scenario shouldn't happen if run_metadata was initialized via clinical_ddl.
        msg = (
            f"There are {len(releases)} unique release_id with the given run_id {run_id}."
            "This should not happen in DATABASE - please update accordingly."
        )
        raise ValueError(msg)

    return int(release_id)


def get_is_otp_ids(
    estimate_id: Optional[int] = None,
    facility_type_id: Optional[int] = None,
    odbc_profile: Optional[str] = "DATABASE",
) -> List[int]:
    """Helper to retrieve is_otp values from DB for ICD-Mart partitions
    based on either an estimate ID or facility type ID. If both are args
    supplied, the input facility_type_id will be ignored. One must be supplied.

    Args:
        estimate_id: Clinial estimate ID. Values found in
            DATABASE table in the DB.
        facility_type_id: Values found in
            DATABASE table in the DB.
        odbc_profile: Inidcate dev or prod profile for database
            connection. Unless dev work, ONLY the default profile (prod) should
            be used, as this is our operating source of truth.

    Raises:
        ValueError: Neither is an estimate ID or facility type ID input.
        ValueError: Both estimate ID and facility type ID were input.

    Returns:
        ICD-Mart is_otp partition values.
    """

    db = Database()
    db.load_odbc(odbc_profile=odbc_profile)

    if not estimate_id and not facility_type_id:
        raise ValueError("An estimate OR a facility type must be input.")

    if estimate_id and facility_type_id:
        raise ValueError("Only an estimate_id or facility_type_id can be input.")

    if estimate_id:
        qstring = "QUERY" 
        facility_type_id = db.query(qstring)["facility_type_id"].item()

    qstring = "QUERY" 
    is_otp_ids = db.query(qstring)["is_otp_id"].to_list()
    is_otp_ids = [int(i) for i in is_otp_ids]

    return is_otp_ids


def get_next_run_id() -> int:
    """Queries Clinical DB to get next run_id to initialize into run_metadata.

    Raises:
        RuntimeError: If the run_id that ought to be the next run
            currently exists. Due to this run_id having no
            release_id.

    Returns:
        ID value for the next run for run_metadata.
    """

    # Get current run_id information.
    db = Database()
    db.load_odbc(odbc_profile="CONN_DEF")
    run_ids = "QUERY" 
    current_run_ids = db.query(run_ids)

    # Valid runs now MUST have a release_id associated with them.
    valid_runs = current_run_ids.loc[current_run_ids["release_id"].notnull()]
    # Increment run_id, but allow discontinuity for testing ids such as 900.
    min_val = valid_runs["run_id"].min()
    # Naively assume next is max + 1.
    max_val = valid_runs["run_id"].max() + 1
    # If there is a gap, incrementally fill the gap, otherwise max_val is correct.
    for id_val in range(min_val, max_val + 1):
        if id_val not in valid_runs["run_id"].to_list():
            run_id = int(id_val)
            break

    # Make sure the run_id doesn't exist already.
    if run_id in current_run_ids["run_id"].to_list():
        msg = f"run_id '{run_id}' already exists"
        reason = "Error occured due to this run having null release_id"
        raise RuntimeError(f"{msg}. {reason}")

    return run_id


def get_clin_dtypes(odbc_profile: str = "DATABASE") -> List[int]:
    """Gets unique clinical data type IDs from the DB.

    Args:
        odbc_profile: Indicates dev or prod DB.

    Returns:
        Unique clinical data type IDs.
    """

    db = Database()
    db.load_odbc(odbc_profile=odbc_profile)

    qu = "QUERY" 
    dtypes = db.query(qu)["data_type_id"].to_list()

    return dtypes


@functools.lru_cache()
def get_map_version(run_id: int, odbc_profile: str = "DATABASE") -> int:
    """Get's the map_version from run_metadata for a given run_id.

    Args:
        run_id: Clinical run_id that exists in run_metadata.
        odbc_profile: Indicates dev or prod DB.

    Raises:
        RuntimeError: If more than one map version is found for a given run_id.

    Returns:
        Map version tagged to the run_id.
    """

    clinical_data_type_ids = get_clin_dtypes(odbc_profile=odbc_profile)

    pw = PipelineWrappers(odbc_profile=odbc_profile)
    map_version = pw.pull_run_metadata(
        run_id=run_id, clinical_data_type_id=clinical_data_type_ids
    )["map_version"].to_list()

    map_version = list(set(map_version))

    if len(map_version) > 1:
        raise RuntimeError(f"run_id={run_id} return more than one distinct map version.")

    return map_version[0]


def downsample_draws(df: pd.DataFrame, draws: int, draw_start=0) -> pd.DataFrame:
    """Given a dataframe with a set of draw columns, return just the quantity of
    draws defined in the input arg.

    Args:
        df: Data which must contain columns using the draw_* pattern.
        draws: The quantity of draws to downsample to.
        draw_start: The initial draw to start with. Defaults to 0, e.g. `draw_0`.

    Raises:
        KeyError if the quantity of draws outnumbers the quantity of draw_* columns in
            the data.
        KeyError if the manually created and naturally ordered draw columns are not present
            in the dataframe.

    Returns:
        Data with the subset of the first `draws` draw columns. All other columns
            are unchanged.
    """
    draw_columns = df.columns.tolist()
    ordered_draws = [f"draw_{i}" for i in range(draw_start, draws + draw_start)]

    draw_cols = [draw for draw in draw_columns if "draw_" in draw]
    if len(draw_cols) < draws:
        raise KeyError(
            f"We expected to find {draws} draw columns but only found {len(draw_cols)}."
        )

    missing_cols = set(ordered_draws) - set(draw_cols)
    if missing_cols:
        raise KeyError(
            f"Selecting the first {draws} columns in natural "
            f"order has failed. {missing_cols}"
        )
    drop_cols = list(set(draw_cols) - set(ordered_draws))

    df = df.drop(drop_cols, axis=1)

    # Re-check the df columns to make sure the counts of draws match the expected count.
    post_downsample_draws = len([draw for draw in df.columns.tolist() if "draw_" in draw])
    if post_downsample_draws != draws:
        raise KeyError(
            f"There are only {post_downsample_draws} 'draw_*' columns after "
            f"downsampling. We expect there to be {draws} columns."
        )
    return df


def enforce_cf_schema(cf_df: pd.DataFrame) -> pd.DataFrame:
    """Given a dataframe containing aggregated CMS data in count space return the same data
    with a new estimate_type column to meet CF modeling pipeline requirements. Estimate type
    is derived from the estimate_id column using a lookup dictionary"""

    if cf_df["estimate_id"].unique().size > 1:
        raise RuntimeError("This can only process data with a single unique estimate_id")

    cf_df["estimate_type"] = legacy_cf_estimate_types[cf_df["estimate_id"].iloc[0]]

    return cf_df
