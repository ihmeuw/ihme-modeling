import datetime
import inspect
from getpass import getuser
from typing import List, Optional, Set, Tuple, Union

import pandas as pd
import yaml
from crosscutting_functions.clinical_constants import values as constants
from crosscutting_functions.clinical_metadata_utils.api import pipeline_wrappers
from crosscutting_functions.db_connector import database

from inpatient.Clinical_Runs.master import make_dir
from inpatient.Clinical_Runs.utils.claims_run_tracker import BestClaimsRuns


def map_diagnosis_id(df: pd.DataFrame) -> pd.DataFrame:
    """Assigns diagnosis_id based on the specified estimate.

    Args:
        df (pd.DataFrame): Requires column 'estimate_id'.

    Raises:
        KeyError: estimate_id not in the input dataframe.
        ValueError: If any diagnosis_id is 0.

    Returns:
        pd.DataFrame: Input df with added or corrected diagnosis_id column.
    """

    if "estimate_id" not in df.columns:
        raise KeyError("'estimate_id' column required.")

    dx_by_est = get_valid_estimates(full_details=True)[["diagnosis_id", "estimate_id"]]
    dx_ids = {
        v: dx_by_est.loc[i, "diagnosis_id"] for i, v in enumerate(dx_by_est["estimate_id"])
    }
    df["diagnosis_id"] = df["estimate_id"].map(dx_ids)

    if not (df["diagnosis_id"] > 0).all():
        raise ValueError("diagnosis_id was not set properly")

    return df


def assign_facility_source_type_from_est(df: pd.DataFrame) -> pd.DataFrame:
    """Assigns source_type_id based on the specified estimate.

    Args:
        df (pd.DataFrame): Requires column 'estimate_id'.

    Raises:
        KeyError: estimate_id not in the input dataframe.
        ValueError: Invalid estimates are present in df.

    Returns:
        pd.DataFrame: Input df with added or corrected source_type_id column.
    """

    if "estimate_id" not in df.columns:
        raise KeyError("'estimate_id' column required.")

    valid_e = get_valid_estimates(full_details=True)
    valid_source_e = [
        e for e in df["estimate_id"].unique() if e in valid_e["estimate_id"].to_list()
    ]
    if len(set(df["estimate_id"]) - set(valid_source_e)) > 0:
        raise ValueError("Unexpected estimate found in the source DataFrame")

    if any(valid_e.loc[valid_e["estimate_id"].isin(valid_source_e), "facility_type_id"] >= 2):
        source_type_id = 17
    else:
        source_type_id = 10

    df["source_type_id"] = source_type_id

    return df


def get_valid_estimates(
    clinical_data_type_ids: Optional[Union[Tuple[int], List[int]]] = None,
    full_details: bool = False,
) -> Union[List[int], pd.DataFrame]:
    """Gets valid Clinical estimates from the clinical DB.

    Args:
        clinical_data_type_ids (Optional[Union[Tuple[int], List[int]]]):
            Collection of Clinical datatypes to filter estimates table to.
            Defaults to None which pulls all estimates.
        full_details (bool): Return all columns of estimate table.
            Defaults to False which only returns valid unique estimates.

    Returns:
        Union[List[int], pd.DataFrame]: Valid estimates or filtered estimate table.
    """
    db = database.Database()
    db.load_odbc(odbc_profile="clinical")

    est_qu = QUERY

    if clinical_data_type_ids:
        query_dtypes = tuple([i for i in clinical_data_type_ids])
        filter_dtype = QUERY
        est_qu = est_qu + filter_dtype

    current_est = db.query(est_qu)
    # Do not return columns such as 'date_inserted'
    cols = [
        col for col in current_est.columns if col.endswith("_id") or col.startswith("estimate")
    ]
    cols.append("is_dedup")

    if not full_details:
        return list(current_est["estimate_id"].unique())
    else:
        return current_est[cols]


def get_next_run_id() -> int:
    """Queries the Clinical DB to get the next run_id to initialize
    into run_metadata.

    Raises:
        RuntimeError: If the run_id that ought to be the next run
            currently exists. Due to this run_id having no
            release_id.

    Returns:
        int: ID value for the next run for run_metadata.
    """

    # Get current run_id information
    db = database.Database()
    db.load_odbc(odbc_profile="clinical")
    run_ids = QUERY
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


def get_clin_dtypes(df: pd.DataFrame, output: str = "ids") -> Set[int]:
    """Gets the clinical_data_type_id(s) for the estimate_id(s) found in
    a table.

    Args:
        df (pd.DataFrame): Table with a numeric 'estiamte_id' column.
        output (str): "ids" to only return the clinical_data_type_id(s)
            "map" to return a mapping for the estimates to datatype.

    Returns:
        Union[Set[int], Dict[int, int]: Unique clinical_data_type_id(s)
            or a mapping for estimates to
    """

    estimates_present = df["estimate_id"].unique()
    estimates = get_valid_estimates(full_details=True)
    if output == "ids":
        return set(
            [
                estimates.loc[estimates["estimate_id"] == est, "clinical_data_type_id"].item()
                for est in estimates_present
            ]
        )
    elif output == "map":
        return (
            estimates[["estimate_id", "clinical_data_type_id"]]
            .set_index("estimate_id")
            .to_dict()["clinical_data_type_id"]
        )
    else:
        raise ValueError(f"output value '{output}' not valid.")


def get_runs_metadata(run_ids: Union[Tuple[int], List[int]], clin_dtype: int) -> pd.DataFrame:
    """Gets a subset of the clinical.run_metadata table for a
    given combination of run_id(s) and a clinical_data_type_id.

    Args:
        run_ids (Union[Tuple[int], List[int]]): run_id to pull metadata for.
        clin_dtype (int): The specific rows of a run to pull.

    Returns:
        pd.DataFrame: subet clinical.run_metadata
    """

    db = database.Database()
    db.load_odbc(odbc_profile="clinical")

    if len(run_ids) == 1:
        qu = f"""QUERY
        """

    else:
        run_ids = tuple(run_ids)

        qu = f"""QUERY
        """
    run_metadata = db.query(qu)

    return run_metadata


def collect_db_values(metadata: pd.DataFrame, params: List[str]) -> dict:
    """For a subset of clinical.run_metadata, store a dictionary of values.
    Each column name outside of run_id will be stored as a key.
    If all values in the column are equal, it will be kept and stored in
    the output dictionary.  If any of the values in the column disagree, the value
    for the column in the dictionary will be None.

    Args:
        metadata (pd.DataFrame): clinical.run_metadata or subset of.
        params (List[str]): Limit the params to pass to initialize_run_metadata
            to only these columns.

    Returns:
        dict: Metadata values decribing the whole subset or null.
    """

    cols2parse = [param for param in list(metadata.columns) if param in params]

    update_values = {}

    for col in cols2parse:
        if metadata[col].nunique() == 1:
            update_values[col] = metadata[col][0]
        else:
            update_values[col] = None

    return update_values


def add_run_metadata(
    df: pd.DataFrame,
    next_run_id: int,
    clinical_data_universe_id: int,
    clinical_data_type_id: int,
) -> None:
    """Adds a new entry into run_metadata for a compiled run. This will copy any values
    from the run_id(s) which make up the compilation that are all equal. If any
    run_metadata values of the component runs are different from one another, the
    new value will be null. The only exception to this is clinical_data_universe_id which
    must be specified.

    Args:
        df (pd.DataFrame): Compiled dataframe after validations.
        next_run_id (int): ID value for the next run for run_metadata.
        clinical_data_universe_id (int): ID to clinical.clinical_data_universe DB table.
        clinical_datatype_id (int): Clinical datatype id for the instantiated run_id.
    """

    cw = pipeline_wrappers.ClaimsWrappers(
        run_id=next_run_id,
        odbc_profile="clinical",
        clinical_data_type_id=clinical_data_type_id,
    )

    run_ids = tuple(df["run_id"].unique())
    selected_metadata = get_runs_metadata(run_ids=run_ids, clin_dtype=clinical_data_type_id)
    params = list(inspect.signature(cw.initialize_run_metadata).parameters.keys())

    if clinical_data_type_id in [3, 5]:
        for param in ["cf_version_set_id", "clinical_envelope_id"]:
            params.remove(param)

    update_values = collect_db_values(metadata=selected_metadata, params=params)
    update_values["clinical_data_universe_id"] = clinical_data_universe_id

    cw.initialize_run_metadata(**update_values)


def validate_universe(df: pd.DataFrame, clinical_data_universe_id: int) -> None:
    """Makes sure that the input clinical_data_universe_id represents the
    compiled DataFrame.

    Args:
        df (pd.DataFrame): Compiled dataframe after validations.
        clinical_data_universe_id (int): Universe ID to validate table against.

    Raises:
        RuntimeError: If the universe does noes have all NID in df.
    """

    db = database.Database()
    db.load_odbc(odbc_profile="clinical")

    qu = f"""QUERY
    """

    compiled_nid = set(df["nid"].unique())

    nid_universe = db.query(qu)
    universe_nid = set(nid_universe["nid"])

    # All NID in the DataFrame need to be in the universe.
    missing_nid = compiled_nid - universe_nid

    if len(missing_nid) > 0:
        raise RuntimeError(f"These NID are missing from the universe \n{missing_nid}")


def make_compilation_record(
    next_run_id: int, write_sources: List[str], clinical_datatype_id: int, root: str
) -> None:
    """Creates compilation_composition.yaml in the root directory.
    Will only add an update note iff the new table has passed all validations
    and metadata recorded in the DataBase.

    Args:
        next_run_id (int): ID value for the next run for run_metadata.
        write_sources (List[str]): Sources found in the compiled dataframe.
        clinical_datatype_id (int): Clinical datatype id for the instantiated run_id.
        root (str): Output dir to write compilation_composition.yaml to.
    """

    bcr = BestClaimsRuns()

    fmt = "%Y/%m/%d"
    dt_label = datetime.datetime.strftime(datetime.datetime.now(), fmt)
    file_path = FILEPATH

    contents = [{source: run_id} for source, run_id in bcr.items() if source in write_sources]

    msg = "source_runs[claim_source, run_id]"

    data = {
        "compilation_run_id": next_run_id,
        "compiled_by": getuser(),
        "compiled_on": dt_label,
        "contents": msg,
        "source_runs": contents,
        "clinical_datatype_id": clinical_datatype_id,
    }

    with open(file_path, "w") as log_yaml:
        yaml.dump(data, log_yaml)


def write_final_data(
    df: pd.DataFrame,
    run_sources: List[str],
    clinical_data_universe_id: int,
    create_metadata: bool,
    next_run_id: Optional[int] = None,
) -> None:
    """Initalizes a new compilation run and fills the run_metadata table
    with appropriate values. clinical_data_universe_id is too variable to infer an
    appropriate value for and must be specified to ensure all NID are included.
    Writes and initializes the run_metadata table in order of clinical_data_type_id.

    Args:
        df (pd.DataFrame): Compiled dataframe after validations.
        run_sources (List[str]): Sources included in compilation.
        clinical_data_universe_id (int): ID to clinical.clinical_data_universe DB table.
        create_metadata (bool): True will add metadata to the run_metadata table in the DB,
            Will also create run_id directries for newly created run.
        next_run_id (Optional[int]): Optional arg to indicate a specific run_id to write to.
            Defaults to None, which will cause a run_id to be generated. Only applies
            if create_metadata=False.
    """

    dtypes = get_clin_dtypes(df=df, output="ids")
    dtype_map = get_clin_dtypes(df=df, output="map")
    df["clin_dtype"] = df["estimate_id"].map(dtype_map)

    for dtype in sorted(dtypes):
        if not next_run_id:
            next_run_id = get_next_run_id()
            tmp = df.loc[df["clin_dtype"] == dtype]
            tmp = tmp.drop("clin_dtype", axis=1)
            if create_metadata:
                make_dir(next_run_id)
                add_run_metadata(
                    df=tmp,
                    next_run_id=next_run_id,
                    clinical_data_universe_id=clinical_data_universe_id,
                    clinical_data_type_id=dtype,
                )

        out_dir = (FILEPATH
        )
        write_sources = [
            source for source in run_sources if dtype == constants.CLAIMS_DTYPE[source]
        ]
        make_compilation_record(
            next_run_id=next_run_id,
            write_sources=write_sources,
            root=out_dir,
            clinical_datatype_id=dtype,
        )

        # Assign single run_id for refreshing
        tmp["run_id"] = next_run_id

        tmp.to_csv(FILEPATH, index=False, na_rep="NULL")
        print("Saved to {}.".format(FILEPATH))
