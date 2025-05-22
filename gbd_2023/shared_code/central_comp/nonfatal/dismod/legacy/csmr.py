"""
To help (un)confuse, a breakdown of how this works:

Some key points:
- We can have four cases, the product of: (CSMR, LNASDR) x (imputed, codcorrect)
- The process type of the output version determines whether we use
    - imputed (cod DB).
    or
    - codcorrect ("get_outputs" or read_csmr_from_get_outputs).
- The output_version_id is set by EpiViz in epi.model_version_mr as csmr_cod_output_version_id.
"""

import json
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

import db_tools_core
from gbd.constants import age

from cascade_ode.legacy import db, demographics, shared_functions
from cascade_ode.legacy.constants import ConnectionDefinitions, CSMRQueries

CSMR_INFO_FNAME = "csmr_info.json"
PARENT_CSMR_METADATA_TYPE_ID = 3  # from cod.output_metadata_type (CSMR for LNASDR)
CODCORRECT = "codcorrect"  # source: codcorrect outputs
IMPUTED = "imputed"  # source: sdi-imputed codcorrect outputs
COD_OUTPUT_PROCESS_ID_MAP = {1: CODCORRECT, 2: IMPUTED}  # from cod.output_process
CSMR_COLS = [
    "location_id",
    "time_lower",
    "age_group_id",
    "x_sex",
    "age_lower",
    "age_upper",
    "meas_value",
    "meas_lower",
    "meas_upper",
]


def find_csmr_for_model(
    model_version_meta: pd.DataFrame, this_model_root_dir: str
) -> pd.DataFrame:
    """
    This function returns csmr for the cause specified in the given model's
    parameters.

    It records information about the csmr version used in the specified root
    directory.
    """
    cod_output_version_id = model_version_meta.csmr_cod_output_version_id.squeeze()
    cause_id = model_version_meta.add_csmr_cause.squeeze()
    release_id = model_version_meta.release_id.squeeze()
    log = logging.getLogger(__name__)

    log.info(f"Using cod_output_version_id: {cod_output_version_id}, cause_id {cause_id}")
    source, _ = determine_source(cod_output_version_id)
    log.info(f"source of this version is: {source}")

    csmr_info = CSMRData(source=source, version_id=cod_output_version_id)

    record_csmr_version(csmr_info.source, csmr_info.version_id, Path(this_model_root_dir))

    if source == CODCORRECT:
        df = read_csmr_from_get_outputs(
            cod_output_version_id=cod_output_version_id,
            cause_id=cause_id,
            model_version_id=model_version_meta.model_version_id.squeeze(),
        )
    else:  # imputed results
        df = _read_imputed_csmr_from_cod_db(
            cod_output_version_id=cod_output_version_id,
            cause_id=cause_id,
            release_id=release_id,
        )

    return df


def determine_source(cod_output_version_id: int, is_lnasdr: bool = False) -> Tuple[str, int]:
    """Gets cod_output_version's source (imputed or codcorrect) and version_id.

    Answers the question: for this cod_output_version_id, where in the cod database do I
    expect to find results? There are 3 different types of results we retrieve related to the
    cod db: imputed CSMR, imputed LNASDR, and codcorrect outputs. This method provides the
    context required to distinguish between those three results.

    Returned version_id differs from provided cod_output_version_id iff is_lnasdr is True.
    This means we're looking to retrieve the cod output_version_id of the imputed LNASDR
    results (which are separate from CSMR).

    Arguments:
        cod_output_version_id: The cod_output_version_id to determine source for.
        is_lnasdr: If True, return the cod_output_version_id of the imputed LNASDR results
            instead of the provided cod_output_version_id.

    Returns:
        source: Either "codcorrect" or "imputed".
        version_id: The cod_output_version_id of the codcorrect or imputed results.
    """
    with db_tools_core.session_scope(ConnectionDefinitions.COD) as session:
        output_process_id = db_tools_core.query_2_df(
            "SELECT output_process_id FROM cod.output_version WHERE output_version_id = :vid",
            session=session,
            parameters={"vid": cod_output_version_id},
        )["output_process_id"].get(0)
        if output_process_id is None:
            raise RuntimeError(f"cod output_version_id {cod_output_version_id} DNE")
        source = COD_OUTPUT_PROCESS_ID_MAP[output_process_id]

        lnasdr_output_version_id = None
        # if given cod_output_version_id is imputed, LNASDR is under a different output_version
        if is_lnasdr and source == IMPUTED:
            lnasdr_query_result = db_tools_core.query_2_df(
                CSMRQueries.LNASDR_VERSION_FROM_CSMR_OUTPUT_VERSION,
                session=session,
                parameters={
                    "csmr_output_type_id": PARENT_CSMR_METADATA_TYPE_ID,
                    "cod_output_version_id": cod_output_version_id,
                },
            )["output_version_id"].tolist()
            if not lnasdr_query_result:
                raise RuntimeError(
                    f"cod output_version_id {cod_output_version_id} does not have an associated "
                    f"imputed LNASDR output_version_id."
                )
            elif len(lnasdr_query_result) > 1:
                raise RuntimeError(
                    f"cod_output_version_id {cod_output_version_id} has multiple "
                    f"LNASDR outputs. This is an invalid database state, please file a ticket "
                    f"with central computation"
                )

            lnasdr_output_version_id = lnasdr_query_result[0]

    return source, lnasdr_output_version_id or cod_output_version_id


def record_csmr_version(source, version_id, directory):
    csmr_info = {"source": source, "version": int(version_id)}
    with open(, "w") as f:
        json.dump(csmr_info, f)


class CSMRData:
    def __init__(self, source: str, version_id: int) -> None:
        self.source = source
        self.version_id = version_id

    @staticmethod
    def from_json(json_path: str):
        with open(json_path, "r") as f:
            data = json.load(f)
        return CSMRData(source=data["source"], version_id=data["version"])


def read_csmr_from_get_outputs(
    cod_output_version_id: int, cause_id: int, model_version_id: int, lnasdr: bool = False
) -> pd.DataFrame:
    """
    We read imputed CSMR from flat files, but for codcorrect CSMR that doesn't
    need imputation, we can just read from the database.

    This function reads death counts for a given cause and codcorrect and then
    uses the current best population envelope for the given output version to convert
    to rates.

    Args:
        cod_output_version_id: the cod_output_version of codcorrect to retrieve results for.
            NOTE: distinct from codcorrect_version_id.
        cause_id: the cause to get CSMR for.
        model_version_id: used to determine demographics.
        lnasdr: if True, return results for the age-standardized
            age_group_id. Used to calculate LNASDR.

    Returns:
        a dataframe with the following columns:
            ['location_id', 'time_lower', 'age_group_id', 'x_sex',
            'age_lower', 'age_upper', 'meas_value', 'meas_lower',
            'meas_upper']
    """
    codcorrect_version_id = _get_codcorrect_version_from_cod_output_version(
        cod_output_version_id=cod_output_version_id
    )
    process_version_df = db.execute_select(
        query=CSMRQueries.PROCESS_VERSION_OF_CODCORRECT,
        conn_def="gbd",
        params={"codcorrect_version": codcorrect_version_id},
    )
    try:
        process_version_id = int(process_version_df.gbd_process_version_id.iat[0])
    except IndexError:
        err = f"codcorrect_version_id {codcorrect_version_id} was not found in gbd database"
        raise RuntimeError(err)

    logging.info(f"CodCorrect process version id is {process_version_id}")

    demo = demographics.Demographics(model_version_id=model_version_id)
    if lnasdr:
        age_group_id = age.AGE_STANDARDIZED
    else:
        age_group_id = demo.mortality_age_grid
    location_ids = shared_functions.get_location_metadata(
        location_set_id=demo.location_set_id, release_id=demo.release_id
    )["location_id"].tolist()

    df = shared_functions.get_outputs(
        process_version_id=process_version_id,
        cause_id=cause_id,
        location_ids=location_ids,
        age_group_ids=age_group_id,
        year_ids=demo.mortality_years,
        sex_ids=demo.sex_ids,
    )
    df = _clean_codcorrect_results(df=df, release_id=demo.release_id)
    if df.empty:
        raise ValueError(
            f"There was a problem with the selected CodCorrect version "
            f"({codcorrect_version_id}) + csmr_cause_id {cause_id}"
        )
    return df


def _read_imputed_csmr_from_cod_db(
    cod_output_version_id: int, cause_id: int, release_id: int
) -> pd.DataFrame:
    """Reads imputed CSMR results from cod database."""
    df = db.execute_select(
        CSMRQueries.GET_IMPUTED_CSMR,
        params={"vid": cod_output_version_id, "cause_id": cause_id},
        conn_def=ConnectionDefinitions.COD,
    )
    return _clean_codcorrect_results(df, release_id)


def _clean_codcorrect_results(df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    nulls = df.val.isna()
    percent_null = nulls.sum() / len(df)
    logging.info(
        f"Found {percent_null} fraction of CSMR null. Dropping {nulls.sum()} " "rows"
    )
    df = df[~nulls]

    df = df.rename(
        columns={
            "sex_id": "x_sex",
            "year_id": "time_lower",
            "val": "meas_value",
            "upper": "meas_upper",
            "lower": "meas_lower",
        }
    )
    age_spans = shared_functions.get_age_spans(release_id=release_id).rename(
        columns={"age_group_years_start": "age_lower", "age_group_years_end": "age_upper"}
    )
    df = df.merge(age_spans, how="left")

    df = df[CSMR_COLS]
    return df


def _get_codcorrect_version_from_cod_output_version(cod_output_version_id: int) -> int:
    """Gets codcorrect_version_id from cod_output_version_id.

    codcorrect_version_id is used to retrieve a process_version_id that's passed to
    get_outputs.

    Arguments:
        cod_output_version_id: The cod_output_version_id to get codcorrect_version_id for.

    Returns:
        codcorrect_version_id
    """
    with db_tools_core.session_scope(ConnectionDefinitions.COD) as session:
        codcorrect_version_id_list = db_tools_core.query_2_df(
            CSMRQueries.CODCORRECT_VERSION_FROM_OUTPUT_VERSION,
            session=session,
            parameters={"cod_output_version_id": cod_output_version_id},
        )["val"].tolist()

    if not codcorrect_version_id_list:
        raise RuntimeError(
            f"codcorrect_version_id does not exist for cod_output_version_id "
            f"{cod_output_version_id}."
        )
    elif len(codcorrect_version_id_list) > 1:
        raise RuntimeError(
            f"cod_output_version_id {cod_output_version_id} has multiple "
            f"codcorrect_version_ids. This is an invalid database state, please file a "
            f"ticket with central computation"
        )

    return codcorrect_version_id_list[0]
