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

import logging
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import text

import db_tools_core

from cascade_ode.legacy import constants, csmr

LNASDR_COLS = [
    "location_id",
    "sex_id",
    "year_id",
    "model_version_id",
    "asdr_cause",
    "csmr_cod_output_version_id",
    "raw_c_lnasdr_{cause_id}.0",
]


def get_lnasdr_from_db(
    cause_id: int, cod_output_version_id: int, model_version_id: int
) -> pd.DataFrame:
    """Get log-normalized age-standardized death rate from get_outputs.

    Args:
        cause_id: Cause to pull.
        cod_output_version_id: CoD output version ID to pull.
        model_version_id: Dismod model_version_id used to impute demographics.

    Returns:
        LNASDR estimates for given cause, codcorrect_version_id, and model_version_id.
    """
    log = logging.getLogger(__name__)
    log.info(f"Pulling LNASDR from outputs database. Output version: {cod_output_version_id}")
    source, cod_output_version_id = csmr.determine_source(
        cod_output_version_id=cod_output_version_id, is_lnasdr=True
    )
    log.info(f"Source {source}, using output version ID {cod_output_version_id}")
    if source == csmr.CODCORRECT:
        df = csmr.read_csmr_from_get_outputs(
            cod_output_version_id=cod_output_version_id,
            cause_id=cause_id,
            model_version_id=model_version_id,
            lnasdr=True,
        )
        # log-normalize. source:
        df["meas_value"] = np.log(df["meas_value"])
    else:
        df = _get_imputed_lnasdr_from_cod_db(
            cod_output_version_id=cod_output_version_id, cause_id=cause_id
        )
        df["model_version_id"] = model_version_id

    output_cols = _get_lnasdr_cols(cause_id=cause_id)
    value_col = output_cols[-1]
    df["csmr_cod_output_version_id"] = cod_output_version_id
    df["asdr_cause"] = cause_id
    df["model_version_id"] = np.NaN
    df = df.rename(
        columns={
            "x_sex": "sex_id",
            "time_lower": "year_id",
            "meas_value": value_col,
            "mean": value_col,
        }
    )
    return df[output_cols]


def _get_lnasdr_cols(cause_id: int) -> List[str]:
    """Gets expected output columns in expected order for lnasdr."""
    cols = LNASDR_COLS.copy()
    cols[-1] = cols[-1].format(cause_id=str(cause_id))
    return cols


def _get_imputed_lnasdr_from_cod_db(
    cod_output_version_id: int, cause_id: int
) -> pd.DataFrame:
    """Gets imputed LNASDR estimates from cod db."""
    with db_tools_core.session_scope(constants.ConnectionDefinitions.COD) as session:
        df = pd.read_sql(
            text(constants.CSMRQueries.GET_IMPUTED_LNASDR),
            session.connection(),
            params={"vid": cod_output_version_id, "cause_id": cause_id},
        )
    return df
