from typing import List, Optional

import numpy as np
import pandas as pd

import db_tools_core

import codem.data.queryStrings as QS
from codem.reference import db_connect


def write_submodel(
    model_version_id: int,
    submodel_type_id: int,
    submodel_dep_id: int,
    weight: float,
    rank: int,
    conn_def: str,
) -> int:
    """Write a submodel to the table and get the id back."""
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        result = session.execute(
            QS.submodel_query_str,
            params={
                "model_version_id": model_version_id,
                "submodel_type_id": submodel_type_id,
                "submodel_dep_id": submodel_dep_id,
                "weight": weight,
                "rank": rank,
            },
        )
        submodel_id = result.lastrowid
        return submodel_id


def write_submodel_covariate(
    submodel_id: int, list_of_covariate_ids: List[int], conn_def: str
) -> None:
    """Writes to cod.submodel_version_covariate."""
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        for covariate_model_version_id in list_of_covariate_ids:
            session.execute(
                QS.submodel_cov_write_str,
                params={
                    "submodel_version_id": submodel_id,
                    "covariate_model_version_id": covariate_model_version_id,
                },
            )


def write_model_pv(tag: str, value: int, model_version_id: int, conn_def: str) -> None:
    """Writes to cod.submodel_version a model PV value."""
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        session.execute(
            QS.pv_write.format(tag=tag),
            params={"value": value, "model_version_id": model_version_id},
        )


def write_model_output(
    df_true: pd.DataFrame,
    model_version_id: int,
    sex_id: int,
    conn_def: str,
    submodel_version_id: Optional[int] = None,
    model_output_path: Optional[str] = None,
) -> None:
    """Writes summaries for a passed model_version or submodel_version."""
    df = df_true.copy()
    df["sex_id"] = sex_id
    columns = [col for col in df if "draw_" in col]
    df[columns] = df[columns].values / df["envelope"].values[..., np.newaxis]
    df["mean_cf"] = df[columns].mean(axis=1)
    df["lower_cf"] = df[columns].quantile(0.025, axis=1)
    df["upper_cf"] = df[columns].quantile(0.975, axis=1)
    df = df[
        [
            "year_id",
            "location_id",
            "sex_id",
            "age_group_id",
            "mean_cf",
            "lower_cf",
            "upper_cf",
            "envelope",
        ]
    ]
    df["model_version_id"] = model_version_id
    if submodel_version_id:
        df["submodel_version_id"] = submodel_version_id
        table = "submodel"
    else:
        table = "model"
    if model_output_path:
        df.to_csv(model_output_path.format(table=table), index=False)
    db_connect.write_df_to_sql(df.drop(["envelope"], axis=1), table=table, conn_def=conn_def)


def truncate_draws(mat, percent=95):
    """
    :param mat:     array where rows correspond to observations and columns draw
    :param percent: a value between 0 and 100 corresponding to the amount of
                    data to keep
    :return:        array where row data outside row percentile has been
                    replaced with the mean.
    """
    if percent <= 0 or percent >= 100:
        raise ValueError(f"percent {percent} is out of range")
    low_bound = (100.0 - float(percent)) / 2.0
    hi_bound = 100.0 - low_bound
    matrix = np.copy(mat)
    row_lower_bound = np.percentile(matrix, low_bound, axis=1)
    row_upper_bound = np.percentile(matrix, hi_bound, axis=1)
    replacements = (matrix.T < row_lower_bound).T | (matrix.T > row_upper_bound).T
    replacements[matrix.std(axis=1) < 10**-5, :] = False
    masked_matrix = np.ma.masked_array(matrix, replacements)
    row_mean_masked = np.mean(masked_matrix, axis=1)
    row_replacements = np.where(replacements)[0]
    matrix[replacements] = row_mean_masked[row_replacements]
    return matrix
