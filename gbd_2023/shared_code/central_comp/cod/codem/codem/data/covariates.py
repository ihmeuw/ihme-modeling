"""Functions to query the covariates information for CODEm models."""

import logging
from typing import List

import numpy as np
import pandas as pd

from db_queries import get_covariate_estimates
from gbd import conn_defs

import codem.data.queryStrings as QS
from codem.data.shared import age_sex_data, get_location_info
from codem.reference import db_connect

logger = logging.getLogger(__name__)


def transform(data: np.array, trans: str) -> np.array:
    """
    Given an array of numeric data and a string indicating the type of
    transformation desired will return an array with the desired transformation
    applied. If the string supplied is invalid the same array will be returned.
    """
    if trans == "ln":
        if not np.all(data > 0):
            raise RuntimeError("Not all values are > 0, natural log is undefined.")
        return np.log(data)
    elif trans == "lt":
        if not (np.all(data > 0) and np.all(data < 1)):
            raise RuntimeError("Not all values are > 0 and < 1, logit function is undefined.")
        return np.log(data / (1.0 - data))
    elif trans == "sq":
        return data**2
    elif trans == "sqrt":
        return data**0.5
    elif trans == "scale1000":
        return data * 1000.0
    else:
        return data


def transform_covariates(df: pd.DataFrame, var: str, trans: str) -> pd.DataFrame:
    """
    Given a pandas data frame, a string that represents a valid numeric
    variable in that column and a string representing a type of transformation,
    will return a Pandas data frame with the variable transform as specified.
    Additionally the name of the variable will be changed to note the
    transformation.
    """
    if trans not in ["ln", "lt", "sq", "sqrt", "scale1000"]:
        return df
    logger.info(f"Applying {trans} transform to {var}")
    df2 = df
    df2[var] = transform(df2[var].values, trans)
    return df2.rename(columns={var: (trans + "_" + var)})


def lag_covariates(df: pd.DataFrame, var: str, lag: int, start_year: int) -> pd.DataFrame:
    """
    Given a pandas data frame, a string that represents a valid numeric
    variable in that column and an integer representing the number of years to
    lag, will return a Pandas data frame with the specified lag applied.
    Additionally, the name of the variable will be changed to note the
    transformation.
    """
    if lag is None or np.isnan(lag):
        return df
    lag = int(lag)
    logger.info(f"Lagging {var} by {lag} years")
    minimum_year = df["year_id"].min()
    if minimum_year > (start_year - lag):
        raise RuntimeError(
            f"Need estimates back to {start_year - lag} to lag by {lag} years, "
            f"earliest year in data is {minimum_year}."
        )
    df2 = df
    df2["year_id"] = df2["year_id"] + lag
    return df2.rename(columns={var: ("lag" + str(lag) + "_" + var)})


def get_covariate_metadata(model_version_id: int, conn_def: str) -> pd.DataFrame:
    """
    Given an integer that represents a valid model ID number, will
    return a pandas data frame which contains the covariate model ID's
    for that model as well as the metadata needed for covariate selection.
    """
    df = db_connect.execute_select(
        QS.metaQueryStr, parameters={"model_version_id": model_version_id}, conn_def=conn_def
    )
    if df.empty:
        raise RuntimeError(
            f"No covariate priors found for model version ID {model_version_id}"
        )
    models = df.covariate_model_id.values.tolist()
    names = db_connect.execute_select(
        QS.covNameQueryStr,
        parameters={"model_version_id": models},
        conn_def=conn_defs.COVARIATES,
    )
    df = df.merge(names, on="covariate_model_id")
    return df


def get_covariates(
    covariate_id: int, covariate_model_id: int, location_id: List[int], release_id: int
) -> pd.DataFrame:
    """
    Given an integer which represents a valid covariate ID will return a data
    frame which contains a unique value for each country, year, age group.
    This data may be aggregated in some form as well.
    """
    logger.info(
        f"Getting covariate estimates for covariate_id {covariate_id} and "
        f"covariate_model_id {covariate_model_id} and GBD release ID {release_id}."
    )
    df = get_covariate_estimates(
        covariate_id=int(covariate_id),
        model_version_id=covariate_model_id,
        location_id=location_id,
        release_id=release_id,
    )
    df = df[
        [
            "covariate_name_short",
            "age_group_id",
            "sex_id",
            "year_id",
            "location_id",
            "mean_value",
        ]
    ]
    df.rename(columns={"covariate_name_short": "name"}, inplace=True)
    df = df[["mean_value", "age_group_id", "sex_id", "year_id", "location_id", "name"]]
    df.rename(columns={"mean_value": df["name"].values[0]}, inplace=True)
    return df


def get_single_covariate_dataframe(
    covariate_id: int,
    covariate_model_id: int,
    start_year: int,
    trans: str,
    lag: int,
    offset: int,
    sex_id: int,
    location_id: List[int],
    release_id: int,
) -> pd.DataFrame:
    """
    Given a covariate id number, a string representing a transformation
    type, an integer representing lags of the variable and an integer
    representing which sex to restrict the data to, will return a
    data frame which contains teh values for that covariate transformed
    as specified.
    """
    df = get_covariates(
        covariate_id=covariate_id,
        covariate_model_id=covariate_model_id,
        location_id=location_id,
        release_id=release_id,
    )
    if (offset is not None) and (~np.isnan(offset)):
        covariate_name_short = df.columns.values[0]
        logger.info(f"Adding offset of {offset} to {covariate_name_short}")
        df[covariate_name_short] = df[covariate_name_short] + offset
    df = transform_covariates(df, df.columns.values[0], trans=trans)
    df = lag_covariates(df, df.columns.values[0], lag=lag, start_year=start_year)
    df = age_sex_data(df, sex_id, release_id=release_id)
    df = df.drop("name", axis=1)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df[df.year_id >= start_year]
    return df


def get_all_covariates(
    model_version_id: int,
    sex_id: int,
    start_year: int,
    release_id: int,
    location_set_version_id: int,
    standard_location_set_version_id: int,
    conn_def: str,
) -> (pd.DataFrame, pd.DataFrame):
    """
    Given an integer which represents a valid model version ID, returns
    two Pandas data frames. The first is a data frame which contains the
    covariate data for that model. The second is the meta data of those
    same covariates which will be used for the model selection process.
    """
    covs = get_covariate_metadata(model_version_id, conn_def)
    location_id = get_location_info(
        location_set_version_id=location_set_version_id,
        standard_location_set_version_id=standard_location_set_version_id,
        conn_def=conn_def,
    )["location_id"].tolist()
    df = get_single_covariate_dataframe(
        covariate_id=covs.covariate_id[0],
        covariate_model_id=covs.covariate_model_id[0],
        start_year=start_year,
        trans=covs.transform_type_short[0],
        lag=covs.lag[0],
        offset=covs.offset[0],
        sex_id=sex_id,
        location_id=location_id,
        release_id=release_id,
    )
    for i in range(1, len(covs)):
        df_temp = get_single_covariate_dataframe(
            covariate_id=covs.covariate_id[i],
            covariate_model_id=covs.covariate_model_id[i],
            start_year=start_year,
            trans=covs.transform_type_short[i],
            lag=covs.lag[i],
            offset=covs.offset[i],
            sex_id=sex_id,
            location_id=location_id,
            release_id=release_id,
        )
        df = df.merge(
            df_temp, how="outer", on=["location_id", "age_group_id", "sex_id", "year_id"]
        )
    n = df.drop(["location_id", "age_group_id", "sex_id", "year_id"], axis=1).columns.values
    covs["name"] = n
    return df, covs
