"""
Functions to query the covariates information for CODEm models.
"""

import numpy as np
import logging

from db_queries import get_covariate_estimates
from gbd.decomp_step import decomp_step_from_decomp_step_id

import codem.data.queryStrings as QS
from codem.reference import db_connect
from codem.data.shared import get_location_info, age_sex_data

logger = logging.getLogger(__name__)


def transform(data, trans):
    """
    (array, string) -> array

    Given an array of numeric data and a string indicating the type of
    transformation desired will return an array with the desired transformation
    applied. If the string supplied is invalid the same array will be returned.
    """
    if trans == "ln":
        return np.log(data)
    elif trans == "lt":
        return np.log(data / (1. - data))
    elif trans == "sq":
        return data**2
    elif trans == "sqrt":
        return data**.5
    elif trans == "scale1000":
        return data * 1000.
    else:
        return data


def transform_covariates(df, var, trans):
    """
    (Pandas data frame, string, string) -> Pandas data frame

    Given a pandas data frame, a string that represents a valid numeric
    variable in that column and a string representing a type of transformation,
    will return a Pandas data frame with the variable transform as specified.
    Additionally the name of the variable will be changed to note the
    transformation.
    """
    df2 = df
    df2[var] = transform(df2[var].values, trans)
    if trans in ["ln", "lt", "sq", "sqrt", "scale1000"]:
        df2 = df2.rename(columns={var: (trans + "_" + var)})
    return df2


def lag_covariates(df, var, lag):
    """
    (Pandas data frame, string, string) -> Pandas data frame

    Given a pandas data frame, a string that represents a valid numeric
    variable in that column and an integer representing the number of years to
    lag, will return a Pandas data frame with the specified lag applied.
    Additionally, the name of the variable will be changed to note the
    transformation.
    """
    if lag is None:
        return df
    if np.isnan(lag):
        return df
    df2 = df
    df2["year"] = df2["year"] + lag
    df2 = df2.rename(columns={var: ("lag" + str(lag) + "_" + var)})
    return df2


def get_covariate_metadata(model_version_id, db_connection):
    """
    integer -> Pandas data frame

    Given an integer that represents a valid model ID number, will
    return a pandas data frame which contains the covariate model ID's
    for that model as well as the metadata needed for covariate selection.

    Note: this was re-worked to call the covariate prior information from the cod database (either dev or prod)
    and then covariate names from ADDRESS because the model versions must be merged with many tables
    only available in the covariate database.
    """
    df = db_connect.query(QS.metaQueryStr.format(mvid=model_version_id), db_connection)
    models = df.covariate_model_id.values.tolist()
    names = db_connect.query(QS.covNameQueryStr.format(', '.join((str(x) for x in models))),
                             'ADDRESS')
    df = df.merge(names, on='covariate_model_id')
    return df


def get_covariates(covariate_id, covariate_model_id, location_set_version_id,
                   gbd_round_id, decomp_step_id, db_connection, standard_location_set_version_id):
    """
    integer -> Pandas data frame

    Given an integer which represents a valid covariate ID will return a data
    frame which contains a unique value for each country, year, age group.
    This data may be aggregated in some form as well.
    """
    logger.info('Getting covariate estimates for covariate_id {} and '
                'covariate_model_id {} and decomp step {}.'.
                format(covariate_id, covariate_model_id, decomp_step_id))
    loc_df = get_location_info(
        location_set_version_id=location_set_version_id,
        standard_location_set_version_id=standard_location_set_version_id,
        db_connection=db_connection
    )
    loc_list = loc_df.location_id.values.tolist()
    df = get_covariate_estimates(
        covariate_id=covariate_id,
        model_version_id=covariate_model_id,
        location_set_version_id=location_set_version_id,
        decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
        gbd_round_id=gbd_round_id
    )
    df = df.loc[df.location_id.isin(loc_list)]
    df = df[['covariate_name_short', 'age_group_id',
             'sex_id', 'year_id', 'location_id', 'mean_value']]
    df.rename(columns={'age_group_id': 'age',
                       'sex_id': 'sex',
                       'year_id': 'year',
                       'covariate_name_short': 'name'}, inplace=True)
    df = df[['mean_value', 'age', 'sex', 'year', 'location_id', 'name']]
    df.rename(columns={'mean_value': df['name'].values[0]}, inplace=True)
    return df


def get_single_covariate_dataframe(covariate_id, covariate_model_id,
                                   trans, lag, offset, sex, location_set_version_id,
                                   gbd_round_id, decomp_step_id, db_connection,
                                   standard_location_set_version_id):
    """
    (integer, string, integer, integer) -> Pandas data frame

    Given a covariate id number, a string representing a transformation
    type, an integer representing lags of the variable and an integer
    representing which sex to restrict the data to, will return a
    data frame which contains teh values for that covariate transformed
    as specified.
    """
    df = get_covariates(
        covariate_id=covariate_id,
        covariate_model_id=covariate_model_id,
        location_set_version_id=location_set_version_id,
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id,
        db_connection=db_connection,
        standard_location_set_version_id=standard_location_set_version_id
    )
    if (offset is not None) and (~np.isnan(offset)):
        df[df.columns.values[0]] = df[df.columns.values[0]] + offset
    df = transform_covariates(df, df.columns.values[0], trans)
    df = lag_covariates(df, df.columns.values[0], lag)
    df = age_sex_data(df, sex, db_connection)
    df = df.drop("name", 1)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df[df.year >= 1980]
    return df


def get_all_covariates(model_version_id, sex, decomp_step_id, gbd_round_id,
                       location_set_version_id, standard_location_set_version_id, db_connection):
    """
    integer -> (Pandas data frame, Pandas data frame)

    Given an integer which represents a valid model version ID, returns
    two Pandas data frames. The first is a data frame which contains the
    covariate data for that model. The second is the meta data of those
    same covariates which will be used for the model selection process.
    """
    covs = get_covariate_metadata(model_version_id, db_connection)
    df = get_single_covariate_dataframe(
        covariate_id=covs.covariate_id[0],
        covariate_model_id=covs.covariate_model_id[0],
        trans=covs.transform_type_short[0],
        lag=covs.lag[0],
        offset=covs.offset[0],
        sex=sex,
        location_set_version_id=location_set_version_id,
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id,
        db_connection=db_connection,
        standard_location_set_version_id=standard_location_set_version_id
    )
    for i in range(1, len(covs)):
        df_temp = get_single_covariate_dataframe(
            covariate_id=covs.covariate_id[i],
            covariate_model_id=covs.covariate_model_id[i],
            trans=covs.transform_type_short[i],
            lag=covs.lag[i],
            offset=covs.offset[i],
            sex=sex,
            location_set_version_id=location_set_version_id,
            gbd_round_id=gbd_round_id,
            decomp_step_id=decomp_step_id,
            db_connection=db_connection,
            standard_location_set_version_id=standard_location_set_version_id
        )
        df = df.merge(df_temp, how="outer",
                      on=["location_id", "age", "sex", "year"])
    n = df.drop(["location_id", "age", "sex", "year"], axis=1).columns.values
    covs["name"] = n
    return df, covs
