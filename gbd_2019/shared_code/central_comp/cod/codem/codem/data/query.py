"""
Functions to query the causes of death database and format and process
all of the CODEm input data. Needs helper functions from the demographics, shared,
and covariates modules.
"""

import pandas as pd
import numpy as np
import sqlalchemy as sql
import logging
import pymysql
import re

import codem.data.queryStrings as QS
import codem.reference.db_connect as db_connect
from codem.data.demographics import get_mortality_data
from codem.data.shared import get_location_info, exclude_regions
from codem.data.covariates import get_all_covariates

logger = logging.getLogger(__name__)


def save_model_outliers(model_version_id, gbd_round_id, decomp_step_id, connection):
    """
    Execute any stored procedure in the specified database with a list of arguments.

    :param model_version_id: int
        model version ID
    :param gbd_round_id: int
        gbd round ID
    :param decomp_step_id: int
        decomposition step ID
    :param connection: str
        database that you wish to execute the stored procedure on
    :return: None
    """
    logger.info('Running outlier stored procedure.')
    creds = db_connect.read_creds()
    db = 'ADDRESS'.format(creds=creds, connection=connection)
    engine = sql.create_engine(db)
    connect = engine.raw_connection()
    cursor = connect.cursor()
    try:
        cursor.callproc('cod.save_model_outliers', [
            float(model_version_id), float(gbd_round_id), float(decomp_step_id)
        ])
    except pymysql.err.InternalError as e:
        code, msg = e.args
        if re.search('No outlier found for this model version id', str(msg)):
            logger.info('There are no outliers for the model version ID {}'.format(model_version_id))
        else:
            if re.search('already exists in outlier_history table', str(msg)):
                logger.info('Model version ID {} already exists in the outlier_history table.'.format(model_version_id))
            else:
                raise e
    finally:
        cursor.close()
        connect.commit()


def copy_model_outliers(old_model_version_id, new_model_version_id, connection):
    """
    Execute any stored procedure in the specified database with a list of arguments.

    :param old_model_version_id: int
        old "from" model version ID
    :param new_model_version_id: int
        new "to" model version ID
    :param connection: str
        database that you wish to execute the stored procedure on
    :return: None
    """
    logger.info('Running outlier stored copy procedure for old model versions..')
    creds = db_connect.read_creds()
    db = 'ADDRESS'.format(creds=creds, connection=connection)
    engine = sql.create_engine(db)
    connect = engine.raw_connection()
    cursor = connect.cursor()
    try:
        cursor.callproc('cod.copy_outliers_by_model_version_id', [
            float(old_model_version_id), float(new_model_version_id)
        ])
    except pymysql.err.InternalError as e:
        logger.info("Hit an error with cod.copy_outliers_by_model_version_id.")
        raise e
    finally:
        cursor.close()
        connect.commit()


def exists_in_outlier_history(model_version_id, connection):
    """
    Check to see if this model version already exists in the outlier history table.
    :param model_version_id: (int)
    :param connection: (str)
    :return:
    """
    logger.info(f"Checking to make sure that {model_version_id} does not exist in the outlier history table.")
    call = f"SELECT COUNT(*) AS count FROM cod.outlier_history WHERE model_version_id = {model_version_id}"
    count = db_connect.query(call, connection=connection)['count'][0]
    if count:
        logger.info(f"The model version {model_version_id} already exists in the outlier history table.")
    return count


def get_cod_data(cause_id, sex, start_year, start_age, end_age,
                 location_set_version_id, refresh_id, outlier_decomp_step_id,
                 db_connection, model_version_id, gbd_round_id, outlier_model_version_id):
    """
    strings indicating model parameters -> Pandas Data Frame

    Given a list of model parameters will query from the COD database and
    return a pandas data frame. The data frame contains the base variables
    used in the CODEm process.

    Also will call the outlier stored procedure in the database to save model outliers if

    """
    logger.info(f"Querying cod data for refresh {refresh_id} and decomp {outlier_decomp_step_id} outliers.")
    if not exists_in_outlier_history(
            model_version_id=model_version_id,
            connection=db_connection):
        if model_version_id in outlier_model_version_id:
            logger.info(f"Running the outlier stored procedure for decomp_step_id {outlier_decomp_step_id}")
            save_model_outliers(
                model_version_id=model_version_id,
                gbd_round_id=gbd_round_id,
                decomp_step_id=outlier_decomp_step_id,
                connection=db_connection
            )
        else:
            for out in outlier_model_version_id:
                logger.info(f"Running the outlier stored procedure to copy outliers from"
                            f"{out} to {model_version_id}")
                copy_model_outliers(
                    old_model_version_id=out,
                    new_model_version_id=model_version_id,
                    connection=db_connection
                )
    else:
        logger.warning("The outlier model version already exists in the table, therefore"
                       "we aren't copying it over.")
        pass
    logger.info(f"Querying cod data for refresh {refresh_id}.")
    call = QS.codQueryStr.format(c=cause_id, s=sex, sy=start_year, sa=start_age,
                                 ea=end_age, loc_set_id=location_set_version_id,
                                 rv=refresh_id,
                                 model_version_id=model_version_id)
    df = db_connect.query(call, db_connection)
    df['national'] = df['national'].map(lambda x: x == 1).astype(int)
    return df


def rbinom(n, p, size):
    """
    Wrapper over np binom function that takes nans

    :param n: int > 0
        number of trials
    :param p: float, 0 < p < 1
        probability of success
    :param size: int > 0
        number of observations
    """
    if np.isnan(p) or np.isnan(n):
        draws = np.repeat(np.nan, size)
    else:
        draws = np.random.binomial(n=n, p=p, size=size)
    return draws


def data_variance(df, response):
    """
    (data frame, string) -> array

    Given a data frame and a response type generates an estimate of the variance
    for that response based on sample size. A single array is returned where
    each observation has been sampled 100 times from a normal distribution to
    find the estimate.
    """
    logger.info("Running data variance for response {}".format(response))
    np.seterr(invalid='ignore')
    cf = df.cf.values
    n = df.sample_size.values
    if response == "lt_cf":
        gc_var = df.gc_var_lt_cf.values
    elif response == "ln_rate":
        gc_var = df.gc_var_ln_rate.values
    else:
        raise RuntimeError("Must specify lt_cf or ln_rate!")
    env = df.envelope.values
    pop = df["pop"].values
    cf[cf <= 0.00000001] = np.NaN
    cf[cf >= 1.] = np.NaN
    cf_sd = (cf * (1-cf) / n)**.5
    cf_sd[cf_sd > .5] = .5  # cap cf_sd
    f = lambda i: np.random.normal(cf[i], cf_sd[i], 100) * (env[i]/pop[i])
    if response == "lt_cf":
        f = lambda i: np.random.normal(cf[i], cf_sd[i], 100)
    draws = np.array(list(map(f, range(len(cf)))))
    draws[draws <= 0] = np.NaN
    if response == "lt_cf":
        draws = np.log(draws / (1 - draws))
    elif response == "ln_rate":
        draws = np.log(draws)
    draws_masked = np.ma.masked_array(draws, np.isnan(draws))
    ss_var = np.array(draws_masked.var(axis=1))
    sd_final = (ss_var + gc_var) ** 0.5
    sd_final[sd_final == 0.] = np.NaN
    np.seterr(invalid='warn')
    return sd_final


def data_process(df):
    """
    Pandas data frame -> Pandas data frame

    Given a pandas data frame that was queried for CODEm returns a
    Pandas data frame that has columns added for mixed effect analysis and
    is re-indexed after removing countries with full sub-national data.
    """
    df2 = df.copy()
    remove = df2[(df.is_estimate == 0) & (df.standard_location == 0)].country_id.unique()
    df2 = df2[df2.location_id.map(lambda x: x not in remove)]
    df2 = df2.replace([np.inf, -np.inf], np.nan)
    df2["region_nest"] = df2.super_region.map(str) + ":" + df2.region.map(str)
    df2["age_nest"] = df2.region_nest + ":" + df2.age.map(str)
    df2["country_nest"] = df2.region_nest + ":" + df2.country_id.map(str)
    df2["sub_nat_nest"] = df2.country_nest + ":" + df2.location_id.map(str)
    df2["ln_rate_sd"] = data_variance(df2, "ln_rate")
    df2["lt_cf_sd"] = data_variance(df2, "lt_cf")
    df2.reset_index(inplace=True, drop=True)
    return df2


def get_codem_data(cause_id, sex, start_year, start_age, end_age, regions_exclude,
                   location_set_version_id, decomp_step_id, refresh_id, gbd_round, db_connection,
                   model_version_id, gbd_round_id,
                   env_run_id, pop_run_id, outlier_model_version_id, outlier_decomp_step_id,
                   standard_location_set_version_id):
    """
    :param cause_id: int
        cause_id to pull results from
    :param sex: int, 1 or 2
        sex_id to query
    :param start_year: int
        year of first data point
    :param start_age: int
        age of first data point
    :param end_age: int
        age of last data point
    :param regions_exclude: str
        str of regions to exclude
    :param location_set_version_id: int
        cod location version to use
    :param decomp_step_id: int
        integer 1-5 that indicates which step of the decomposition analysis (for pulling outliers)
    :param refresh_id: int
        refresh ID to use to pull cod.cv_data
    :param db_connection: str
        db connection string not including .ihme.forecasting.edu
    :param model_version_id: int
        model version of the CODEm model
    :param gbd_round_id: int
        GBD round ID
    :param gbd_round: int
        year round that we are working with
    :param pop_run_id: int
        run ID for get_population
    :param env_run_id: int
        run ID for get_envelope
    :param outlier_model_version_id: int
        which model version to use for outliers
    :param outlier_decomp_step_id: int
        which outliers to pull for those that are pulling active outliers
    :param standard_location_set_version_id: int
        standard location set version ID to use
    :return: data frame
        data frame with all model data
    """
    logger.info("Beginning full CoD query.")
    cod = get_cod_data(
        cause_id=cause_id,
        sex=sex,
        start_year=start_year,
        start_age=start_age,
        end_age=end_age,
        location_set_version_id=location_set_version_id,
        refresh_id=refresh_id,
        outlier_decomp_step_id=outlier_decomp_step_id,
        db_connection=db_connection,
        model_version_id=model_version_id,
        gbd_round_id=gbd_round_id,
        outlier_model_version_id=outlier_model_version_id
    )
    mort = get_mortality_data(
        sex=sex,
        start_year=start_year,
        start_age=start_age,
        end_age=end_age,
        location_set_version_id=location_set_version_id,
        gbd_round_id=gbd_round_id,
        gbd_round=gbd_round,
        decomp_step_id=decomp_step_id,
        db_connection=db_connection,
        pop_run_id=pop_run_id,
        env_run_id=env_run_id,
        standard_location_set_version_id=standard_location_set_version_id
    )
    loc = get_location_info(location_set_version_id,
                            standard_location_set_version_id=standard_location_set_version_id,
                            db_connection=db_connection)
    loc = exclude_regions(loc, regions_exclude=regions_exclude)
    mort_df = mort.merge(loc, how='right', on=['location_id'])
    cod_df = cod.merge(mort_df, how='right',
                       on=['location_id', 'age', 'sex', 'year'])
    cod_df.loc[cod_df["cf"] == 1, "cf"] = np.NAN
    cod_df.loc[cod_df["cf"] == 0, "cf"] = np.NAN
    cod_df['ln_rate'] = np.log(cod_df['cf'] * cod_df['envelope'] / cod_df['pop'])
    cod_df['lt_cf'] = np.log(cod_df['cf'].map(lambda x: x/(1.0-x)))
    df = data_process(cod_df)
    return df


def get_codem_input_data(model_parameters):
    """
    Given an integer which represents a valid model version ID, returns
    two pandas data frames. The first is the input data needed for
    running CODEm models and the second is a data frame of meta data
    needed for covariate selection.

    :param model_parameters: dictionary of model parameters
    """
    df = get_codem_data(
        cause_id=model_parameters["cause_id"],
        sex=model_parameters["sex_id"],
        start_year=model_parameters["start_year"],
        start_age=model_parameters["age_start"],
        end_age=model_parameters["age_end"],
        regions_exclude=model_parameters["locations_exclude"],
        location_set_version_id=model_parameters["location_set_version_id"],
        decomp_step_id=model_parameters["decomp_step_id"],
        refresh_id=model_parameters["refresh_id"],
        db_connection=model_parameters["db_connection"],
        gbd_round=model_parameters["gbd_round"],
        model_version_id=model_parameters["model_version_id"],
        gbd_round_id=model_parameters["gbd_round_id"],
        env_run_id=model_parameters["env_run_id"],
        pop_run_id=model_parameters["pop_run_id"],
        outlier_model_version_id=model_parameters["outlier_model_version_id"],
        outlier_decomp_step_id=model_parameters['outlier_decomp_step_id'],
        standard_location_set_version_id=model_parameters["standard_location_set_version_id"]
    )
    cov_df, priors = get_all_covariates(
        model_version_id=model_parameters["model_version_id"],
        sex=model_parameters["sex_id"],
        decomp_step_id=model_parameters["decomp_step_id"],
        gbd_round_id=model_parameters["gbd_round_id"],
        location_set_version_id=model_parameters["location_set_version_id"],
        db_connection=model_parameters["db_connection"],
        standard_location_set_version_id=model_parameters["standard_location_set_version_id"]
    )
    df = df[
        (df.year >= model_parameters["start_year"]) &
        (df.age >= model_parameters["age_start"]) &
        (df.age <= model_parameters["age_end"])
    ]
    df2 = df.merge(cov_df, how="left", on=["location_id", "age", "sex", "year"])
    covs = df2[priors.name.values]
    df = df.drop_duplicates()
    covs = covs.loc[df.index]
    df.reset_index(drop=True, inplace=True)
    covs.reset_index(drop=True, inplace=True)
    columns = df.columns.values[df.dtypes.values == np.dtype('float64')]
    df[columns] = df[columns].astype('float32')
    return df, covs, priors


def adjust_input_data(df, covs):
    """
    Adjust the input data such that observations with missing covariates,
    or the envelope/population are equal to zero. Also change cf values of
    zero to NaN
    """
    logger.info("Adjusting input data.")

    # remove observations where covariate values are missing
    adjust_df = df.copy()
    covariates = covs.copy()
    if covariates.isnull().values.any():
        raise RuntimeError("You have null covariates!")
    covariates.dropna(inplace=True)

    adjust_df.drop(np.setdiff1d(adjust_df.index.values, covariates.index.values), inplace=True)

    # remove observations where population or envelope is zero
    zeroes = adjust_df[(adjust_df["envelope"] <= 0) | (adjust_df["pop"] <= 0)]
    if not zeroes.empty:
        raise RuntimeError("You have negative or 0 envelope/pops!")
    adjust_df = adjust_df[(adjust_df["envelope"] > 0) & (adjust_df["pop"] > 0)]
    covariates.drop(np.setdiff1d(covariates.index.values, adjust_df.index.values), inplace=True)

    # change cf values of zero and one in the main data frame to np.NaN
    adjust_df["cf"] = adjust_df["cf"].map(lambda x: np.NaN if x <= 0.00000001 or x >= 1 else x)
    adjust_df["cf"][(adjust_df["lt_cf_sd"].isnull()) | (adjust_df["ln_rate_sd"].isnull())] = np.NaN
    adjust_df["ln_rate"][(adjust_df["cf"].isnull())] = np.NaN
    adjust_df["lt_cf"][(adjust_df["cf"].isnull())] = np.NaN

    covariates.reset_index(drop=True, inplace=True)
    adjust_df.reset_index(drop=True, inplace=True)

    return adjust_df, covariates, pd.concat([adjust_df, covariates], axis=1)
