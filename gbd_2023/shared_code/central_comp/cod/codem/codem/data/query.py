"""
Functions to query the causes of death database and format and process
all of the CODEm input data. Needs helper functions from the demographics, shared,
and covariates modules.
"""

import logging
import re
from typing import Final

import numpy as np
import pandas as pd
import pymysql

import db_tools_core
from db_queries import get_cod_data
from db_queries.api.internal import get_location_hierarchy_by_version

from codem.data.covariates import get_all_covariates
from codem.data.demographics import get_mortality_data
from codem.data.shared import exclude_regions, get_location_info
from codem.reference import db_connect

logger = logging.getLogger(__name__)


_COD_DATA_REQUIRED_COLUMNS: Final[str] = [
    "location_id",
    "year_id",
    "age_group_id",
    "sex_id",
    "is_representative",
    "data_type_name",
    "refresh_id",
    "sample_size",
    "cf_raw",
    "cf",
    "variance_rd_logit_cf",
    "variance_rd_log_dr",
]


def save_model_outliers(
    model_version_id: int, release_id: int, refresh_id: int, conn_def: str
) -> None:
    """
    Execute the save outliers stored procedure in the specified database.

    Arguments:
        model_version_id
        release_id
        refresh_id
        conn_def
    """
    engine = db_tools_core.get_engine(conn_def=conn_def)
    connect = engine.raw_connection()
    cursor = connect.cursor()
    try:
        cursor.callproc(
            "cod.save_model_outliers",
            [float(model_version_id), float(release_id), float(refresh_id)],
        )
    except (pymysql.err.InternalError, pymysql.err.OperationalError) as e:
        code, msg = e.args
        if re.search("No outlier found for this model version id", str(msg)):
            logger.info(
                "There are no outliers for the model version ID {}".format(model_version_id)
            )
        else:
            if re.search("already exists in outlier_model_version table", str(msg)):
                logger.info(
                    "Model version ID {} already exists in the outlier_model_version "
                    "table.".format(model_version_id)
                )
            else:
                raise e
    finally:
        cursor.close()
        connect.commit()


def exists_in_outlier_model_version(model_version_id: int, conn_def: str) -> int:
    """
    Check to see if this model version already exists in the outlier model version table.
    :param model_version_id: (int)
    :param conn_def: (str)
    """
    logger.info(
        f"Checking to make sure that {model_version_id} does not exist "
        f"in the outlier model version table."
    )
    call = """
        SELECT COUNT(*) AS count
        FROM cod.outlier_model_version
        WHERE model_version_id = :model_version_id
        """
    count = db_connect.execute_select(
        call, conn_def=conn_def, parameters={"model_version_id": model_version_id}
    )["count"][0]
    if count:
        logger.info(
            f"The model version {model_version_id} already exists "
            f"in the outlier model version table."
        )
    return count


def prep_cod_data(
    cause_id: int,
    start_year: int,
    location_set_version_id: int,
    refresh_id: int,
    conn_def: str,
    model_version_id: int,
    release_id: int,
) -> pd.DataFrame:
    """
    Given a list of model parameters, save model outliers if passed model_version_id does not
    exist in cod.outlier_model_version table. Then, use get_cod_data with passed parameters
    and return a pandas DataFrame.
    """
    logger.info(f"Querying cod data for outliers in refresh {refresh_id}")
    if not exists_in_outlier_model_version(
        model_version_id=model_version_id, conn_def=conn_def
    ):
        logger.info(f"Running the outlier stored procedure for refresh_id {refresh_id}")
        save_model_outliers(
            model_version_id=model_version_id,
            release_id=release_id,
            refresh_id=refresh_id,
            conn_def=conn_def,
        )
    else:
        logger.warning(
            "The outlier model version already exists in the table, "
            "therefore we aren't copying it over."
        )
    logger.info(f"Querying cod data for refresh {refresh_id}.")
    with db_tools_core.session_scope(conn_def) as session:
        location_df = get_location_hierarchy_by_version(
            location_set_version_id=location_set_version_id, session=session
        )
        df = get_cod_data(
            cause_id=cause_id,
            location_id=location_df.query("most_detailed == 1")["location_id"].tolist(),
            model_version_id=model_version_id,
            release_id=release_id,
            is_outlier=False,
            include_rd_variance=True,
            include_data_type_name=True,
            include_citation=False,
            include_site_name=False,
            include_rate=False,
            include_deaths=False,
            include_cod_source_label=False,
            cod_session=session,
        )[_COD_DATA_REQUIRED_COLUMNS].rename(
            columns={
                "data_type_name": "source_type",
                "variance_rd_logit_cf": "variance_rd_lt_cf",
                "variance_rd_log_dr": "variance_rd_ln_rate",
            }
        )
    df["is_representative"] = df["is_representative"].astype(int)
    return df.loc[df["year_id"] >= start_year]


def data_variance(df: pd.DataFrame, response: str) -> pd.DataFrame:
    """
    Given a data frame and a response type generates an estimate of the variance
    for that response based on sample size. A new column is added to the returned
    dataframe where  each observation has been sampled 100 times from a normal
    distribution to find the estimate.
    """

    def sd_from_cf_ss(cf, sample_size, var_method):
        if var_method == "binomial":
            cf_sd = np.sqrt(cf * (1 - cf) / sample_size)
        elif var_method == "wilson_score_interval":
            cf_sd = np.sqrt(
                ((cf * (1 - cf) / sample_size) + ((1.96**2) / (4 * sample_size**2)))
            ) / (1 + 1.96**2 / sample_size)
        else:
            raise NotImplementedError(f"{var_method} not yet implemented.")
        return cf_sd

    def calc_sampling_sd(cf, sample_size, env, pop, variance_rd, var_method, response):
        # set any cause fractions outside of 0.0-1.0 to nan, at this point we
        # shouldn't have any, as they're nan'd out before in get_codem_data
        # CCOMP-4578 - switched lower bound from .00000001 to 0
        if not ((cf > 0.00000001) and (cf < 1.0)):
            return np.NaN
        cf_sd = sd_from_cf_ss(cf, sample_size, var_method)
        # cap SD at .5
        cf_sd = 0.5 if cf_sd > 0.5 else cf_sd
        n_samples = 100
        if response == "lt_cf":
            draws = np.random.normal(cf, cf_sd, n_samples)
        elif response == "ln_rate":
            draws = np.random.normal(cf, cf_sd, n_samples) * (env / pop)
        # set any negative draws to nan
        draws[draws <= 0] = np.NaN
        if response == "lt_cf":
            draws = np.log(draws / (1 - draws))
        elif response == "ln_rate":
            draws = np.log(draws)
        draws_masked = draws[~np.isnan(draws)]
        ss_var = draws_masked.var(axis=0)
        sd_final = np.sqrt(ss_var + variance_rd)
        # set any sd of 0 to nan
        sd_final = np.NaN if sd_final == 0.0 else sd_final
        return sd_final

    logger.info(f"Running data sampling variance for response {response}")
    np.seterr(invalid="ignore")
    # if final deaths is less than or equal to 5, use wilson's score internval,
    # otherwise use binomial distribution
    df_nodata = df.loc[df["death_final"].isnull()]
    df_wilson = df.loc[df["death_final"] <= 5]
    df_binomial = df.loc[df["death_final"] > 5]
    if not df_wilson.empty:
        df_wilson[f"{response}_sd"] = df_wilson.apply(
            lambda x: calc_sampling_sd(
                x.loc["cf"],
                x.loc["sample_size"],
                x.loc["envelope"],
                x.loc["population"],
                x.loc[f"variance_rd_{response}"],
                "wilson_score_interval",
                response,
            ),
            axis=1,
        )
    if not df_binomial.empty:
        df_binomial[f"{response}_sd"] = df_binomial.apply(
            lambda x: calc_sampling_sd(
                x.loc["cf"],
                x.loc["sample_size"],
                x.loc["envelope"],
                x.loc["population"],
                x.loc[f"variance_rd_{response}"],
                "binomial",
                response,
            ),
            axis=1,
        )
    np.seterr(invalid="warn")
    variance_df = pd.concat([df_nodata, df_wilson, df_binomial], sort=True)
    return variance_df.reset_index(drop=True)


def data_process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas data frame -> Pandas data frame

    Given a pandas data frame that was queried for CODEm returns a
    Pandas data frame that has columns added for mixed effect analysis.
    """
    df2 = df.copy()
    df2 = df2.replace([np.inf, -np.inf], np.nan)
    df2["level_2_nest"] = df2.level_1.map(str) + ":" + df2.level_2.map(str)
    df2["age_nest"] = df2.level_2_nest + ":" + df2.age_group_id.map(str)
    df2["level_3_nest"] = df2.level_2_nest + ":" + df2.level_3.map(str)
    df2["level_4_nest"] = df2.level_3_nest + ":" + df2.level_4.map(str)
    # add on column of cf  * envelope  = deaths to determine which method to use
    df2["death_final"] = df2["cf"] * df2["envelope"]
    df2 = data_variance(df2, "ln_rate")
    df2 = data_variance(df2, "lt_cf")
    df2 = df2.drop(columns=["cf_raw", "death_final"])
    return df2


def get_codem_data(
    cause_id: int,
    sex_id: int,
    start_year: int,
    start_age: int,
    end_age: int,
    regions_exclude: str,
    location_set_version_id: int,
    refresh_id: int,
    conn_def: str,
    model_version_id: int,
    release_id: int,
    env_run_id: int,
    with_hiv: int,
    pop_run_id: int,
    standard_location_set_version_id: int,
) -> pd.DataFrame:
    """
    :param cause_id: int
        cause_id to pull results from
    :param sex_id: int, 1 or 2
        sex_id to query
    :param start_year: int
        year_id of first data point
    :param start_age: int
        age_group_id of first data point
    :param end_age: int
        age_group_id of last data point
    :param regions_exclude: str
        str of regions to exclude
    :param location_set_version_id: int
        cod location version to use
    :param refresh_id: int
        refresh ID to use to pull cod.cv_data
    :param conn_def: str
        db connection def
    :param model_version_id: int
        model version of the CODEm model
    :param release_id: int
        GBD release ID
    :param pop_run_id: int
        run ID for get_population
    :param env_run_id: int
        run ID for get_envelope
    :param standard_location_set_version_id: int
        standard location set version ID to use
    :return: data frame
        data frame with all model data
    """
    logger.info("Beginning full CoD query.")
    cod = prep_cod_data(
        cause_id=cause_id,
        start_year=start_year,
        location_set_version_id=location_set_version_id,
        refresh_id=refresh_id,
        conn_def=conn_def,
        model_version_id=model_version_id,
        release_id=release_id,
    )
    mort = get_mortality_data(
        sex_id=sex_id,
        start_year=start_year,
        start_age=start_age,
        end_age=end_age,
        location_set_version_id=location_set_version_id,
        conn_def=conn_def,
        release_id=release_id,
        pop_run_id=pop_run_id,
        env_run_id=env_run_id,
        with_hiv=with_hiv,
        standard_location_set_version_id=standard_location_set_version_id,
    )
    loc = get_location_info(
        location_set_version_id,
        standard_location_set_version_id=standard_location_set_version_id,
        conn_def=conn_def,
    )
    loc = exclude_regions(loc, regions_exclude=regions_exclude)
    mort_df = mort.merge(loc, how="right", on=["location_id"])
    cod_df = cod.merge(
        mort_df, how="right", on=["location_id", "age_group_id", "sex_id", "year_id"]
    )
    cod_df.loc[cod_df["cf"] >= 1, "cf"] = np.NAN
    cod_df.loc[cod_df["cf"] <= 0, "cf"] = np.NAN
    cod_df["ln_rate"] = np.log(cod_df["cf"] * cod_df["envelope"] / cod_df["population"])
    cod_df["lt_cf"] = np.log(cod_df["cf"] / (1.0 - cod_df["cf"]))
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
        sex_id=model_parameters["sex_id"],
        start_year=model_parameters["start_year"],
        start_age=model_parameters["age_start"],
        end_age=model_parameters["age_end"],
        location_set_version_id=model_parameters["location_set_version_id"],
        regions_exclude=model_parameters["locations_exclude"],
        refresh_id=model_parameters["refresh_id"],
        conn_def=model_parameters["conn_def"],
        model_version_id=model_parameters["model_version_id"],
        release_id=model_parameters["release_id"],
        env_run_id=model_parameters["env_run_id"],
        with_hiv=model_parameters["with_hiv"],
        pop_run_id=model_parameters["pop_run_id"],
        standard_location_set_version_id=model_parameters["standard_location_set_version_id"],
    )
    cov_df, priors = get_all_covariates(
        model_version_id=model_parameters["model_version_id"],
        sex_id=model_parameters["sex_id"],
        start_year=model_parameters["start_year"],
        release_id=model_parameters["release_id"],
        location_set_version_id=model_parameters["location_set_version_id"],
        conn_def=model_parameters["conn_def"],
        standard_location_set_version_id=model_parameters["standard_location_set_version_id"],
    )
    df2 = df.merge(
        cov_df, how="left", on=["location_id", "age_group_id", "sex_id", "year_id"]
    )
    covs = df2[priors.name.values]
    df = df.drop_duplicates()
    covs = covs.loc[df.index]
    df.reset_index(drop=True, inplace=True)
    covs.reset_index(drop=True, inplace=True)
    columns = df.columns.values[df.dtypes.values == np.dtype("float64")]
    df[columns] = df[columns].astype("float32")
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

    adjust_df.drop(
        np.setdiff1d(adjust_df.index.values, covariates.index.values), inplace=True
    )

    # remove observations where population or envelope is zero
    zeroes = adjust_df[(adjust_df["envelope"] <= 0) | (adjust_df["population"] <= 0)]
    if not zeroes.empty:
        raise RuntimeError("You have negative or 0 envelope/pops!")
    adjust_df = adjust_df[(adjust_df["envelope"] > 0) & (adjust_df["population"] > 0)]
    covariates.drop(
        np.setdiff1d(covariates.index.values, adjust_df.index.values), inplace=True
    )

    # change cf values outside of zero and one in the main data frame to np.NaN
    # CCOMP-4578 - switched lower bound from .00000001 to 0
    adjust_df["cf"] = adjust_df["cf"].map(
        lambda x: np.NaN if x <= 0.00000001 or x >= 1.0 else x
    )
    # if any of calcuated SD, ln_rate, lt_cf is nan, set cf also to nan
    adjust_df["cf"][
        (adjust_df["lt_cf_sd"].isnull()) | (adjust_df["ln_rate_sd"].isnull())
    ] = np.NaN
    adjust_df["ln_rate"][(adjust_df["cf"].isnull())] = np.NaN
    adjust_df["lt_cf"][(adjust_df["cf"].isnull())] = np.NaN

    covariates.reset_index(drop=True, inplace=True)
    adjust_df.reset_index(drop=True, inplace=True)

    return adjust_df, covariates, pd.concat([adjust_df, covariates], axis=1)
