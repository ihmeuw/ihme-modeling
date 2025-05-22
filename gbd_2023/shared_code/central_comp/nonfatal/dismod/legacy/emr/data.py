import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

import db_tools_core
from elmo import get_crosswalk_version

from cascade_ode.legacy.crosswalk import (
    crosswalk_version_id_from_model_version_id,
    row_table_name_from_model_version_id,
)
from cascade_ode.legacy.demographics import DemographicContext, Demographics
from cascade_ode.legacy.emr import envs, templating
from cascade_ode.legacy.io import datastore
from cascade_ode.legacy.shared_functions import get_envelope, get_population

#############################################################################
# MATH FUNCTIONS
#############################################################################


def se_from_ui(p, lower, upper, method, confidence=0.95):
    """Calculates standard error from the uncertainty interval"""

    assert method in ["ratio", "non-ratio"], "must specify whether to" "use the ratio method"

    quantile = 1 - (1 - confidence) / 2
    if method == "ratio":
        n = np.log(upper) - np.log(lower)
        d = 2 * stats.norm.ppf(quantile)
        se = (np.exp(n / d) - 1) * p
    elif method == "non-ratio":
        se = np.max([upper - p, p - lower]) / stats.norm.ppf(quantile)
    return se


def lower_from_se(p, se, confidence=0.95):
    """Calculates lower bound of the UI based on standard error"""
    quantile = (1 - confidence) / 2
    lower = p + stats.norm.ppf(quantile) * se
    lower = np.max([0, lower])  # truncate all data at 0
    return lower


def upper_from_se(p, se, param_type, confidence=0.95):
    """Calculates upper bound of the UI based on standard error"""
    quantile = 1 - (1 - confidence) / 2
    upper = p + stats.norm.ppf(quantile) * se
    if param_type == "proportion":
        upper = np.min([1, upper])
    return upper


def aggregate_mean_se(mean, se):
    """compute aggregated value of standard error"""
    return pd.Series({"mean": np.mean(mean), "se": np.mean(se)})


#############################################################################
# CUSTOM EXCEPTIONS
#############################################################################


class NoNonZeroValues(Exception):
    pass


#############################################################################
# TEMPLATING FUNCTION
#############################################################################


def adj_data_template(df, model_version_id):
    # year value
    df = templating.df_mean(df, "year_id", ["year_start", "year_end"])
    df = templating.df_round(df, "year_id", "year_id", base=5)

    # get age mapping
    ages = Demographics(model_version_id=model_version_id).age_group_ids
    ages_query = """
    SELECT
        age_group_id, age_group_years_start, age_group_years_end
    FROM
        shared.age_group
    WHERE
        age_group_id in ({})
    """.format(
        ", ".join([str(a) for a in ages])
    )
    with db_tools_core.session_scope(envs.Environment.get_odbc_key()) as sesh:
        age_map = db_tools_core.query_2_df(ages_query, sesh)
    span_map = templating.SpanMap(
        age_map, "age_group_id", "age_group_years_start", "age_group_years_end"
    )

    # configure age target
    span_target = templating.SpanTarget(df, "age_start", "age_end")

    # expand for ages
    df = templating.span_2_id(span_map, span_target)
    return df


#############################################################################
# DATA CLASSES
#############################################################################


class Data(object):

    _data_key = "input_data_key"

    def __init__(self, *args, **kwargs):
        self.df = self.get_data(*args, **kwargs)

    def get_data(self):
        raise NotImplementedError

    @staticmethod
    def calc_se_from_ui(df, mean_col, lower_col, upper_col, method="non-ratio"):

        # compute standard error
        df["se"] = df.apply(
            lambda x: se_from_ui(x[mean_col], x[lower_col], x[upper_col], method=method),
            axis=1,
        )
        df = df.drop([lower_col, upper_col], axis=1)
        df = df[(df[mean_col] > 0) & (df["se"] != 0)]
        return df

    @staticmethod
    def calc_aggregate_se(df, data_key_col, mean_col, se_col):

        grouped = df.groupby([data_key_col])
        df = grouped.apply(lambda x: aggregate_mean_se(x[mean_col], x[se_col])).reset_index()
        df = df[(df["mean"] > 0) & (df[se_col] != 0)]
        return df

    @staticmethod
    def drop_zeros_nulls(df, mean_col, lower_col, upper_col):
        df = df.loc[df[mean_col] > 0]  # get rid of 0 means
        df = df.loc[
            (df[mean_col].notnull()) & (df[lower_col].notnull()) & (df[upper_col].notnull())
        ]  # mean upper and lower not null
        return df


class DataMetadata(Data):
    def get_data(self, model_version_id):
        columns_to_return = [
            self._data_key,
            "location_id",
            "sex_id",
            "year_start",
            "year_end",
            "age_start",
            "age_end",
            "measure_id",
            "nid",
            "underlying_nid",
            "outlier_type_id",
        ]
        rename_columns = {"seq": self._data_key, "is_outlier": "outlier_type_id"}

        crosswalk_version_id = crosswalk_version_id_from_model_version_id(model_version_id)
        df = get_crosswalk_version(crosswalk_version_id)

        # get sex_id from sex, drop both-sex data
        with db_tools_core.session_scope("epi") as sesh:
            df = df.merge(
                db_tools_core.query_2_df("SELECT sex, sex_id FROM shared.sex", session=sesh)
            )
            df = df.loc[df["sex_id"] != 3]

            # get measure_id
            df = df.merge(
                db_tools_core.query_2_df(
                    "SELECT measure, measure_id FROM shared.measure", session=sesh
                )
            )

        # subset out terminal ages and ages with width greater than 20
        df = df.loc[((df["age_end"] - df["age_start"]) < 20) | (df["age_start"] >= 95)]

        df = df.rename(columns=rename_columns)
        if df.empty:
            raise NoNonZeroValues

        # set index
        df[self._data_key] = df[self._data_key].astype(int)
        df = df.set_index(self._data_key)

        # subset, preserve index
        df = df[[col for col in columns_to_return if col != self._data_key]]

        return df


class DataModelDataAdj(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, model_version_id):
        # import data
        root_dir = 
        submodel_dir = 
        context = DemographicContext(
            parent_location_id=None, location_id=1, year_id=2000, sex_id=3
        )
        store = datastore.Datastore(
            submodel_dir=submodel_dir, cascade_root_dir=root_dir, context=context
        )
        df = store["adj_data_upload"]
        df = df[[self._data_key, "mean", "lower", "upper"]]

        # subset
        df.drop_duplicates(inplace=True)
        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")

        if df.empty:
            raise NoNonZeroValues

        # set index
        df[self._data_key] = df[self._data_key].astype(int)
        df = df.set_index(self._data_key)
        return df


class DataModelDataAdjPrevalence(DataModelDataAdj):

    _measure_id = 5

    def get_data(self, data_model_data_adj, data_metadata):
        df = data_metadata.df.merge(data_model_data_adj.df, left_index=True, right_index=True)
        df = df.loc[df["measure_id"] == self._measure_id, :]

        # exclude prevalence from EMR calc if its < 1/100,000,
        # as small prev -> ludicrous EMR
        df = df[df["mean"] >= 1.0 / 100000]

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = df[["mean", "se"]]
        return df


class DataModelDataAdjIncidence(DataModelDataAdj):

    _measure_id = 6

    def get_data(self, data_model_data_adj, data_metadata):
        df = data_metadata.df.merge(data_model_data_adj.df, left_index=True, right_index=True)
        df = df.loc[df["measure_id"] == self._measure_id, :]

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = df[["mean", "se"]]
        return df


class DataRemission(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, remission_df):
        """calculate remission aggregates from remission_df"""
        # if remission is not missing try and generate values
        if remission_df is not None:
            mean = np.mean(remission_df["mean"])

            # if mean is non zero we can calculate standard error and upper
            if mean != 0:

                #  calculate aggregate standard error
                remission_df = self.calc_se_from_ui(remission_df, "mean", "lower", "upper")
                remission_df["agg_dummy"] = 1
                se = self.calc_aggregate_se(remission_df, "agg_dummy", "mean", "se")[
                    "se"
                ].iat[0]

                # calculate upper
                self.upper = upper_from_se(mean, se, param_type="rate")

                # if upper < 1 it's a long duration so only use prevalence
                self.mean = mean
                self.se = se

            # if mean is 0 then we cannot calculate the aggregate standard
            # error and therefore just assign the 0 case
            else:
                self.mean = 0
                self.se = 0
                self.upper = 0
        # if remission_df is missing then assign the zero case
        else:
            self.mean = 0
            self.se = 0
            self.upper = 0


class DataModelEstimateFitEMR(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, model_version_id, id_template_df):
        root_dir = 
        submodel_dir = 
        context = DemographicContext(
            parent_location_id=None, location_id=1, year_id=2000, sex_id=3
        )
        store = datastore.Datastore(
            submodel_dir=submodel_dir, cascade_root_dir=root_dir, context=context
        )
        df = store["posterior_upload"]
        df = df.loc[df["measure_id"] == 9]
        df = df.drop(["measure_id"], axis=1)
        df = self.drop_zeros_nulls(df, "pred_mean", "pred_lower", "pred_upper")
        df = df.merge(id_template_df, on=["year_id", "age_group_id", "sex_id"])
        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "pred_mean", "pred_lower", "pred_upper")
        df = self.calc_aggregate_se(df, self._data_key, "pred_mean", "se")
        df = df.set_index(self._data_key)
        return df


class DataCoDCSMR(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, model_version_id, id_template_df):

        # pull data
        query = """
        SELECT
            t3mvcsmr.location_id,
            t3mvcsmr.year_id,
            t3mvcsmr.age_group_id,
            t3mvcsmr.sex_id,
            t3mvcsmr.mean,
            t3mvcsmr.upper,
            t3mvcsmr.lower
        FROM
            epi.t3_model_version_csmr t3mvcsmr
        WHERE
            t3mvcsmr.model_version_id = {}
        """.format(
            model_version_id
        )
        with db_tools_core.session_scope(envs.Environment.get_odbc_key()) as session:
            df = db_tools_core.query_2_df(query, session=session)
        df = templating.df_round(df, "year_id", "year_id", base=5)
        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")
        df = df.merge(id_template_df, on=["location_id", "year_id", "age_group_id", "sex_id"])

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = self.calc_aggregate_se(df, self._data_key, "mean", "se")
        df = df.set_index(self._data_key)
        return df


class DataCustomCSMR(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, model_version_id, id_template_df):
        tbl_name = row_table_name_from_model_version_id(model_version_id)
        query = f"""
        SELECT
            cw.location_id,
            cw.sex_id,
            cw.year_start,
            cw.year_end,
            cw.age_start,
            cw.age_end,
            cw.mean,
            cw.upper,
            cw.lower
        FROM
            crosswalk_version.{tbl_name} cw
        WHERE
            cw.measure_id = 15
            """
        with db_tools_core.session_scope(envs.Environment.get_odbc_key()) as session:
            df = db_tools_core.query_2_df(query, session=session)
        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")
        df = adj_data_template(df, model_version_id)
        df = df.merge(id_template_df, on=["location_id", "year_id", "age_group_id", "sex_id"])

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = self.calc_aggregate_se(df, self._data_key, "mean", "se")
        df = df.set_index(self._data_key)
        return df


class DataACMR(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, id_template_df, release_id):
        locs = id_template_df.location_id.unique().tolist()
        ages = id_template_df.age_group_id.unique().tolist()
        sexes = id_template_df.sex_id.unique().tolist()
        years = id_template_df.year_id.unique().tolist()

        # Get deaths and pop.
        env_df = get_envelope(
            age_group_id=ages,
            location_id=locs,
            year_id=years,
            sex_id=sexes,
            with_hiv=1,
            with_shock=0,
            release_id=release_id,
        )
        pop_df = get_population(
            age_group_id=ages,
            location_id=locs,
            year_id=years,
            sex_id=sexes,
            release_id=release_id,
        )
        log = logging.getLogger(__name__)
        log.info(f"ACMR envelope run_id: {env_df['run_id'].iat[0]}")
        log.info(f"ACMR population run_id: {pop_df['run_id'].iat[0]}")
        df = env_df.merge(pop_df, on=["location_id", "year_id", "age_group_id", "sex_id"])

        # convert to rates
        for col in ["mean", "lower", "upper"]:
            df[col] = df[col] / df["population"]

        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")

        # add input_data_key
        df = df.merge(id_template_df, on=["location_id", "year_id", "age_group_id", "sex_id"])

        if df.empty:
            raise NoNonZeroValues

        # aggregate
        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = self.calc_aggregate_se(df, self._data_key, "mean", "se")
        df = df.set_index(self._data_key)
        return df
