import pandas as pd
import numpy as np
from scipy import stats
from db_tools import ezfuncs
from db_queries import get_envelope, get_population
from emr_calc import envs
import templating

#############################################################################
# MATH FUNCTIONS
#############################################################################


def se_from_ui(p, lower, upper, method, confidence=0.95):
    """ Calculates standard error from the uncertainty interval """

    assert method in ['ratio', 'non-ratio'], "must specify whether to" \
        "use the ratio method"

    quantile = 1 - (1 - confidence) / 2
    if method == 'ratio':
        n = np.log(upper) - np.log(lower)
        d = 2 * stats.norm.ppf(quantile)
        se = (np.exp(n / d) - 1) * p
    elif method == 'non-ratio':
        se = np.max([upper - p, p - lower]) / stats.norm.ppf(quantile)
    return se


def lower_from_se(p, se, confidence=0.95):
    """ Calculates lower bound of the UI based on standard error """
    quantile = (1 - confidence) / 2
    lower = p + stats.norm.ppf(quantile) * se
    lower = np.max([0, lower])  # truncate all data at 0
    return lower


def upper_from_se(p, se, param_type, confidence=0.95):
    """ Calculates upper bound of the UI based on standard error """
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


def adj_data_template(df):
    # year value
    df = templating.df_mean(df, "year_id", ["year_start", "year_end"])
    df = templating.df_round(df, "year_id", "year_id", base=5)

    # get age mapping
    ages_query = """
    SELECT
        age_group_id, age_group_years_start, age_group_years_end
    FROM
        shared.age_group
    WHERE
        (age_group_id BETWEEN 2 AND 20)
        OR (age_group_id BETWEEN 30 AND 32)
        OR (age_group_id = 235)
    """
    age_map = ezfuncs.query(
        ages_query, conn_def=envs.Environment.get_odbc_key())
    span_map = templating.SpanMap(
        age_map, "age_group_id", "age_group_years_start",
        "age_group_years_end")

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
    def calc_se_from_ui(df, mean_col, lower_col, upper_col,
                        method="non-ratio"):

        # compute standard error
        df["se"] = df.apply(
            lambda x:
                se_from_ui(x[mean_col], x[lower_col], x[upper_col],
                           method=method),
            axis=1)
        df = df.drop([lower_col, upper_col], axis=1)
        df = df[(df[mean_col] > 0) & (df["se"] != 0)]
        return df

    @staticmethod
    def calc_aggregate_se(df, data_key_col, mean_col, se_col):

        grouped = df.groupby([data_key_col])
        df = grouped.apply(
            lambda x: aggregate_mean_se(x[mean_col], x[se_col])
        ).reset_index()
        df = df[(df["mean"] > 0) & (df[se_col] != 0)]
        return df

    @staticmethod
    def drop_zeros_nulls(df, mean_col, lower_col, upper_col):
        df = df.ix[df[mean_col] > 0]  # get rid of 0 means
        df = df.ix[
            (df[mean_col].notnull()) &
            (df[lower_col].notnull()) &
            (df[upper_col].notnull())]  # mean upper and lower not null
        return df


class DataMetadata(Data):

    def get_data(self, model_version_id):
        demo_query = """
        SELECT
            t3mvd.model_version_dismod_id as {data_key},
            t3mvd.location_id,
            t3mvd.sex_id,
            t3mvd.year_start,
            t3mvd.year_end,
            t3mvd.age_start,
            t3mvd.age_end,
            t3mvd.measure_id,
            t3mvd.nid,
            t3mvd.underlying_nid,
            t3mvd.outlier_type_id
        FROM
            epi.t3_model_version_dismod t3mvd
        WHERE
            t3mvd.model_version_id = {model_version_id}
        """.format(data_key=self._data_key, model_version_id=model_version_id)
        df = ezfuncs.query(
            demo_query, conn_def=envs.Environment.get_odbc_key())

        # subset out demographics
        df = df.ix[df["sex_id"] != 3]  # get rid of both sex
        df = df.ix[
            ((df["age_end"] - df["age_start"]) < 20) |  # > 20 age group
            (df["age_start"] >= 95)]  # or terminal

        if df.empty:
            raise NoNonZeroValues

        # set index
        df[self._data_key] = df[self._data_key].astype(int)
        df = df.set_index(self._data_key)
        return df


class DataModelDataAdj(Data):

    mean_col = "mean"
    se_col = "se"
    _path = ("{cascade_root}/{mv}/full/locations/1/outputs/both/2000/"
             "model_data_adj.csv")

    def get_data(self, model_version_id):
        # import data
        path = self._path.format(
            cascade_root=envs.Environment.get_cascade_root(),
            mv=model_version_id)
        df = pd.read_csv(path)
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
        df = data_metadata.df.merge(data_model_data_adj.df, left_index=True,
                                    right_index=True)
        df = df.loc[df["measure_id"] == self._measure_id, :]

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = df[["mean", "se"]]
        return df


class DataModelDataAdjIncidence(DataModelDataAdj):

    _measure_id = 6

    def get_data(self, data_model_data_adj, data_metadata):
        df = data_metadata.df.merge(data_model_data_adj.df, left_index=True,
                                    right_index=True)
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
                remission_df = self.calc_se_from_ui(
                    remission_df, "mean", "lower", "upper")
                remission_df["agg_dummy"] = 1
                se = self.calc_aggregate_se(
                    remission_df, "agg_dummy", "mean", "se")["se"].item()

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

    _path = ("{cascade_root}/{mv}/full/locations/1/outputs/both/2000/"
             "model_estimate_fit.csv")

    def get_data(self, model_version_id, id_template_df):
        path = self._path.format(
            cascade_root=envs.Environment.get_cascade_root(),
            mv=model_version_id)
        df = pd.read_csv(path)
        df = df.ix[df["measure_id"] == 9]
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
        """.format(model_version_id)
        df = ezfuncs.query(query, conn_def=envs.Environment.get_odbc_key())
        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")
        df = df.merge(id_template_df,
                      on=["location_id", "year_id", "age_group_id", "sex_id"])

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
        query = """
        SELECT
            t3mvd.location_id,
            t3mvd.sex_id,
            t3mvd.year_start,
            t3mvd.year_end,
            t3mvd.age_start,
            t3mvd.age_end,
            t3mvd.mean,
            t3mvd.upper,
            t3mvd.lower
        FROM
            epi.t3_model_version_dismod t3mvd
        WHERE
            t3mvd.model_version_id = {}
            AND t3mvd.measure_id = 15
        """.format(model_version_id)
        df = ezfuncs.query(query, conn_def=envs.Environment.get_odbc_key())
        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")
        df = adj_data_template(df)
        df = df.merge(id_template_df,
                      on=["location_id", "year_id", "age_group_id", "sex_id"])

        if df.empty:
            raise NoNonZeroValues

        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = self.calc_aggregate_se(df, self._data_key, "mean", "se")
        df = df.set_index(self._data_key)
        return df


class DataACMR(Data):

    mean_col = "mean"
    se_col = "se"

    def get_data(self, id_template_df):
        locs = id_template_df.location_id.tolist()
        ages = id_template_df.age_group_id.tolist()
        sexes = id_template_df.sex_id.tolist()
        years = id_template_df.year_id.tolist()

        # get deaths and pop
        env_df = get_envelope(
            age_group_id=ages, location_id=locs, year_id=years, sex_id=sexes,
            with_hiv=1, with_shock=0)
        pop_df = get_population(
            age_group_id=ages, location_id=locs, year_id=years, sex_id=sexes)
        df = env_df.merge(
            pop_df, on=["location_id", "year_id", "age_group_id", "sex_id"])

        # convert to rates
        for col in ["mean", "lower", "upper"]:
            df[col] = df[col] / df["population"]

        df = self.drop_zeros_nulls(df, "mean", "lower", "upper")

        # add input_data_key
        df = df.merge(id_template_df,
                      on=["location_id", "year_id", "age_group_id", "sex_id"])

        if df.empty:
            raise NoNonZeroValues

        # aggregate
        df = self.calc_se_from_ui(df, "mean", "lower", "upper")
        df = self.calc_aggregate_se(df, self._data_key, "mean", "se")
        df = df.set_index(self._data_key)
        return df
