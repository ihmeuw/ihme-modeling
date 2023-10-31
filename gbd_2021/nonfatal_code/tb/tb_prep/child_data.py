import pandas as pd
import numpy as np
from scipy import stats
from db_tools import ezfuncs
from db_queries import get_envelope, get_population, get_model_results, get_demographics, get_age_metadata
import sys
#Custom scripts repository
sys.path.append("FILEPATH")
import envs, templating

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

def span_intersect(span_1_low, span_1_high, span_2_low, span_2_high):
    if span_1_low >= span_2_low and span_1_high <= span_2_high:
        return 1
    elif span_1_low <= span_2_low and span_1_high > span_2_low:
        return 1
    elif span_1_high > span_2_high and span_1_low <= span_2_low:
        return 1
    else:
        # This means that there is no intersection between the two spans
        return 0

def get_span_lower(span_1_low, span_1_high, span_2_low, span_2_high):
    # Returns the proper lower span based on three potential cases
    # Case 1: The in span lower bound falls somewhere within the map span
    if span_1_low >= span_2_low and span_1_low <= span_2_high:
        # The proper lower bound comes from the in span
        return span_1_low
    # Case 2: the in span's upper bound falls within the map span, but the
    #  lower bound falls outside
    elif span_1_low < span_2_low and span_1_high >= span_2_low:
        # The proper lower bound comes from the map span
        return span_2_low
    # Otherwise, the in span does not intersect with the map span
    # Return 0
    else:
        return 0

def get_span_higher(span_1_low, span_1_high, span_2_low, span_2_high):
    # Returns the proper upper span based on three potential cases
    # Case 1: the in span upper bound falls somewhere within the map span
    if span_1_high <= span_2_high and span_1_high >= span_2_low:
        # The proper upper span comes from the in span
        return span_1_high
    # Case 2: the in span's lower bound falls within the map span, but the 
    #  upper bound falls outside
    elif span_1_low <= span_2_high and span_1_high > span_2_high:
        # The proper upper span comes from the map span
        return span_2_high
    # Case 3: the in span and the map span do not intersect
    else:
        # Return 0
        return 0


# Adjusts the span of the in_df to cross-cut the out_df
def adjust_span(in_df, in_span,
                map_df, map_span):
    in_span_low = in_span[0]
    in_span_high = in_span[1]
    map_span_low = map_span[0]
    map_span_high = map_span[1]
    
    in_df['cross'] = 1
    map_df['cross'] = 1
    # Cross multiply
    crossed = in_df.merge(map_df, on='cross')
    crossed['intersect'] = crossed.apply(lambda x: span_intersect(x[in_span_low],x[in_span_high],
                                                                  x[map_span_low],x[map_span_high]),
                                                                  axis=1)
    crossed['new_span_low'] = crossed.apply(lambda x: get_span_lower(x[in_span_low],x[in_span_high],
                                                                     x[map_span_low],x[map_span_high]),
                                                                     axis=1)
    crossed['new_span_high'] = crossed.apply(lambda x: get_span_higher(x[in_span_low],x[in_span_high],
                                                                      x[map_span_low],x[map_span_high]),
                                                                      axis=1)
    crossed = crossed.loc[crossed['intersect'] == 1]
    # drop the old span columns and the intersect column
    crossed = crossed.drop(labels=[in_span_low, in_span_high, 
                                    map_span_low, map_span_high,
                                    'intersect','cross'], axis=1)
    # Rename the new span columns
    crossed = crossed.rename(columns = {'new_span_low':in_span_low,
                                        'new_span_high':in_span_high})
    return crossed
    

def adj_data_template(df):
    print("adjusting years")
    # Returns the closest year that contains GBD results
    # Create the year_id column and set it to a year that contains GBD results
    df = templating.df_mean(df, "year_id", ["year_start", "year_end"])
    gbd_years = get_demographics('epi')['year_id']
    #gbd_years = [1990,1995,2000,2005,2010,2015,2017]
    df['year_id'] = df['year_id'].apply(lambda x: min(gbd_years, key=lambda y: abs(y-x)))
    
    print("Adjusting sexes")
    # subset out demographics
    if 'sex_id' in df.columns:
        sex_dict = {1:'Male', 2:'Female', 3:'Both'}
        df['sex'] = df.apply(lambda x: sex_dict[x['sex_id']], axis=1)
    else:
        sex_dict = {'Male':1, 'male':1,
                    'Female':2, 'female':2,
                    'Both':3, 'both':3}
        df['sex_id'] = df.apply(lambda x: sex_dict[x['sex']],axis=1)

    print("Adjusting ages")

    df = df.loc[
        ((df["age_end"] - df["age_start"]) < 40) |  # > 20 age group
        (df["age_start"] >= 80)] # or terminal
        #| (df["age_end"] ==125)
    
    # get age mapping
    age_map = get_age_metadata(age_group_set_id=12)[
                                ['age_group_id',
                                'age_group_years_start',
                                'age_group_years_end']]
    
    # find the intersection between the in dataframe and the age group df
    df = adjust_span(df, ('age_start','age_end'),
                     age_map, ('age_group_years_start','age_group_years_end'))

    if df.empty:
        raise NoNonZeroValues
    
    return df

#Under-5 grouping
def custom_adj_data_template(df):
    print("adjusting years")
    # Returns the closest year that contains GBD results
    # Create the year_id column and set it to a year that contains GBD results
    df = templating.df_mean(df, "year_id", ["year_start", "year_end"])
    gbd_years = get_demographics('epi')['year_id']
    df['year_id'] = df['year_id'].apply(lambda x: min(gbd_years, key=lambda y: abs(y-x)))

    print("adjusting sexes")
    # subset out demographics
    if 'sex_id' in df.columns:
        sex_dict = {1:'Male', 2:'Female', 3:'Both'}
        df['sex'] = df.apply(lambda x: sex_dict[x['sex_id']], axis=1)
    else:
        sex_dict = {'Male':1, 'male':1,
                    'Female':2, 'female':2,
                    'Both':3, 'both':3}
        df['sex_id'] = df.apply(lambda x: sex_dict[x['sex']],axis=1)

    print("adjusting ages")   

    df = df.loc[
        ((df["age_end"] - df["age_start"]) < 40) |  # > 20 age group
        (df["age_start"] >= 80)] # or terminal
        #| (df["age_end"] ==125)

    age_map = ezfuncs.query(
        ages_query, conn_def="gbd")
    
    # find the intersection between the in dataframe and the age group df
    df = adjust_span(df, ('age_start','age_end'),
                     age_map, ('age_group_years_start','age_group_years_end'))

    if df.empty:
        raise NoNonZeroValues
    
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
        df = df.loc[df[mean_col] > 0]  # get rid of 0 means
        df = df.loc[
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
        df = df.loc[df["sex_id"] != 3]  # get rid of both sex
        df = df.loc[
            ((df["age_end"] - df["age_start"]) < 20) |  # > 20 age group
            (df["age_start"] >= 80)]  # or terminal

        if df.empty:
            raise NoNonZeroValues

        # Add a year_id column
        df['year_mid'] = (df['year_start'] + df['year_end'])/2
        gbd_years = get_demographics('epi')['year_id']
        df['year_id'] = df['year_mid'].apply(lambda x: min(gbd_years, key=lambda y: abs(y-x)))

        df = df.drop(labels=['year_mid'],axis=1)
        # set index
        df[self._data_key] = df[self._data_key].astype(int)
        df = df.set_index(self._data_key)
        return df


class DataModelDataAdj(Data):

    mean_col = "mean"
    se_col = "se"
    _path = ("FILEPATH/"
             "model_data_adj.csv")

    def get_data(self, model_version_id):
        # import data
        path = self._path.format(
            cascade_root="FILEPATH",
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
        df = data_metadata.df.merge(data_model_data_adj.df,
                                    how="inner",
                                    left_index=True,
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
            ######################################
            ## ** CHANGE THESE NUMBERS MANUALLY **
            ######################################
            self.mean = 2
            self.se = .1020408
            self.upper = 2.2


class DataModelEstimateFitEMR(Data):

    mean_col = "mean"
    se_col = "se"

    _path = ("FILEPATH/"
             "model_estimate_fit.csv")

    def get_data(self, model_version_id, id_template_df):
        path = self._path.format(
            cascade_root=envs.Environment.get_cascade_root(),
            mv=model_version_id)
        df = pd.read_csv(path)
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
    # SET MODEL VERSION ID HERE

    def get_data(self, model_version_id, id_template_df):
        print("Loading CSMR csv...")
        # NOW PULLS A DF INSTEAD OF QUERYING THE DATABASE
        # Required fields:
        # location_id, year_id, age_group_id, sex_id, mean, upper, lower
        # The old way: Getting mortality data from a csv
        # data_filepath = '/ihme/gbd/WORK/04_epi/01_database/02_data/tb/csmr/custom_csmr.csv'
        
        age_group_ids = [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,33]
        
        df = get_model_results('epi',model_version_id=model_version_id,
                               age_group_id=age_group_ids,measure_id=15)

        df = df[['location_id','year_id','sex_id','age_group_id',
             'mean','lower','upper']].copy()
        print (df.head(5))

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
