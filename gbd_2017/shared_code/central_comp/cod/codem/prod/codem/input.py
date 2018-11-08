import query as Q
import queryStrings as QS
import knockout as ko
import numpy as np
import pandas as pd
import sqlalchemy
import os
import matplotlib
import db_connect
matplotlib.use('Agg')
import pylab
import matplotlib.pyplot as plt
import seaborn as sns
import emailer as email
from db_queries import get_envelope
from space_time_smoothing import import_json
from model import dic2df


class Inputs:
    def __init__(self, model_version_id, db_connection, update, debug_mode, gbd_round_id):
        for k, v in Q.getModelParams(model_version_id, db_connection, update).items():
            setattr(self, k, v)
        self.data_frame, self.covariates, self.priors = \
            Q.getCodemInputData(model_version_id, db_connection,
                                gbd_round_id, outlier_save=True)
        self.db_connection = db_connection
        self.cv_names = self.priors.name.values
        self.reference = Q.get_raw_reference_data(self.priors, self.data_frame,
                                                  "reference")
        self.site_spec = Q.get_raw_reference_data(self.priors, self.data_frame,
                                                  "site_specific")
        self.knockouts = None
        self.response_list = None
        self.best_psi = None
        self.ensemble_preds = None
        self.psi_values = np.arange(self.psi_weight_min, self.psi_weight_max +
                                    self.psi_weight_int, self.psi_weight_int)
        self.draws = None
        self.country_df = None
        self.region_df = None
        self.super_region_df = None
        self.age_df = None
        self.submodel_covariates = None
        self.draw_id = None
        self.submodel_rmse = None
        self.submodel_trend = None
        self.model_dir = "FILEPATH".format(self.acause, self.model_version_id)
        self.debug_mode = debug_mode
        self.gbd_round_id = gbd_round_id
        self.cause_name = Q.cause_name_from_id(
            self.cause_id, self.db_connection)
        self.date = Q.model_date(self.model_version_id, self.db_connection)
        self.model_version_type_id = Q.model_version_type_id(
            self.model_version_id, self.db_connection)
        self.agg_dfs = {}

    def __repr__(self):
        m, c = (self.model_version_id, self.acause)
        return str("Inputs for model version {0}, cause: {1}").format(m, c)

    def create_knockouts(self, seed=None):
        if seed is None:
            seed = self.model_version_id
        self.knockouts = ko.generate_knockouts(self.data_frame,
                                               self.holdout_number, seed)

    def label_submodels(self, space_ids, mixed_ids):
        self.submodel_covariates["space"] = \
            {space_ids[x]: self.submodel_covariates["space"][x]
             for x in range(len(space_ids))}
        self.submodel_covariates["mixed"] = \
            {mixed_ids[x]: self.submodel_covariates["mixed"][x]
             for x in range(len(mixed_ids))}

    def add_draws_to_df(self, truncate=False, percentile=99):
        self.draws = np.exp(self.draws) * \
            self.data_frame["pop"].values[:, np.newaxis]
        if truncate:
            self.draws = Q.truncate_draws(self.draws, percent=percentile)
        draw_df = pd.DataFrame(self.draws)
        draw_df.columns = \
            ["draw_%d" % i for i in range(0, draw_df.shape[1])]
        self.draws = None
        self.data_frame = pd.concat([self.data_frame, draw_df], axis=1)
        self.data_frame.drop_duplicates(
            ["year", "location_id", "age"], inplace=True)
        self.data_frame.reset_index(drop=True, inplace=True)

    def bare_necessities(self):
        variables = ["super_region", "region", "country_id", "location_id",
                     "age", "year", "cf", "envelope", "pop"]
        self.data_frame = self.data_frame[variables]

    def aggregate_draws(self):
        '''
        (self) -> None

        Aggregate the draws up to the country, region, and super region levels.
        We also need to throw in the age aggregates in globally.
        '''
        # store age aggregate dataframes in a dictionary for easier access later
        self.agg_dfs['age_location'] = \
            self.data_frame.groupby(
                ['location_id', 'year'], as_index=False).sum()
        self.agg_dfs['age_location']["age"] = 22

        self.country_df = \
            self.data_frame.groupby(
                ['country_id', 'age', 'year'], as_index=False).sum()
        self.country_df.drop('location_id', axis=1, inplace=True)
        self.country_df.rename(
            columns={'country_id': 'location_id'}, inplace=True)
        countries = np.setdiff1d(self.country_df.location_id.unique(),
                                 self.data_frame.location_id.unique())
        self.country_df = self.country_df[self.country_df["location_id"].isin(
            countries)]
        self.country_df.reset_index(drop=True, inplace=True)

        self.agg_dfs['age_country'] = \
            self.country_df.groupby(
                ['location_id', 'year'], as_index=False).sum()
        self.agg_dfs['age_country']["age"] = 22

        self.region_df = \
            self.data_frame.groupby(
                ['region', 'age', 'year'], as_index=False).sum()
        self.region_df.drop(['location_id', 'country_id'],
                            axis=1, inplace=True)
        self.region_df.rename(columns={'region': 'location_id'}, inplace=True)

        self.agg_dfs['age_region'] = \
            self.region_df.groupby(
                ['location_id', 'year'], as_index=False).sum()
        self.agg_dfs['age_region']["age"] = 22

        self.super_region_df = \
            self.data_frame.groupby(
                ['super_region', 'age', 'year'], as_index=False).sum()
        self.super_region_df.drop(
            ['location_id', 'country_id', 'region'], axis=1, inplace=True)
        self.super_region_df.rename(
            columns={'super_region': 'location_id'}, inplace=True)

        self.agg_dfs['age_super_region'] = \
            self.super_region_df.groupby(
                ['location_id', 'year'], as_index=False).sum()
        self.agg_dfs['age_super_region']["age"] = 22

        self.age_df = \
            self.data_frame.groupby(['age', 'year'], as_index=False).sum()
        self.age_df.drop(['super_region', 'country_id',
                          'region'], axis=1, inplace=True)
        self.age_df.location_id = 1

        self.agg_dfs['global'] = self.data_frame.groupby(
            ["year"], as_index=False).sum()
        self.agg_dfs['global']["location_id"] = 1
        self.agg_dfs['global']["age"] = 22
        self.agg_dfs['global'].drop(
            ["super_region", "region", "country_id"], axis=1, inplace=True)
        self.data_frame["sex"] = self.sex_id
        self.data_frame["cause"] = self.acause

    def get_full_envelope(self):
        '''
        (self) -> None

        For models with age restrictions, the all-ages group should use envelope
        for all ages. This function replaces the age-restricted envelope for the
        full envelope for each location-year, country-year, region-year,
        super-region-year, and global-year
        '''
        print "We're definitely pulling the full envelope."
        for df_type in ['age_location', 'age_country', 'age_region',
                        'age_super_region', 'global']:
            self.agg_dfs[df_type].drop('envelope', inplace=True, axis=1)
            locs = self.agg_dfs[df_type].location_id.values.tolist()
            years = self.agg_dfs[df_type].year.values.tolist()
            # get the envelope, then merge in with the aggregated dfs
            env_df = get_envelope(age_group_id=22, gbd_round_id=self.gbd_round_id,
                                  location_id=locs, sex_id=self.sex_id,
                                  year_id=years)
            env_df = env_df[['location_id', 'year_id', 'mean']]
            env_df.rename(columns={'mean': 'envelope',
                                   'year_id': 'year'}, inplace=True)
            self.agg_dfs[df_type] = self.agg_dfs[df_type].merge(env_df,
                                                                on=['location_id', 'year'])

    def write_draws(self):
        '''
        (self) -> None

        Write the most granular results to an HDF file. Make the keys a
        location_id year for ease of access in cod correct use. Includes all of
        the draws from codem as well as some other variables such as age, sex,
        population, envelope.
        '''
        if self.sex_id == 1:
            sex = "male"
        else:
            sex = "female"
        keep_cols = ["draw_%d" % i for i in range(0, 1000)] + \
                    ["envelope", "pop", "age", "sex", "year", "location_id"]
        if not os.path.exists("draws"):
            os.makedirs("draws")
        df2 = self.data_frame[keep_cols]
        df2 = df2.rename(
            columns={"age": "age_group_id", "sex": "sex_id", "year": "year_id"})
        df2["measure_id"] = 1
        df2["cause_id"] = self.cause_id
        df2.location_id = df2.location_id.astype(int)
        df2.age_group_id = df2.age_group_id.astype(int)
        df2.year_id = df2.year_id.astype(int)
        df2.to_hdf('FILEPATH'.format(sex=sex), 'data', mode='w', format='table',
                   data_columns=['location_id', 'year_id', 'age_group_id'])
        pd.DataFrame({"keys": ['location_id', 'year_id',
                               'age_group_id']}).to_hdf('FILEPATH'.format(sex=sex),
                                                        key="keys", mode='a', format='table')

    def write_submodel_covariates(self):
        '''
        (self) -> None

        For every submodel that is used in codem write all the covariates for
        that submodel into the database for the ability to skip covariate
         selection in future runs of codem.
        '''
        link = {self.priors.name[i]: self.priors.covariate_model_id[i]
                for i in range(len(self.priors))}
        for model in self.submodel_covariates.keys():
            for submodel in self.submodel_covariates[model].keys():
                dic = self.submodel_covariates[model][submodel]
                submodel_covariate_ids = [link[c] for c in dic]
                Q.write_submodel_covariate(submodel, submodel_covariate_ids,
                                           self.db_connection)

    def write_model_mean(self):
        '''
        (self) -> None

        Write the submodel means of the codem run for all submodels which we
        have more than 50 draws for.

        May increase the partitions if run out of space for model_version in cod.submodel.
        '''
        valid_models = [m for m in set(
            self.draw_id) if self.draw_id.count(m) >= 100]
        for model in valid_models:
            columns = ["draw_%d" % i for i in np.where(
                np.array(self.draw_id) == model)[0]]
            for df_true in [self.data_frame, self.country_df, self.region_df,
                            self.super_region_df, self.age_df, self.agg_dfs['age_location'],
                            self.agg_dfs['age_country'], self.agg_dfs['age_region'],
                            self.agg_dfs['age_super_region'], self.agg_dfs['global']]:
                df = df_true.copy()
                df["sex"] = self.sex_id
                if df.shape[0] == 0:
                    continue
                df_sub = df.loc[:, ["year", "location_id",
                                    "sex", "age", "envelope"] + columns]
                df_sub.loc[:, columns] = df_sub[columns].values / \
                    df_sub["envelope"].values[..., np.newaxis]
                df_sub["mean_cf"] = df_sub[columns].mean(axis=1)
                df_sub["lower_cf"] = df_sub[columns].quantile(q=0.025, axis=1)
                df_sub["upper_cf"] = df_sub[columns].quantile(q=0.975, axis=1)
                df_sub = df_sub[["year", "location_id", "sex",
                                 "age", "mean_cf", "lower_cf", "upper_cf"]]
                df_sub.rename(
                    columns={'year': 'year_id', 'sex': 'sex_id', 'age': 'age_group_id'}, inplace=True)
                df_sub["model_version_id"] = self.model_version_id
                df_sub["submodel_version_id"] = model
                # check for partitions. When there are enough partitions for this model version ID,
                # countPartition will return 1, and then we can write the df to sql
                count = db_connect.countPartition(
                    'cod', 'submodel', self.model_version_id, self.db_connection)
                if count == 0:
                    db_connect.increase_partitions(
                        self.db_connection, 'cod', 'submodel')

                db_connect.write_df_to_sql(
                    df_sub, db="cod", table="submodel", connection=self.db_connection)
        for df_true in [self.data_frame, self.country_df, self.region_df,
                        self.super_region_df, self.age_df,
                        self.agg_dfs['global'], self.agg_dfs['age_location'],
                        self.agg_dfs['age_country'], self.agg_dfs['age_region'],
                        self.agg_dfs['age_super_region']]:
            if df_true.shape[0] == 0:
                continue
            Q.write_model_output(df_true, self.model_version_id, self.sex_id,
                                 self.db_connection)

    def write_pv(self):
        tags = ["pv_rmse_in", "pv_rmse_out", "pv_coverage_in", "pv_coverage_out",
                "pv_trend_in", "pv_trend_out", "pv_psi", "status"]
        values = [self.pv_rmse_in, self.pv_rmse_out, self.pv_coverage_in,
                  self.pv_coverage_out, self.pv_trend_in, self.pv_trend_out,
                  self.best_psi, 1]
        for i in range(len(tags)):
            Q.write_model_pv(tags[i], float(values[i]), self.model_version_id,
                             self.db_connection)

    def create_global_table(self):
        df = self.agg_dfs['global'].copy()
        columns = ["draw_%d" % (i) for i in range(100)]
        df["mean_deaths"] = df[columns].mean(axis=1)
        df["lower_deaths"] = df[columns].quantile(.025, axis=1)
        df["upper_deaths"] = df[columns].quantile(.975, axis=1)
        plt.figure(figsize=(16, 12))
        plt.plot(df.year.values, df.mean_deaths.values,
                 label="Mean Death Prediction")
        plt.fill_between(df.year.values, df.lower_deaths.values, df.upper_deaths.values,
                         alpha=.3, color='blue', label='95% confidence interval')
        plt.legend(loc=0)
        plt.xlabel("$Year$", fontsize=20)
        plt.ylabel("$Deaths$", fontsize=20)
        plt.title("CODEm Estimates", fontsize=24)
        pylab.savefig("global_estimates.png")

    def purge_fe(self, df, column):
        """
        Get rid of unwanted fixed effects from data frame:
        the age fixed effects and intercept.

        :param df: input data frame
        :param column: column with names for fixed effects
        :return df: data frame without rows for age fixed effects or intercept
        """
        age_cols = [x for x in df[column].values if x.startswith('age')]
        df = df.loc[~df[column].isin(["(Intercept)"] + age_cols)]
        return df

    def get_fe_and_var(self, d, key, mtype):
        """
        Get fixed effects and standard error for a model. Note: we are using "ko21" because the last knockout
        contains all of the data (nothing knocked-out).

        :param d: dictionary with model type, model names, and knockouts
        :param key: model index in key
        :param mtype: model type -- mixed or spacetime
        :return fix_eff: data frame with covariate fixed effects and standard error for a given submodel.
        """
        d_sub = d[mtype][key]["ko21"]
        fix_eff = dic2df(d_sub["fix_eff"]).reset_index()

        vcov = d_sub["vcov"]
        se = np.sqrt(np.diagonal(vcov))
        fix_eff["se"] = se

        fix_eff = self.purge_fe(fix_eff, '_row')
        fix_eff.rename(columns={'_row': 'covariate'}, inplace=True)

        fix_eff["model"] = key
        fix_eff["type"] = mtype

        return fix_eff

    def calc_sd(self, vartype, name):
        '''
        Calculates the standard deviation of a variable in the mod inputs
        Either a covariate or an outcome variable.

        :param vartype: (str) one of "covariate" or "outcome"
        :param name: (str) name of the covariate or name of outcome
        :return std: (float) the standard deviation of the input variable
        '''
        if vartype == "covariate":
            var = self.covariates[name]
        else:
            data = self.data_frame
            if name == "rate":
                var = np.log(data["cf"] * data["envelope"] / data["pop"])
            else:
                var = np.log(data["cf"].map(lambda x: x / (1.0 - x)))

        std = np.std(var)
        return std

    def get_sd_ratio(self, covariate, outcome):
        '''
        Gets the ratio of the standard deviations. This is done in a separate
        function than the standardizing betas because it takes a while. So just saving
        the ratios cuts down so that we don't have to compute this for every draw.

        :param covariate: (str) name of covariate
        :param outcome: (str) outcome being predicted (so rate or cf)
        :return ratio: (float) ratio of the standard deviations
        '''
        cov_sd = self.calc_sd("covariate", covariate)
        out_sd = self.calc_sd("outcome", outcome)

        ratio = cov_sd / out_sd
        return ratio

    def make_ratio_dict(self, covariates):
        '''
        Makes a dictionary of all of the standard deviation ratios for the list of covariates, by model type.

        :param covariates: list of covariates
        :return rdict: (dict) dictionary like {covariate: ratio}
        '''
        rdict = {}
        for mtype in ["rate", "cf"]:
            covdict = {}
            for cov in covariates:
                covdict[cov] = self.get_sd_ratio(cov, mtype)
            rdict[mtype] = covdict
        return rdict

    def get_covlist(self, d):
        '''
        Gets a unique list of covariates that is used across all submodels.

        :param d: dictionary of linear model or spacetime model (they have the same covariate lists in aggregate)
        :return covlist: (list) list of covariate names
        '''
        covlist = []
        for key in d.keys():
            covs = d[key]["ko21"]["fix_eff"]["_row"]
            covlist = covlist + covs
        covlist = list(set(covlist))

        bad_cols = [x for x in covlist if x.startswith(
            'age')] + ["(Intercept)"]
        covlist = [x for x in covlist if x not in bad_cols]
        return covlist

    def standardize_beta(self, covariate, outcome, value, rat_dict):
        '''
        Creates a standardized beta by multiplying the beta value by the ratio of the standard
        deviation of the outcome to the standard deviation of the covariate.

        :param covariate: (str) covariate name
        :param outcome: (str) outcome type (rate or cf)
        :param value: (float) un-standardized beta value
        :param rat_dict: (dict) dictionary with keys for model type and covariate with values for std. ratios
        :return standard: (float) standardized beta value
        '''
        ratio = rat_dict[outcome][covariate]

        standard = value * ratio
        return standard

    def weighted(self, x, cols, weight):
        '''
        Get weighted average for each column based on weight col. Used in a groupby call.

        :param x: (dataframe) data frame from groupby
        :param cols: (list) column names to apply this over
        :param weight: (str) name of the weight column to use
        :return: series of weighted average
        '''
        return pd.Series(np.average(x[cols], weights=x[weight], axis=0), cols)

    def submodel_summary_table(self):
        """
        Create submodel-specific summary table (1 row for each submodel) mostly with predictive validity metrics.

        :return df: data frame with submodel-specific summaries
        """
        df = Q.get_submodel_summary(self.model_version_id, self.db_connection)
        df = df.sort_values(['rank', 'submodel_version_id'], ascending=False)
        df = df.drop_duplicates(['rank'], keep="first")
        covs = self.submodel_covariates["mixed"]
        covs.update(self.submodel_covariates["space"])
        df["rmse_out"] = df.submodel_version_id.map(
            lambda x: self.submodel_rmse[x])
        df["trend_out"] = df.submodel_version_id.map(
            lambda x: self.submodel_trend[x])
        df["covariates"] = df.submodel_version_id.map(
            lambda x: ', '.join(covs[x]))
        df.sort_values("rank", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def covariate_summary_df(self):
        """
        Create summary of submodels with n-draws.

        :return submodel_df: submodel data frame
        """
        submodels = self.submodel_summary_table()
        draws = self.draw_id
        df = pd.DataFrame(pd.DataFrame(
            draws, columns=["id"]).groupby('id').size()).reset_index()
        df.columns = ['submodel_version_id', 'n_draws']
        submodel_df = submodels.merge(
            df, how='left', on=['submodel_version_id'])
        return submodel_df

    def create_model_type_table(self):
        '''
        Create table of types of models for modeler email. Counts the number of mixed/st cf/rate models.

        :return df: data frame for model types
        '''
        df = self.covariate_summary_df()
        df = df.loc[df.n_draws > 0]
        df["Number"] = 1
        df.rename(columns={'n_draws': 'Draws'}, inplace=True)
        df = df[['Type', 'Dependent_Variable', 'Number', 'Draws']
                ].groupby(['Type', 'Dependent_Variable']).sum()
        df = df.reset_index()
        df.columns = ['Type', 'Dependent Variable',
                      'Number of Models', 'Number of Draws']
        return df

    def get_covar_df(self):
        '''
        Get covariate data frame for all submodels. Gives their fixed effects and their standard errors.

        :return df: covariate data frame for all submodels
        :return keys: model names (e.g. ln_rate_model001) in order
        '''
        lm_dict = import_json("FILEPATH.txt")
        st_dict = import_json("FILEPATH.txt")

        master_json = {'mixed': lm_dict, 'spacetime': st_dict}

        keys = sorted(lm_dict.keys())
        covar_list = [[self.get_fe_and_var(
            master_json, key, mtype) for key in keys] for mtype in ['mixed', 'spacetime']]

        df = pd.concat(map(pd.concat, covar_list))
        df.rename(columns={'values': 'beta'}, inplace=True)
        return df, keys

    def merge_submodels_with_keys(self):
        '''
        IMPORTANT: Merge the submodel metadata with predictive validity/n_draws/rank onto the covariate dataframe.
        USES the keys from the submodel. The sorted values are key -- the keys are sorted and the submodel table
        is sorted, and they line up row by row.

        Note: This is the only way to link the json with covariate betas/SE with the submodel metadata.  Do not
        unsort by submodel version ID whatever you do! If you do they keys will align with the wrong models.

        :return df: data frame
        '''
        submodels = self.covariate_summary_df(
        ).sort_values(['submodel_version_id'])
        beta_df, keys = self.get_covar_df()

        submodels['model'] = keys + keys
        submodels.rename(columns={'Type': 'type'}, inplace=True)

        df = beta_df.merge(submodels, on=['model', 'type'])

        lm_dict = import_json("FILEPATH.txt")
        covlist = self.get_covlist(lm_dict)
        ratio_dict = self.make_ratio_dict(covlist)

        df.loc[df.n_draws.isnull(), 'n_draws'] = 0.0

        df.sort_values(['covariate', 'n_draws'], inplace=True)
        for mtype in ['ln_rate', 'lt_cf']:
            df.loc[df['model'].str.contains(mtype), 'mtype'] = mtype

        df["standard_beta"] = 0.
        for index, row in df.iterrows():
            standard = self.standardize_beta(
                row["covariate"], row["Dependent_Variable"], row["beta"], ratio_dict)
            df.set_value(index, 'standard_beta', standard)
        return df

    def create_weighted_beta_column(self):
        '''
        Create a data frame with a row for each covariate with weighted betas and standardized betas.

        :return df: data frame
        '''
        df = self.merge_submodels_with_keys()
        df = df.loc[df.n_draws > 0]
        df = df.groupby(df.covariate).apply(
            self.weighted, ["beta", "standard_beta"], weight="n_draws").reset_index()

        df.columns = ['name', 'beta', 'standard_beta']
        return df

    def create_covariate_table(self):
        '''
        Create table with one row per covariate with covariate metadata and also the standardized betas/weighted betas.

        At the end it also creates "relative" betas. These are to show the relative influence of the betas in the
        final ensemble because effectively, in submodels where this covariate doesn't exist, it should have a value of
        0 with weight sum(n_draws) for those submodels. Note: This is not taken into account with the weighted average
        because it is only a weighted average across submodels that have that covariate.

        :return info: data frame
        '''

        df = self.covariate_summary_df(
        )[['covariates', 'n_draws', 'submodel_version_id']]
        num_models = len(df.submodel_version_id)
        df.loc[df.n_draws.isnull(), 'n_draws'] = 0.0

        covariates = df['covariates'].str.split(', ', expand=True)
        new_cols = ["covariate_{}".format(x) for x in range(
            1, len(covariates.columns) + 1)]
        df[new_cols] = covariates

        ids = ['n_draws', 'submodel_version_id']
        df = pd.melt(df[ids + new_cols], id_vars=ids)
        df = df.loc[df.value.notnull()]
        df.rename(columns={'value': 'name'}, inplace=True)
        df.drop('variable', inplace=True, axis=1)

        draws = pd.DataFrame(df.groupby('name')['n_draws'].sum()).reset_index()
        submodels = pd.DataFrame(df.groupby(
            'name')['submodel_version_id'].count()).reset_index()
        submodels.rename(
            columns={'submodel_version_id': 'n_submodels'}, inplace=True)

        info = self.priors
        info = info.merge(draws, how='right', on='name').merge(submodels, how='right',
                                                               on='name')
        info = info[['name', 'lag', 'transform_type_short',
                     'level', 'direction', 'offset', 'n_draws', 'n_submodels']]
        info = info.sort_values(
            ['n_draws', 'n_submodels'], ascending=False).reset_index(drop=True)

        beta_df = self.create_weighted_beta_column()

        info = info.merge(beta_df, on=['name'])
        info["percent_included_ensemble_draws"] = info.n_draws / 1000
        info["percent_included_ensemble_submodel"] = info.n_submodels / num_models

        info["beta_relative"] = info.beta * \
            info.percent_included_ensemble_draws
        info["standard_beta_relative"] = info.standard_beta * \
            info.percent_included_ensemble_draws

        return info

    def make_draw(self, fix_eff, vcov):
        '''
        Make one draw from the beta mean + variance-covariance matrix.

        :param fix_eff: (array) fixed effects array
        :param vcov: (array) the variance-covariance matrix for the submodel
        :return: (array) beta draw for all fixed effects
        '''
        return np.dot(np.linalg.cholesky(vcov), np.random.normal(size=len(vcov))) + fix_eff.values.ravel()

    def make_ndraws_df(self, n_draws, fix_eff, vcov, response, rat_dict, scale):
        '''
        Make n draws from the beta mean + variance-covariance matrix. Also standardizes them for an additional column.

        :param n_draws: (int) number of draws for this submodel
        :param fix_eff: (array) fixed effects array
        :param vcov: (array) variance-covariance matrix of this submodel
        :param response: (str) type of response (lt or cf)
        :param rat_dict: (dict) dictionary of ratios of standard deviations of outcome / covariate by model type
                                and covariate combination
        :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
        :return df: data frame
        '''
        n_draws = int(n_draws)
        df = [self.make_draw(fix_eff, vcov) for x in range(n_draws) * scale]

        df = pd.DataFrame(df)
        df.columns = fix_eff.index
        df = pd.melt(df)

        df.columns = ['covariate', 'beta']

        df = self.purge_fe(df, "covariate")

        df["standard_beta"] = df.apply(lambda x: self.standardize_beta(x['covariate'], response, x['beta'], rat_dict),
                                       axis=1)
        return df

    def get_full_df(self, json, mtype, key, response, n_draws, rat_dict, scale):
        '''
        Makes a data frame for all covariates in ONE submodel with (n_draws * scale) draws of the betas
        (Also standardizes the betas)

        :param json: (dict) master json dictionary -- both mixed and spacetime models put together
        :param mtype: (str) model type (mixed or spacetime)
        :param key: (str) name of the model (e.g. ln_rate_model001)
        :param response: (str) model response (rate or cf)
        :param n_draws: (int) number of draws for the submodel
        :param rat_dict: (dict) dictionary of ratios of standard deviations of outcome / covariate by model type
                                and covariate combination
        :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
        :return df: data frame
        '''
        little_json = json[mtype][key]["ko21"]
        fix_eff = dic2df(little_json["fix_eff"])
        vcov = little_json["vcov"]

        df = self.make_ndraws_df(
            n_draws, fix_eff, vcov, response, rat_dict, scale)
        df["model_type"] = mtype
        df["model_name"] = key
        df["dependent_variable"] = response
        df["n_draws"] = n_draws
        return df

    def get_beta_draws(self, scale):
        '''
        Make beta draws (and standard beta draws) for ALL submodels.

        :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
        :return df: data frame
        '''

        submodels = self.covariate_summary_df(
        ).sort_values(['submodel_version_id'])
        beta_df, keys = self.get_covar_df()

        submodels['model'] = keys + keys
        submodels.rename(columns={'Type': 'type'}, inplace=True)

        lm_dict = import_json("FILEPATH.txt")
        st_dict = import_json("FILEPATH.txt")

        master_json = {'mixed': lm_dict, 'spacetime': st_dict}
        submodels = submodels.loc[submodels.n_draws > 0]

        covlist = self.get_covlist(lm_dict)
        ratio_dict = self.make_ratio_dict(covlist)

        df = pd.concat([self.get_full_df(master_json, row['type'], row['model'], row['Dependent_Variable'],
                                         row['n_draws'], ratio_dict, scale) for index, row in submodels.iterrows()])

        return df

    def plot_comparisons(self, df, name, varname, sharex=False, sharey=False):
        '''
        Plot the kernel densities of covariate distributions in the same seaborn FacetGrid. Outputs the plot
        to the model directory.

        :param df: input data frame with draws
        :param name: name of the plot
        :param varname: name of the variable to put in the kernel density (one of "beta", "standard_beta")
        :param sharex: fix x-scales
        :param sharey: fix y-scales
        '''
        df["model"] = df["model_type"] + " " + df["dependent_variable"]
        g = sns.FacetGrid(df, col="covariate", hue="dependent_variable", margin_titles=True, col_wrap=2, sharex=sharex,
                          sharey=sharey, aspect=3)
        g.map(sns.distplot, varname, hist=False, kde_kws={
              "shade": True}).add_legend(title="Model Type")
        g.map(plt.axvline, x=0, ls=":", c=".5")
        plt.subplots_adjust(top=0.9)
        g.fig.suptitle(name)
        g.set_titles(col_template='{col_name}')
        pylab.savefig("FILEPATH" % (varname))

    def create_draw_plots(self):
        '''
        Create both beta draw plots -- one for standard betas and one for betas.

        :return df: data frame of beta draws
        '''
        df = self.get_beta_draws(scale=100)
        df.to_csv(self.model_dir + "/beta_draws.csv")

        title = "{cause} \n Date: {date} \n Model Version ID: {mvid}, Sex ID: {sex}".format(cause=self.cause_name,
                                                                                            date=self.date,
                                                                                            mvid=self.model_version_id,
                                                                                            sex=self.sex_id)
        sns.set(style="white", palette="muted",
                color_codes=True, font_scale=1.5)
        self.plot_comparisons(df, "Standard Betas: '%s'" % (
            title), "standard_beta", sharex=True, sharey=False)
        self.plot_comparisons(df, "Betas: '%s'" % (
            title), "beta", sharex=False, sharey=False)

        return df

    def create_covar_ndraws_plot(self):
        '''
        Create plot of number of draws per covariate for modeler email.
        '''
        df = self.create_covariate_table()
        plt.figure(figsize=(16, 12))
        plt.bar(df.index, height=df.n_draws.values, align="center")
        plt.xticks(df.index, df.name, fontsize=15, rotation=45, ha='right')
        plt.ylabel("Number of Draws", fontsize=20)
        plt.title("{cause} \n Date: {date} \n Model Version ID: {mvid}, Sex ID: {sex}".format(cause=self.cause_name,
                                                                                              date=self.date,
                                                                                              mvid=self.model_version_id,
                                                                                              sex=self.sex_id), fontsize=22)
        sns.set_style("darkgrid")
        plt.tight_layout()
        pylab.savefig("FILEPATH.png")

    def create_covar_submodel_plot(self):
        '''
        Create plot fo number of submodels per covariate for modeler email.
        '''
        df = self.create_covariate_table()
        plt.figure(figsize=(16, 12))
        plt.bar(df.index, height=df.n_submodels.values, align="center")
        plt.xticks(df.index, df.name, fontsize=15, rotation=45, ha='right')
        plt.ylabel("Number of Submodels")
        plt.title("{cause} \n Date: {date} \n Model Version ID: {mvid}, Sex ID: {sex}".format(cause=self.cause_name,
                                                                                              date=self.date,
                                                                                              mvid=self.model_version_id,
                                                                                              sex=self.sex_id), fontsize=22)
        sns.set_style("darkgrid")
        plt.tight_layout()
        pylab.savefig("FILEPATH.png")

    def create_email_body(self):
        self.create_global_table()
        self.create_covar_ndraws_plot()
        self.create_covar_submodel_plot()
        self.create_draw_plots()

        # save the submodel summary table as a csv

        df = self.merge_submodels_with_keys()
        df.to_csv(self.model_dir + "/FILEPATH.csv")

        df = self.submodel_summary_table()
        df.to_csv(self.model_dir + "/FILEPATH.csv")

        validation_table = QS.validation_table.format(RMSE_in_ensemble=self.pv_rmse_in,
                                                      RMSE_out_ensemble=self.pv_rmse_out,
                                                      trend_in_ensemble=self.pv_trend_in[0],
                                                      trend_out_ensemble=self.pv_trend_out,
                                                      coverage_in_ensemble=self.pv_coverage_in,
                                                      coverage_out_ensemble=self.pv_coverage_out,
                                                      RMSE_out_sub=df[df["rank"]
                                                                      == 1]["rmse_out"][0],
                                                      trend_out_sub=df[df["rank"] == 1]["trend_out"][0])
        validation_table = validation_table.replace("\n", "")

        # get the two additional tables to include
        model_type_table = self.create_model_type_table()
        covariate_table = self.create_covariate_table()
        covariate_table.to_csv(self.model_dir + "/FILEPATH.csv")

        frame_work = QS.frame_work.format(user=self.inserted_by,
                                          description=self.description,
                                          sex=["", "males",
                                               "females"][self.sex_id],
                                          start_age=self.age_start,
                                          end_age=self.age_end,
                                          run_time=Q.get_codem_run_time(
                                              self.model_version_id, self.db_connection),
                                          codx_link="URL",
                                          best_psi=self.best_psi,
                                          validation_table=validation_table,
                                          number_of_submodels=df.shape[0],
                                          model_type_table=model_type_table.to_html(
                                              index=False),
                                          number_of_covariates=covariate_table.shape[0],
                                          covariate_table=covariate_table.to_html(
                                              index=False),
                                          path_to_outputs=self.model_dir)
        frame_work = frame_work.replace("\n", "")
        return frame_work

    def get_modeler(self):
        '''
        (self) -> string

        Look up the modeler for the cause being modeled in the cod.modeler table
        '''
        call = """ SELECT m.username
            FROM cod.modeler m
                INNER JOIN shared.cause c
                ON m.cause_id = c.cause_id
            WHERE c.acause = '{acause}' AND m.gbd_round_id = {gbd}
            """.format(acause=self.acause, gbd=self.gbd_round_id)
        modeler = db_connect.query(call, self.db_connection)
        return modeler.ix[0, 0].split(', ')

    def send_email(self):
        '''
        (self) -> None

        Send an email, and create a png of global estimates, but only if the
        user is not codem or nmarquez. Don't send an email to codem@uw.edu ever (it is a real person).
        '''
        email_body = self.create_email_body()

        s = email.Server("smtp.gmail.com", 'EMAIL', 'PASSWORD')

        s.connect()
        e = email.Emailer(s)

        # add graphs
        e.add_attachment('FILEPATH.png')
        e.add_attachment('FILEPATH.png')
        e.add_attachment('FILEPATH.png')
        e.add_attachment('FILEPATH.png')
        e.add_attachment('FILEPATH.png')

        # add the submodel table as an attachment
        e.add_attachment("FILEPATH.csv")
        e.add_attachment("FILEPATH.csv")

        recipients = set(self.get_modeler() + [self.inserted_by])
        recipients = [x for x in recipients if x not in ["codem"]]

        for recipient in recipients:
            e.add_recipient('%s@EMAIL' % recipient)

        if self.model_version_type_id == 1:
            model_version_string = "global"
        else:
            model_version_string = "data rich"

        e.set_subject('CODEm model complete for {acause}, sex {sex}, age group ids {age_start}-{age_end}, '
                      '{mtype}, model version {mvid}'.format(acause=self.acause,
                                                             mvid=self.model_version_id,
                                                             mtype=model_version_string,
                                                             sex=self.sex_id,
                                                             age_start=self.age_start,
                                                             age_end=self.age_end))
        e.set_body(email_body)
        e.send_email()
        s.disconnect()

    def post_processing(self):
        self.bare_necessities()
        self.add_draws_to_df()
        self.aggregate_draws()
        self.get_full_envelope()
        self.write_draws()
        self.write_submodel_covariates()
        self.write_model_mean()
        self.write_pv()
        self.send_email()
