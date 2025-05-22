import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.special import logit, expit
from mrtool import MRBRT, MRData, LinearCovModel
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import mrtool.core.other_sampling as sampling
import dill
from sklearn.preprocessing import StandardScaler
from typing import List
import yaml
import shutil
import glob
from sklearn.metrics import mean_squared_error, mean_absolute_error
from utils import split_time_since_initiation, get_region_iso3


N_DRAWS = 100

REGIONS_DICT = {
    'Western Sub-Saharan Africa': 'WSA',
    'Latin America and Caribbean': 'LAC',
    'Southern Sub-Saharan Africa': 'SSA',
    'Southeast Asia, East Asia, and Oceania': 'SEA',
    'Central Sub-Saharan Africa': 'CSA',
    'Eastern Sub-Saharan Africa': 'ESA',
    'South Asia': 'SA',
    'Central Europe, Eastern Europe, and Central Asia': 'CEEECA',
    'High-income': 'HI'
}


@dataclass
class OnART:
    meta_file: str = "meta.yaml"
    region: str = None

    def __post_init__(self):
        self.load_meta_data()
        self.assign_meta_data()
        
        print(self.covariates)
        if not isinstance(self.covariates, list):
            self.covariates = list(self.covariates)

        if len(self.covariates) > 0:
            self.use_covariates = True
        else:
            self.use_covariates = False

        if not self.i_file.exists():
            raise ValueError(f"{self.i_file} does not exist.")
        
        self.o_folder = Path(os.path.join(self.o_folder, self.version))
        if not self.o_folder.exists():
            self.o_folder.mkdir()
        shutil.copy(self.meta_file, self.o_folder)

        self.run(self.region)
    
    def load_meta_data(self):
        """Load meta data."""
        with open(self.meta_file, "r") as f:
            self.meta = yaml.load(f, Loader=yaml.FullLoader)

    def assign_meta_data(self):
        """Assign meta data."""
        meta = self.meta.copy()
        self.i_file = Path(meta['i_file'])
        self.o_folder = Path(meta['o_folder'])
        self.version = meta['version']
        self.gbd_folder_brad_models = meta['gbd_folder_brad_models']
        self.gbd_folder_90_models = meta['gbd_folder_90_models']
        self.covariates = meta['covariates']
        self.use_beta_priors = meta['use_beta_priors']
        self.use_beta_monotonicity = meta['use_beta_monotonicity']
        self.use_cluster = meta['use_cluster']
        self.transform_covariates = meta['transform_covariates']
        self.dem_covs = meta['dem_covs']
        self.split_data = meta['split_data']
        self.train_size = meta['train_size']
        self.use_beta_priors_covs = meta['use_beta_priors_covs']
        self.covariates_file = meta['covariates_file']
        self.explore_sub_models = meta['explore_sub_models']
        self.use_weighted_errors = meta['use_weighted_errors']
        self.dropout_rate = meta['dropout_rate']
        self.use_existing_model = meta['use_existing_model']
        self.use_cross_validation = meta['use_cross_validation']
        self.use_spline_monotonicity = meta['use_spline_monotonicity']
        self.outputs_folder = meta['outputs_folder']
        self.same_se = meta['same_se']
        self.for_maggie = meta['for_maggie']

    def load_data(self, method_se='Wilson'):
        """Read and process the data."""
        df = pd.read_csv(self.i_file)
        # Inflating 0s
        df['meas_value'] = df['meas_value'].map(
            lambda x: 10**(-7) if x == 0 else x
        )
        # Drop infs
        df = df[df.meas_stdev != np.inf]
        df = df[df.super != 'none']
        df = df[~df.meas_stdev.isnull()]

        df['meas_stdev'] = pd.to_numeric(df['meas_stdev'])
        # Take logit
        df['logit_mean'] = logit(df.meas_value)
        # Delta method
        df['logit_se'] = df.meas_stdev/(df.meas_value * (1 - df.meas_value))
        # Create study_id
        df['study_id'] = df.apply(self.unique_study_id, axis=1)
        df['study_id'] = df['study_id'].map(lambda x: str(x))
        # Calculate median CD4; age_lower and age_upper is the
        # convention name from bradmod for exposure.
        df['cd4_lower'] = df['age_lower']*10
        df['cd4_upper'] = df['age_upper']*10
        df['cd4_mid'] = (df.cd4_lower + df.cd4_upper)/2

        # Drop values higher than a certain percentile of mortality at CD4 mid of 750
        cutoff = np.percentile(df.loc[df.cd4_mid == 750].meas_value, 10)
        df = df.loc[~((df.cd4_mid == 750) & (df.meas_value >= cutoff))]

        # Reformat duration time
        df['time_point'] = df['time_point'].map(
            lambda x: "time_0_6" if x == 6 else x
        )
        df['time_point'] = df['time_point'].map(
            lambda x: "time_7_12" if x == 12 else x
        )
        df['time_point'] = df['time_point'].map(
            lambda x: "time_13_24" if x == 24 else x
        )
        df['age'] = df['age'].map(lambda x: f"age_{x}")
        df['sex'] = df['sex'].map(lambda x: f"sex_{int(x)}")
        # To avoid name conflicts with ART duration time
        df = df.rename(columns={'time': 'year'})
        df.loc[:, 'mid_year'] = df.loc[:, 'year'].map(
            lambda x: np.mean([int(x.split('_')[0]), int(x.split('_')[1])])
        )
        # Encode categorical variables
        df = self.one_hot_encoding(df, 'time_point')
        df = self.one_hot_encoding(df, 'age')
        df = self.one_hot_encoding(df, 'sex')
        df = self.one_hot_encoding(df, 'super')
        self.df = df

        self.load_covariates()

        if self.use_covariates:
            if self.transform_covariates:
                self.standardize_covariates()

        self.df['super'] = self.df.apply(lambda x: 'high' if x['high'] == 1 else ('ssa' if x['ssa'] == 1 else 'other'), axis=1)

        if self.split_data:
            self.df, self.df_test = self.equal_proportion_train_test_split('super')
        else:
            self.df_test = pd.DataFrame()

        # Newly added after pediatric adaptation
        self.age_range = 'adult'
        self.SCATTER_SIZE = 1
        

    @staticmethod
    def describe_data(df, description=None):
        covs = ['high', 'other', 'ssa', 'sex_1', 'sex_2',
                'time_0_6', 'time_7_12', 'time_13_24',
                'age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100',
                'cd4_mid', 'logit_mean', 'logit_se']
        print(description)
        print(f"Number of observations: {len(df)}")
        print(f"Number of unique pubmed_id: {len(df.pubmed_id.unique())}")
        print(f"Number of unique study_id: {len(df.study_id.unique())}")
        print("Data description:")
        df.loc[:, covs].describe().to_csv("data_description.csv", index=True, mode='a')

    @staticmethod
    def train_test_split(df, train_size=0.8):
        """Split data into training and test set."""
        np.random.seed(10086)
        n_df = len(df)
        index = np.arange(n_df)
        np.random.shuffle(index)
        n_train = int(n_df * train_size)
        train_index = index[0: n_train]
        test_index = index[n_train:]
        return df.iloc[train_index,], df.iloc[test_index,]

    @staticmethod
    def specific_year_train_test_split(df):
        """Test specific data (eg. certain location or year)."""
        year = 2011.5 # One of [1998, 2011.5, 2008]
        df_test = df.query(f"mid_year=={year}")
        df_train = df.query(f"mid_year!={year}")
        return df_train, df_test

    def equal_proportion_train_test_split(self, col):
        """Split the data to reflect the same proportion of certain covariate"""
        dict_df = {'train': [], 'test': []}
        for value in self.df[col].unique():
            tmp = self.df.loc[self.df[col] == value]
            tmp_train, tmp_test = self.train_test_split(tmp, self.train_size)
            dict_df['train'].append(tmp_train)
            dict_df['test'].append(tmp_test)
        df_train = pd.concat(dict_df['train'])
        df_test = pd.concat(dict_df['test'])
        return df_train, df_test

    def standardize_covariates(self):
        """Standardize the covariates."""
 
        self.scaler = {}
        # Keep original covariates values
        for cov in self.covariates:
            self.df[f"{cov}_ori"] = self.df[f"{cov}"]
            self.scaler[cov] = StandardScaler().fit(self.df[cov].values.reshape(-1, 1))
            self.df[cov] = self.scaler[cov].transform(self.df[cov].values.reshape(-1, 1))

    def fit_models(self, region):
        """Fit MRBRT model."""
        self.region = region
        # if 'all', data for all three regions are fitted together.
        # if one region is specified, only data for that region are fitted.
        if region == 'all':
            self.df_region = self.df.append(self.df_test).copy()
            self.regions = ['high', 'ssa', 'other']
        else:
            self.df_region = self.df.loc[self.df.super == region]
            self.df_test = self.df_test.loc[self.df_test.super == region]
            self.regions = [self.region]

        if self.age_range == 'adult' and self.regions == ['ssa']:
             tmp = self.df_region.copy()

            print(tmp.loc[(tmp.pubmed_id == 29329301)])

            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==1) & (tmp.age_55_100==1) & (tmp.time_13_24==1)].meas_value, 90)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==1) & (tmp.age_55_100==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==1) & (tmp.age_45_55==1) & (tmp.time_13_24==1)].meas_value, 90)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==1) & (tmp.age_45_55==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==1) & (tmp.age_35_45==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==1) & (tmp.age_35_45==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==1) & (tmp.age_25_35==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==1) & (tmp.age_25_35==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==1) & (tmp.age_15_25==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==1) & (tmp.age_15_25==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]

            # Male
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==0) & (tmp.age_55_100==1) & (tmp.time_13_24==1)].meas_value, 90)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==0) & (tmp.age_55_100==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==0) & (tmp.age_45_55==1) & (tmp.time_13_24==1)].meas_value, 90)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==0) & (tmp.age_45_55==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==0) & (tmp.age_35_45==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==0) & (tmp.age_35_45==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==0) & (tmp.age_25_35==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==0) & (tmp.age_25_35==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]
            cutoff = np.percentile(tmp.loc[(tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                                  (tmp.sex_2==0) & (tmp.age_15_25==1) & (tmp.time_13_24==1)].meas_value, 95)
            tmp = tmp.loc[~((tmp.cd4_mid > 99) & (tmp.cd4_mid < 110) & 
                             (tmp.sex_2==0) & (tmp.age_15_25==1) & 
                             (tmp.time_13_24==1) & (tmp.meas_value >= cutoff))]

            tmp = tmp.loc[~(tmp.pubmed_id == 29329301)]

            tmp = tmp.loc[~((tmp.pubmed_id == 22296265) & (tmp.cd4_mid==350) & (tmp.meas_stdev < 0.007))]

            tmp = tmp.loc[~(tmp.pubmed_id==22296265)]


            tmp = tmp.loc[~((tmp.sex_2==0) & (tmp.age_15_25==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.04))]
            tmp = tmp.loc[~((tmp.sex_2==1) & (tmp.age_15_25==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.04))]

            tmp = tmp.loc[~((tmp.sex_2==0) & (tmp.age_25_35==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.04))]

            tmp = tmp.loc[~((tmp.sex_2==0) & (tmp.age_35_45==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.03))]

            tmp = tmp.loc[~((tmp.sex_2==1) & (tmp.age_35_45==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.04))]

            tmp = tmp.loc[~((tmp.sex_2==0) & (tmp.age_45_55==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.015))]

            tmp = tmp.loc[~((tmp.sex_2==1) & (tmp.age_45_55==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.015))]

            tmp = tmp.loc[~((tmp.sex_2==1) & (tmp.age_55_100==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.015))]

            tmp = tmp.loc[~((tmp.sex_2==0) & (tmp.age_55_100==1) & (tmp.time_13_24==1) & (tmp.meas_value > 0.02))]

            self.df_region = tmp.copy()

            


        self.fit_signal_model()
        if self.use_beta_monotonicity:
            self.fit_me_model_monotonicity()
        else:
            self.fit_me_model()

    def fit_signal_model(self, spline_degree=3):
        """Fit the signal model."""
        print("#############################################")
        print("Fit signal model ...")
        print("#############################################")
        self.data = MRData()
        
        # Scatterplot of logit(y) ~ CD4
        plt.figure(figsize=(12, 6))

        filtered_df = self.df_region
        plt.scatter((filtered_df['cd4_lower'] + filtered_df['cd4_upper'])/2,
            filtered_df['logit_mean'], color='b', label='y vs cd4_upper')
        plt.xlabel('cd4_mid')
        plt.ylabel('y')
        plt.title('y vs cd4_mid')
        plt.grid(True)
        plt.legend()

        plt.tight_layout()
        plt.savefig(os.path.join(self.o_folder, f"scatter_{self.age_range}_{self.region}.png"))
        
        self.df_region.loc[:, 'cd4_lower'] = self.df_region.cd4_lower.astype(float)
        self.df_region.loc[:, 'cd4_upper'] = self.df_region.cd4_upper.astype(float)
        self.df_region.loc[:, 'logit_se'] = self.df_region.logit_se.astype(float)

        self.data.load_df(
            self.df_region,
            col_obs='logit_mean',
            col_covs=['cd4_lower', 'cd4_upper'],
            col_obs_se='logit_se',
            col_study_id='study_id'
        )
        # The number of observations is the same if there's no missing values
        try:
            assert len(self.df_region) == len(self.data.to_df())
        except:
            print("There are missing values!")

        cov_intercept = LinearCovModel('intercept', use_re=False)

        if self.use_spline_monotonicity:
            cov_x = LinearCovModel(['cd4_lower', 'cd4_upper'], use_spline=True, spline_degree=2,
                                   spline_knots=np.linspace(0, 1, num=4),
                                   prior_spline_monotonicity='decreasing',
                                   spline_l_linear=True, spline_r_linear=True,
                                   use_re=False)
            cov_x = LinearCovModel(['cd4_lower', 'cd4_upper'], use_spline=True, spline_degree=2,
                                   spline_knots=np.linspace(0, 1, num=4),
                                   prior_spline_monotonicity='decreasing',
                                   spline_l_linear=True, spline_r_linear=True,
                                   use_re=False)
        else:
            cov_x = LinearCovModel(['cd4_lower', 'cd4_upper'], use_spline=True, spline_degree=3,
                                   spline_knots=np.linspace(0, 1, num=5),
                                   #    prior_spline_convexity='convex',
                                   use_re=False)
            

        cov_models = [cov_intercept, cov_x]


        self.signal_model = MRBRT(
            self.data,
            cov_models,
            inlier_pct=0.95
        )
        print(self.signal_model.data.to_df())
        tmp = self.signal_model.data.to_df()
        assert self.signal_model.data.to_df().isna().any().sum() == 0

        if self.age_range in ['under_five', 'over_five']:
            assert self.signal_model.data.to_df().applymap(np.isinf).any().sum() == 0

        print("#############################################")
        # try:
        self.signal_model.fit_model(inner_max_iter=5000, outer_max_iter=1000, inner_print_level=5)
        # except:
        print("Beta solution of signal model: ", self.signal_model.beta_soln)
        print(self.signal_model.data.to_df())
        self.df_region['trimming'] = self.signal_model.w_soln[np.argsort(
            self.data.data_id)]
        # Predict signal. The intercept is assigned to be 0 while predicting.
        pred_data = MRData()
        pred_df = self.df_region.copy()
        pred_data.load_df(
            pred_df,
            col_obs='logit_mean',
            col_covs=['cd4_lower', 'cd4_upper'],
            col_obs_se='logit_se',
            col_study_id='study_id'
        )
        # Assign signal to data for use in later stage
        # sort_by_data_id=True to be consistent with input dataframe.
        self.df_region['pred'] = self.signal_model.predict(
            pred_data, predict_for_study=False, sort_by_data_id=True)
        self.df_region['signal'] = self.df_region['pred'] - \
            self.signal_model.beta_soln[0]
        print(self.df_region.loc[:, ['logit_mean', 'pred', 'signal']])

        self.plot_signal_model()

    def plot_signal_model(self):
        """Plot the fit of signal model."""
        pred_df = pd.DataFrame()

        if self.age_range == 'over_five':
            pred_df['cd4_lower'] = np.arange(100, 1500, 100)
            pred_df['cd4_upper'] = np.arange(100, 1500, 100)
        elif self.age_range == 'under_five':
            pred_df['cd4_lower'] = np.arange(100, 1500, 100)
            pred_df['cd4_upper'] = np.arange(100, 1500, 100)
        else:
            pred_df['cd4_lower'] = np.arange(25, 800, 50)
            pred_df['cd4_upper'] = np.arange(25, 800, 50)
        pred_df['intercept'] = 1
        pred_data = MRData()
        pred_data.load_df(pred_df, col_covs=['cd4_lower', 'cd4_upper'])
        # Only keep the signal
        pred = self.signal_model.predict(pred_data) - \
            self.signal_model.beta_soln[0]
        pred = self.signal_model.predict(pred_data)
        obs = self.df_region.copy()
        # For the aesthetics of figure, drop mortality of 0.
        obs = obs.loc[obs.meas_value != 10**(-15), ]
        plt.scatter(obs.cd4_mid,
                    obs.logit_mean,
                    s=1/(self.SCATTER_SIZE*obs.logit_se**2),
                    alpha=0.5,
                    label="Observed")
        pred_df['cd4_mid'] = (pred_df['cd4_lower'] + pred_df['cd4_upper'])/2
        plt.plot(pred_df.cd4_mid, pred, color='r', label='Signal model')
        # Plot trimmed data point
        trimmed = obs.loc[obs.trimming == 0]
        plt.xlabel("CD4")
        plt.ylabel("Mortality (logit)")
        plt.legend()
        plt.savefig(os.path.join(self.o_folder,
                    f"{self.region}_signal_model.pdf"))
        plt.close()

        # CD4 vs residuals
        orig_data = MRData()
        orig_data.load_df(obs, col_covs=['cd4_lower', 'cd4_upper'])
        # Only keep the signal
        orig_pred = self.signal_model.predict(orig_data)
        plt.scatter(obs.cd4_mid, orig_pred - obs.logit_mean,
                    s=1/(self.SCATTER_SIZE*obs.logit_se**2),
                    alpha=0.5,
                    label="Observed")
        plt.xlabel("CD4")
        plt.ylabel("Residual (logit)")
        plt.legend()
        plt.savefig(os.path.join(self.o_folder,
                    f"{self.region}_residual_signal_model.pdf"))
        plt.close()

    def fit_me_model(self):
        """Fit mixed-effects model."""

        self.df_me = self.df_region.loc[:, ['logit_mean', 'logit_se', 'study_id', 'trimming', 'signal', 'iso3', 'year'] 
                                            + self.dem_covs
                                            + self.covariates]

        # Test adding an interaction term
        self.df_me['time_13_24_age_55_100'] = self.df_me['time_13_24'] * self.df_me['age_55_100']
        self.df_me['time_13_24_age_45_55'] = self.df_me['time_13_24'] * self.df_me['age_45_55']
        self.dem_covs.extend(['time_13_24_age_55_100', 'time_13_24_age_45_55'])

        # import pdb; pdb.set_trace()
        self.me_mrdata = MRData()
        self.me_mrdata.load_df(
            self.df_me.loc[self.df_me.trimming == 1],
            col_obs='logit_mean',
            col_covs=['signal'] + self.dem_covs + self.covariates,
            col_obs_se='logit_se',
            col_study_id='study_id'
        )
        cov_intercept = LinearCovModel('intercept', use_re=True)
        cov_signal = LinearCovModel('signal', use_re=False)

        if self.use_beta_priors_covs:
            cov_covariates = [
                    LinearCovModel(cov, use_re=False, prior_beta_laplace=np.array([0.0, 0.5])) 
                    for cov in self.covariates
                ]
        else:
            cov_covariates = [
                    LinearCovModel(cov, use_re=False) 
                    for cov in self.covariates
                ]

        # Specify beta priors
        if self.use_beta_priors:
            cov_models = [cov_intercept,
                          cov_signal,
                          LinearCovModel('sex_2'),
                          LinearCovModel('time_7_12',
                                         prior_beta_uniform=[-1, 0]),
                          LinearCovModel('time_13_24',
                                         prior_beta_uniform=[-1, 0]),
                          LinearCovModel('age_25_35',
                                         prior_beta_uniform=[0, 1]),
                          LinearCovModel('age_35_45',
                                         prior_beta_uniform=[0, 1]),
                          LinearCovModel('age_45_55',
                                         prior_beta_uniform=[0, 1]),
                          LinearCovModel('age_55_100',
                                         prior_beta_uniform=[0, 1])] + cov_covariates
        else:
            cov_models = [cov_intercept, cov_signal]
            for cov in self.dem_covs:
                cov_models.append(LinearCovModel(f'{cov}'))
            cov_models = cov_models + cov_covariates

        self.me_model = MRBRT(
            self.me_mrdata,
            cov_models,
            inlier_pct=1.0
        )

        self.me_model.fit_model(inner_max_iter=6000, inner_print_level=1)


    def fit_me_model_monotonicity(self):
        """Fit mixed-effects model with beta monotonicity."""
        self.df_me = self.df_region.loc[:, ['logit_mean', 'logit_se', 'study_id', 'time_7_12',
                                            'time_13_24', 'sex_2', 'age_25_35', 'age_35_45',
                                            'age_45_55', 'age_55_100', 'trimming', 'signal']]
        self.df_me['age_z1'] = self.df_me['age_25_35'] + self.df_me['age_35_45'] + \
            self.df_me['age_45_55'] + self.df_me['age_55_100']
        self.df_me['age_z2'] = self.df_me['age_35_45'] + \
            self.df_me['age_45_55'] + self.df_me['age_55_100']
        self.df_me['age_z3'] = self.df_me['age_45_55'] + \
            self.df_me['age_55_100']
        self.df_me['age_z4'] = self.df_me['age_55_100']
        self.df_me['time_z1'] = self.df_me['time_7_12'] + \
            self.df_me['time_13_24']
        self.df_me['time_z2'] = self.df_me['time_13_24']
        self.me_mrdata = MRData()
        self.me_mrdata.load_df(
            self.df_me.loc[self.df_me.trimming == 1],
            col_obs='logit_mean',
            col_covs=['signal', 'time_z1', 'time_z2', 'sex_2',
                      'age_z1', 'age_z2', 'age_z3', 'age_z4'],
            col_obs_se='logit_se',
            col_study_id='study_id'
        )
        cov_intercept = LinearCovModel('intercept', use_re=True)
        cov_signal = LinearCovModel('signal', use_re=False)
        # Specify beta priors
        cov_models = [cov_intercept,
                      cov_signal,
                      LinearCovModel('sex_2'),
                      LinearCovModel('time_z1', prior_beta_uniform=[-1, 0]),
                      LinearCovModel('time_z2', prior_beta_uniform=[-1, 0]),
                      LinearCovModel('age_z1', prior_beta_uniform=[0, 1]),
                      LinearCovModel('age_z2', prior_beta_uniform=[0, 1]),
                      LinearCovModel('age_z3', prior_beta_uniform=[0, 1]),
                      LinearCovModel('age_z4', prior_beta_uniform=[0, 1])]
        self.me_model = MRBRT(
            self.me_mrdata,
            cov_models,
            inlier_pct=1.0
        )

        self.me_model.fit_model(inner_max_iter=6000, inner_print_level=0)

    def design_dataframe(self, sex_cols, time_cols, age_cols, region_cols, dict_covariates, cd4_mid=None):
        """Create dataframe for prediction."""
        if cd4_mid is None:
            # To match old version of adult model
            cd4_mid = np.array([25, 75, 150, 225, 300, 425, 750])
        else:
            if self.for_maggie:
                # To match spectrum of pediatric model
                cd4_mid = np.array([100, 275, 425, 625, 875, 1250])
            else:
                cd4_mid = np.array(cd4_mid)

        sex = np.array(sex_cols)
        duration_time = np.array(time_cols)
        # If model age as continuous
        if len(age_cols) > 0:
            age = np.array(age_cols)
        else:
            age = np.arange(5, 15)

        region = np.array(region_cols)

        matrix_design = np.vstack(
            [np.tile(np.vstack(
                [np.tile(np.vstack(
                    [np.tile(np.vstack(
                        [np.tile(cd4_mid, len(sex)),
                         np.repeat(sex, len(cd4_mid))
                         ]), len(duration_time)),
                     np.repeat(duration_time, len(sex)*len(cd4_mid))
                     ]), len(age)),
                 np.repeat(age, len(sex)*len(cd4_mid)*len(duration_time))
                 ]), len(region)),
             np.repeat(region, len(age)*len(sex) *
                       len(cd4_mid)*len(duration_time))
             ]
        ).T
        df_design = pd.DataFrame(matrix_design,
                                 columns=['cd4_mid', 'sex', 'time_point', 'age_numeric', 'super'])

        df_design = OnART.one_hot_encoding(df_design, 'sex')
        df_design = OnART.one_hot_encoding(df_design, 'time_point')
        df_design = OnART.one_hot_encoding(df_design, 'super')
        if len(age_cols) > 0:
            df_design = OnART.one_hot_encoding(df_design, 'age_numeric')
            df_design = df_design.loc[:,
                                  ['cd4_mid'] + time_cols[1:] + sex_cols[1:] + age_cols[1:] + region_cols[1:]]
        elif len(age_cols) > 2:
            df_design = df_design.loc[:,
                                  ['cd4_mid'] + time_cols[1:] + sex_cols[1:] + ['age_numeric'] + region_cols[1:]]
            df_design['age_numeric'] = pd.to_numeric(df_design['age_numeric'])
        else:
            df_design = OnART.one_hot_encoding(df_design, 'age_numeric')
            df_design = df_design.loc[:,
                                  ['cd4_mid'] + time_cols[1:] + sex_cols[1:] + age_cols[1:] + region_cols[1:]]
        df_design['cd4_mid'] = pd.to_numeric(df_design['cd4_mid'])

        if cd4_mid is not None:

            if self.for_maggie:
                # For pediatric model
                cd4_mid_to_bound = {
                    100: (0, 200),
                    275: (200, 349),
                    425: (350, 499),
                    625: (500, 749),
                    875: (750, 999),
                    1250: (1000, 1500)
                }

                df_design['cd4_lower'] = df_design['cd4_mid'].map(lambda x: cd4_mid_to_bound[x][0])
                df_design['cd4_upper'] = df_design['cd4_mid'].map(lambda x: cd4_mid_to_bound[x][1])
            else:
                df_design['cd4_lower'] = 0
                df_design['cd4_upper'] = df_design['cd4_mid'] * 2
        else:
            # For adult model
            cd4_mid_to_bound = {
                25: (0, 50),
                75: (50, 100),
                150: (100, 200),
                225: (200, 250),
                300: (250, 350),
                425: (350, 500),
                750: (500, 1000)
            }

            df_design['cd4_lower'] = df_design['cd4_mid'].map(lambda x: cd4_mid_to_bound[x][0])
            df_design['cd4_upper'] = df_design['cd4_mid'].map(lambda x: cd4_mid_to_bound[x][1])

        df_design['intercept'] = 1
        
        for cov, value in dict_covariates.items():
            df_design[cov] = value
        return df_design

    def predict_signal(self, sex_cols=['sex_1', 'sex_2'], time_cols=['time_0_6', 'time_7_12', 'time_13_24'],
            age_cols=['age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100'],
            region_cols=['high', 'ssa', 'other'], cd4_mid=None, scenario_dict_covariates=None):
        """Predict the signal with fitted signal model."""
        dict_covariates = {}
        for cov in self.covariates:
            # Set the default value as 0 (the mean after standardization)
            dict_covariates.update({cov: 0})
        if scenario_dict_covariates is None:
            pass
        else:
            dict_covariates.update(scenario_dict_covariates)

        self.df_design = self.design_dataframe(sex_cols, time_cols, age_cols, region_cols, dict_covariates, cd4_mid)


        if 'haqi' in self.covariates:
            region_covs = self.region_specific_covariates()
            self.df_design = self.assign_region_covariates(self.transform_covariates, region_covs, self.df_design)
        if 'mid_year' in self.covariates:
            self.df_design['mid_year'] = 2022
        df_pred = self.df_design.copy()
        data_signal_pred = MRData()
        data_signal_pred.load_df(
            df_pred, col_covs=['cd4_lower', 'cd4_upper', 'intercept'])
        self.df_design['signal'] = self.signal_model.predict(data_signal_pred) - \
            self.signal_model.beta_soln[0]


    def get_coef_samples(self, num_samples=N_DRAWS):
        if np.isclose(self.me_model.gamma_soln[0], 0):
            self.me_model.gamma_soln = 0
            self.me_model.lt.gamma = np.array([0])
        self.beta_samples = sampling.sample_simple_lme_beta(num_samples, self.me_model)
        self.gamma_samples = np.repeat(self.me_model.gamma_soln, num_samples).reshape((num_samples, -1))

        with open(os.path.join(self.o_folder, f"beta_samples.pkl"), "wb") as f_write:
            dill.dump(self.beta_samples, f_write)

        with open(os.path.join(self.o_folder, f"gamma_samples.pkl"), "wb") as f_write:
            dill.dump(self.gamma_samples, f_write)

        # Extract beta coefficients and estimate lower and upper bounds
        columns = self.me_model.summary()[0].columns
        mean = np.mean(self.beta_samples, axis=0)
        upper = np.percentile(self.beta_samples, 97.5, axis=0)
        lower = np.percentile(self.beta_samples, 2.5, axis=0)
        df = pd.DataFrame(np.hstack([np.stack([mean, lower, upper]).transpose(), np.exp(np.stack([mean, lower, upper]).transpose())]),
            columns=['mean', 'lower', 'upper', 'mean_exp', 'lower_exp', 'upper_exp'], index=columns)
        df = df.iloc[1:]
        df.to_csv(os.path.join(self.o_folder, f"beta_coefficients_{self.region}.csv"), index=True)


    def predict_final(self):
        """Predict the final results."""
        self.df_pred = self.df_design.copy()
        # Test adding an interaction term

        self.df_pred['time_13_24_age_55_100'] = self.df_pred['time_13_24'] * self.df_pred['age_55_100']
        self.df_pred['time_13_24_age_45_55'] = self.df_pred['time_13_24'] * self.df_pred['age_45_55']

        if self.use_beta_monotonicity:
            self.df_pred['age_z1'] = self.df_pred['age_25_35'] + self.df_pred['age_35_45'] + \
                self.df_pred['age_45_55'] + self.df_pred['age_55_100']
            self.df_pred['age_z2'] = self.df_pred['age_35_45'] + \
                self.df_pred['age_45_55'] + self.df_pred['age_55_100']
            self.df_pred['age_z3'] = self.df_pred['age_45_55'] + \
                self.df_pred['age_55_100']
            self.df_pred['age_z4'] = self.df_pred['age_55_100']
            self.df_pred['time_z1'] = self.df_pred['time_7_12'] + \
                self.df_pred['time_13_24']
            self.df_pred['time_z2'] = self.df_pred['time_13_24']
            data_final_pred = MRData()
            data_final_pred.load_df(self.df_pred,
                                    col_covs=['signal', 'time_z1', 'time_z2', 'sex_2',
                                              'age_z1', 'age_z2', 'age_z3', 'age_z4'])
        else:
            data_final_pred = MRData()
            data_final_pred.load_df(self.df_pred,
                                    col_covs=['signal', 'time_7_12', 'time_13_24', 'sex_2',
                                              'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100',
                                              'time_13_24_age_55_100', 'time_13_24_age_45_55'] + \
                                              self.covariates)
        self.df_design['logit_preds'] = self.me_model.predict(data_final_pred)
        self.df_design['preds'] = expit(self.df_design['logit_preds'])

        self.y_draws = self.me_model.create_draws(data_final_pred, self.beta_samples,
                                             self.gamma_samples, random_study=True)
        self.df_design['logit_preds_lower'] = np.quantile(self.y_draws,
                                                          0.025,
                                                          axis=1)
        self.df_design['logit_preds_upper'] = np.quantile(self.y_draws,
                                                          0.975,
                                                          axis=1)
        self.df_design['preds_lower'] = expit(
            self.df_design['logit_preds_lower'])
        self.df_design['preds_upper'] = expit(
            self.df_design['logit_preds_upper'])


    def save_outputs(self, sex_cols=['sex_1', 'sex_2'], time_cols=['time_0_6', 'time_7_12', 'time_13_24'],
            age_cols=['age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100'],
            region_cols=['high', 'ssa', 'other']):
        """Save outputs for use."""
        df_output = self.df_design.copy()
        cols_draw = ['draw_{}'.format(i) for i in range(N_DRAWS)]
        df_output.loc[:, cols_draw] = expit(self.y_draws)

        def subset_output(df_output, query):
            subset = df_output.query(query)
            subset = subset.rename(columns={'cd4_mid': 'X_cd4_mid'})
            subset.loc[:, 'time'] = time_col.strip('time_')
            subset.loc[:, 'sex'] = sex_col.strip('sex_')
            subset.loc[:, 'age'] = age_col.strip('age_')
            subset = subset.loc[:, ['X_cd4_mid', 'time', 'sex', 'age'] + cols_draw]
            return subset

        for region in self.regions:
            if not Path(os.path.join(self.outputs_folder, self.version, region)).exists():
                os.mkdir(os.path.join(self.outputs_folder, self.version, region))

            for sex_col in sex_cols:
                for time_col in time_cols:
                    for age_col in age_cols:
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        subset = subset_output(df_output, query)
                        subset.to_csv(os.path.join(
                            self.outputs_folder,
                            self.version,
                            region,
                            f"{region}_{age_col.strip('age_')}_{sex_col.strip('sex_')}_{time_col.strip('time_')}.csv"),
                        index=False)

    def plot_90_model_fits(self, plot_space='linear'):
        """Plot model fits for 90 models."""
        df_models_90 = self.get_90_models_results()
        if self.split_data:
            df_obs = self.df.append(self.df_test)
        else:
            df_obs = self.df.copy()
        cols_shared = ['cd4_mid', 'super', 'age', 'time_point', 'sex']
        df_obs = df_obs.loc[:, cols_shared + ['meas_value', 'meas_stdev', 'logit_mean', 'logit_se']]
        df = df_models_90.merge(df_obs, on=cols_shared)
        if plot_space == 'linear':
            obs = df.meas_value.values
            fits = df.prob.values
            size = 1/(df.meas_stdev*5)
            df['se_inverse'] = size
        else:
            df = df.loc[df.logit_mean!=df.logit_mean.min()]
            obs = df.logit_mean.values
            fits = logit(df.prob.values)
            size = 1/(df.logit_se.values**2)
            df['se_inverse'] = size
        xmin = min(obs.min(), fits.min())
        xmax = max(obs.max(), fits.max())
        plt.figure()
        sns.scatterplot(data=df, x=obs, y=fits, size='se_inverse', alpha=0.15, hue='time_point',
                    sizes=(np.quantile(size, 0.25), np.max(size)))
        plt.plot(np.arange(xmin, xmax, 0.01),
                 np.arange(xmin, xmax, 0.01),
                 color='k')
        plt.title("Observed vs Fitted")
        plt.suptitle("Model fits for 90 models")
        plt.xlabel("Observations")
        plt.ylabel("Fitted")
        plt.savefig(os.path.join(self.o_folder, f"model_fits_90_models_{plot_space}.pdf"))
        plt.close()

        # Compare current results to 90 models
        df_current = self.df_design.copy()
        df_current = self.reverse_one_hot_encoding(df_current)
        df_current = df_current.loc[:, cols_shared + ['preds']]
        df = df_current.merge(df_models_90, on=cols_shared)
        xmin = min(df.prob.min(), df.preds.min())
        xmax = max(df.prob.max(), df.preds.max())
        plt.figure()
        sns.scatterplot(data=df, x='prob', y='preds', alpha=0.5, hue='time_point')
        plt.plot(np.arange(xmin, xmax, 0.01),
                 np.arange(xmin, xmax, 0.01),
                 color='k')
        plt.title("90 Models vs Current Model")
        plt.suptitle(f"{self.region}")
        plt.xlabel("90 Models")
        plt.ylabel("Current Model")
        plt.savefig(os.path.join(self.o_folder, f"{self.region}_90_models_vs_current.pdf"))
        plt.close()


    def plot_model_fits(self, suffix='', plot_space='logit'):
        """Plot model fits."""
        if plot_space == 'linear':
            fits = expit(self.me_model.predict(self.me_mrdata))
            obs = expit(self.me_mrdata.obs)
            obs_se = self.me_mrdata.obs_se*(obs*(1 - obs))
        else:
            fits = self.me_model.predict(self.me_mrdata)
            # fits = self.me_model.predict(self.me_mrdata, predict_for_study=False, sort_by_data_id=True)            
            obs = self.me_mrdata.obs
            obs_se = self.me_mrdata.obs_se
        size = 5 * (1/obs_se)
        size = 1/(5*obs_se**2)
        # In-sample
        df_plot = self.me_mrdata.to_df()
        df_plot['fits'] = fits
        df_plot['size'] = size

        plt.figure(figsize=(12, 18))
        plt.subplot(2, 1, 1)

        if self.same_se:
             # No size 
            if 'time_7_12' in self.dem_covs:
                df_plot['time'] = df_plot.apply(lambda x: self.label_time(x), axis=1)
                sns.scatterplot(data=df_plot, x=obs, y=fits, hue="time", alpha=0.15 )
            else:
                sns.scatterplot(data=df_plot, x=obs, y=fits, alpha=0.10, legend=False)
        else:
            if 'time_7_12' in self.dem_covs:
                df_plot['time'] = df_plot.apply(lambda x: self.label_time(x), axis=1)
                sns.scatterplot(data=df_plot, x=obs, y=fits, hue="time", size="size", alpha=0.15,
                    sizes=(np.quantile(size, 0.25), np.max(size)))
            else:
                sns.scatterplot(data=df_plot, x=obs, y=fits, size="size", alpha=0.10,
                    sizes=(np.quantile(size, 0.25), np.max(size)), legend=False)

        xmin = min(obs.min(), fits.min())
        xmax = max(obs.max(), fits.max()) 

        plt.plot(np.arange(xmin, xmax, 1),
                     np.arange(xmin, xmax, 1),
                     color='k')
        plt.title("Observed vs Fitted")
        plt.suptitle(f"{suffix}")
        plt.xlabel("Logit conditional probability (observed)")
        plt.ylabel("Logit conditional probability (fitted)")

        residuals = fits - obs


        plt.subplot(2, 1, 2)

        if self.same_se:
            # No size
            sns.scatterplot(data=df_plot, x=fits, y=residuals, alpha=0.10, legend=False)
        else:
            sns.scatterplot(data=df_plot, x=fits, y=residuals, size="size", alpha=0.95,
                sizes=(np.quantile(size, 0.35), 10*np.max(size)), legend=False)
            if self.region == 'high':
                plt.xlim(left=np.percentile(fits, 2.5), right=np.percentile(fits, 97.5))
            else:
                plt.xlim(left=np.percentile(fits, 2.5), right=np.percentile(fits, 97.5))
            plt.ylim([-2, 2])
       

        plt.xlabel('Logit conditional probability (predicted)')
        plt.ylabel('Residuals')
        plt.title('Residuals vs Predictions')
        plt.axhline(y=0, color='r', linestyle='--')

        plt.savefig(os.path.join(self.o_folder, f"model_fits_{self.age_range}_{self.region}.pdf"))
        plt.close()

        obs = self.me_mrdata.obs
        draws = self.me_model.create_draws(self.me_mrdata, self.beta_samples, self.gamma_samples,
            random_study=True)

        for percent in  [95]: #[10, 30, 50, 80, 90, 95]:
            alpha = 100 - percent
            lower = np.percentile(draws, alpha/2, axis=1)
            upper = np.percentile(draws, 100 - alpha/2, axis=1)
            df = pd.DataFrame({'obs': obs, 'lower': lower, 'upper': upper})
            df['is_covered'] = df.apply(lambda row: row['obs'] >= row['lower'] and row['obs'] <= row['upper'], axis=1)
            coverage = [percent, df.is_covered.mean(), df.loc[df.obs > -5].is_covered.mean()]
            coverage = np.round(coverage, 3)
            print(coverage)

        coverage = pd.DataFrame({'percent': percent, 'all': df.is_covered.mean(),
            'part': df.loc[df.obs > -5].is_covered.mean()}, index=[0])
        coverage.to_csv(os.path.join(self.o_folder, f"model_coverage_{self.age_range}_{self.region}.csv"), index=False)

    def plot_out_of_sample(self, plot_space='logit'):
        """Plot out of sample predictions versus observations. CODE TO BE CLEANED"""
        ###
        test_data = MRData()
        pred_df = self.df_region.copy()
        test_data.load_df(
            self.df_test,
            col_obs='logit_mean',
            col_covs=['cd4_lower', 'cd4_upper'],
            col_obs_se='logit_se',
            col_study_id='study_id'
        )
        # Assign signal to data for use in later stage
        # sort_by_data_id=True to be consistent with input dataframe.
        self.df_test['pred'] = self.signal_model.predict(
            test_data, predict_for_study=False, sort_by_data_id=True)
        self.df_test['signal'] = self.df_test['pred'] - \
            self.signal_model.beta_soln[0]

        self.df_test = self.df_test.loc[:, ['logit_mean', 'logit_se', 'meas_value', 'meas_stdev', 
                                            'study_id', 'signal', 'cd4_mid', 'super'] + self.dem_covs
                                            + self.covariates]
        if 'ssa' in self.df_test.columns or 'other' in self.df_test.columns:
            pass
        else:
            # One hot encoding 'super' to add covariates of region
            self.df_test = OnART.one_hot_encoding(self.df_test, 'super')
        self.me_test_data = MRData()
        # If region is included as covariate
        if len(self.regions) == 3:
            self.me_test_data.load_df(
                self.df_test,
                col_obs='logit_mean',
                col_covs=['signal', 'ssa', 'other'] + self.dem_covs + self.covariates,
                col_obs_se='logit_se',
                col_study_id='study_id'
            )
        else:
            # If region-specific model
            self.me_test_data.load_df(
                self.df_test,
                col_obs='logit_mean',
                col_covs=['signal'] + self.dem_covs + self.covariates,
                col_obs_se='logit_se',
                col_study_id='study_id'
            )

        ###
        self.df_test['logit_preds'] = self.me_model.predict(self.me_test_data)

        if plot_space == 'linear':
            fits = expit(self.df_test.logit_preds)
            obs = expit(self.me_test_data.obs)
            # Calculate back the SE in normal space for visualization
            obs_se = self.me_test_data.obs_se*(obs*(1 - obs))
        else:
            fits = self.df_test.logit_preds
            obs = self.me_test_data.obs
            # Calculate back the SE in normal space for visualization
            obs_se = self.me_test_data.obs_se
        size = 5 * (1/obs_se)
        plt.figure()
        plt.scatter(obs, fits, s=size, alpha=0.15)
        xmin = min(obs.min(), fits.min())
        xmax = max(obs.max(), fits.max())
        plt.plot(np.arange(xmin, xmax, 0.01),
                 np.arange(xmin, xmax, 0.01),
                 color='k')
        plt.title("Observed vs Predicted")
        plt.xlabel("Observations")
        plt.ylabel("Predicted")
        plt.savefig(os.path.join(self.o_folder, f"model_fits_out_of_sample_{self.region}.pdf"))


    @staticmethod
    def load_gbd_loc():
        """Load GBD location table."""
        gbd_loc_file = "FILEPATH/gbd_loc_table.csv"
        df_loc = pd.read_csv(gbd_loc_file)
        return df_loc

    def load_covariates(self):
        """Load country specific covariates for prediction."""
        df_covs = pd.read_csv(self.covariates_file)
        df_loc = self.load_gbd_loc()
        # 204 locations for level 3
        locs = df_loc.loc[df_loc.level==3,['location_name']].location_name.unique()
        df_covs = df_covs.loc[
            df_covs.location_name.isin(locs), 
            ['location_name', 'year_id', 'age_group_id', 'sex_id',
            'mean_value', 'lower_value', 'upper_value']
        ]
        self.df_covs = df_covs

    @staticmethod
    def identify_region(location_name):
        """Return the region to which the location belongs.
           region include: ssa, high, other.
        """
        df_loc = OnART.load_gbd_loc()
        df_loc = df_loc.loc[:,['location_name', 'super_region_name']]
        dict_loc = df_loc.set_index('location_name')['super_region_name'].to_dict()
        return dict_loc[location_name]

    def evaluate_model(self):
        """Calculate fitted errors."""
        df_train = self.df_me.copy()
        df_test = self.df_test.copy()
        df_train['logit_preds'] = self.me_model.predict(self.me_mrdata)
        weights_train = 1/(df_train['logit_se']**2)
        weights_test =  1/(df_test['logit_se']**2)
        # To be consistent with DNN implementation
        train_mse_weighted = mean_squared_error(df_train['logit_mean'], df_train['logit_preds'],
            sample_weight = weights_train)
        train_mae_weighted = mean_absolute_error(df_train['logit_mean'], df_train['logit_preds'],
            sample_weight = weights_train)
        test_mse_weighted = mean_squared_error(df_test['logit_mean'], df_test['logit_preds'],
            sample_weight = weights_test)
        test_mae_weighted = mean_absolute_error(df_test['logit_mean'], df_test['logit_preds'],
            sample_weight = weights_test)

        print(f"Root mean squared error for training: {train_mse_weighted}")
        print(f"Mean absolute error for training: {train_mae_weighted}")
        print(f"Root mean squared error for test: {test_mse_weighted}")
        print(f"Mean absolute error for test: {test_mae_weighted}")
        return np.round((train_mse_weighted, train_mae_weighted,
                         test_mse_weighted, test_mae_weighted), 3)

    def save_model_errors(self):
        # Save the covariates and their errors in a dataframe.
        train_mse_weighted, train_mae_weighted, test_mse_weighted, test_mae_weighted = self.evaluate_model()
        errors = {
            'model': self.version,
            'train_MSE_weighted': [train_mse_weighted],
            'train_MAE_weighted': [train_mae_weighted],
            'test_MSE_weighted': [test_mse_weighted],
            'test_MAE_weighted': [test_mae_weighted]
        }
        df_err = pd.DataFrame(errors)
        if self.region is None:
            o_file = f"{self.version}_model_errors.csv"
        else:
            o_file = f"{self.version}_{self.region}_model_errors.csv"
        df_err.to_csv(os.path.join(self.o_folder, o_file), index=False)

    def subset_predictions(self):
        """Subset the predictions only for regions targetted,
           since all regions were predicted."""
        region_dict = {'ssa': 0, 'other': 0}
        lst_df = []
        for region in self.regions:
            if region in region_dict:
                region_dict.update({region: 1})
            query = ' and '.join(
                [f'{k} == {repr(v)}' for k, v in region_dict.items()])
            subset = self.df_design.query(query)
            lst_df.append(subset)
            region_dict = {'ssa': 0, 'other': 0}
        self.df_design = pd.concat(lst_df)

    def plot_design_predictions(self, plot_space='linear'):
        """Plot prediction of design matrix and scatterplot of observed"""
        self.plot_space = plot_space
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55', 'age_55_100']

        self.single_page_plots(sex, time, age)
        self.one_page_plots()

    def scenarios_plots(self, cov):
        """Plot predictions varying a covariate level."""
        pp = PdfPages(os.path.join(self.o_folder, f"scenarios_plots_{cov}.pdf"))
        lst_scenarios = [{f'{cov}': 0}, {f'{cov}': 1}, {f'{cov}': -1}]
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55', 'age_55_100']

        dict_df = {}
        for ix, dict_covariates in enumerate(lst_scenarios):
            self.predict_final(dict_covariates)
            df_plot = self.df_design.copy()
            value = dict_covariates[f'{cov}']
            dict_df.update({f'{cov}_{value}': df_plot})

        for region in self.regions:
            for sex_col in sex:
                for time_col in time:
                    for age_col in age:
                        query = self.create_query(sex_col, time_col, age_col, region)
                        for key, df_plot in dict_df.items():
                            pred = df_plot.query(query)
                            sd_score = int(key.strip(f'{cov}_'))
                            value = self.scaler[cov].inverse_transform(np.array([sd_score]).reshape(-1,1)).flatten()[0]
                            if value > 1:
                                value = np.round(value, 0)
                            else:
                                value = np.round(value, 2)
                            plt.plot(pred.cd4_mid, pred.preds, label=f"{cov}: {value}")
                        plt.title(f"Region: {region}; Sex: {sex_col.strip('sex_')}; "
                                  f"Age: {age_col.strip('age_')}; Time: {time_col.strip('time_')}")
                        plt.xlabel("cd4_mid")
                        plt.ylim(bottom=0)
                        plt.legend()
                        pp.savefig()
                        plt.close()
        pp.close()

    def single_page_plots(self, sex, time, age):
        """One plot for each subset group."""

        for region in self.regions:
            pp = PdfPages(os.path.join(self.o_folder, f"{region}.pdf"))

            for sex_col in sex:
                for time_col in time:
                    for age_col in age:
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        pred = self.df_design.query(query)
                        obs = self.df_region.query(query)
                        # Not showing mortality of zeros
                        obs = obs.loc[obs.meas_value != 10**(-15), ]

                        plt.figure()

                        if self.plot_space == 'linear':
                            plt.scatter(obs.cd4_mid, obs.meas_value)
                            plt.plot(pred.cd4_mid, pred.preds)
                            plt.ylim(bottom=0)
                        elif self.plot_space == 'logit':
                            plt.scatter(obs.cd4_mid, obs.logit_mean,
                                        s=1/(self.SCATTER_SIZE*obs.logit_se))
                            plt.plot(pred.cd4_mid, pred.logit_preds)
                            plt.fill_between(pred.cd4_mid,
                                             pred.logit_preds_lower,
                                             pred.logit_preds_upper,
                                             color='b',
                                             alpha=0.3)
                            trimmed = obs.loc[obs.trimming == 0]
                            plt.scatter(trimmed.cd4_mid,
                                        trimmed.logit_mean,
                                        marker='x')
                        plt.title(f"Region: {self.region}; Sex: {sex_col.strip('sex_')}; "
                                  f"Age: {age_col.strip('age_')}; Time: {time_col.strip('time_')}")
                        plt.xlabel("cd4_mid")
                        pp.savefig()
                        plt.close()
            pp.close()

    def one_page_plots(self):
        """Plot all subsets for one region in one page."""
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55', 'age_55_100']
        colors = ['b', 'r']

        for region in self.regions:
            fig, axes = plt.subplots(
                figsize=(12, 12), nrows=3, ncols=5, sharex=True, sharey=True)
            for i, time_col in enumerate(time):
                for j, age_col in enumerate(age):
                    for k, sex_col in enumerate(sex):
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        pred = self.df_design.query(query)
                        obs = self.df_region.query(query)
                        # Not showing mortality of zeros
                        obs = obs.loc[obs.meas_value != 10**(-15), ]
                        if self.plot_space == 'linear':
                            axes[i][j].scatter(obs.cd4_mid, obs.meas_value,
                                               color=colors[k], alpha=0.5)
                            axes[i][j].plot(
                                pred.cd4_mid, pred.preds, color=colors[k])
                        elif self.plot_space == 'logit':
                            axes[i][j].scatter(obs.cd4_mid, obs.logit_mean,
                                               color=colors[k], alpha=0.5,
                                               s=1/(self.SCATTER_SIZE*obs.logit_se))
                            axes[i][j].plot(
                                pred.cd4_mid, pred.logit_preds, color=colors[k])
                            axes[i][j].fill_between(pred.cd4_mid,
                                                    pred.logit_preds_lower,
                                                    pred.logit_preds_upper,
                                                    color=colors[k], alpha=0.5)
            # Title
            for j, age_col in enumerate(age):
                axes[0][j].set_title(age_col.strip("age_"))

            # y-label on the right
            for i, time_col in enumerate(time):
                axes[i][4].yaxis.set_label_position("right")
                axes[i][4].yaxis.tick_right()
                axes[i][4].set_ylabel(time_col.strip("time_"))
            plt.suptitle(f"{region}", fontsize=15)
            fig.tight_layout()
            plt.savefig(os.path.join(self.o_folder,
                        f"{region}_aggregate.pdf"))
            plt.close()

    def one_page_plots_source(self, age_cols=['age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100']):
        """Plot all subsets for one region in one page."""
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_0_12', 'time_0_24']
        colors = ['b', 'r']

        for region in self.regions:
            if self.age_range == 'under_five':
                time = ['time_0_6', 'time_0_12']
            fig, axes = plt.subplots(
                figsize=(12, 12), nrows=len(time), ncols=len(age_cols), sharex=True, sharey=True)
            for i, time_col in enumerate(time):
                for j, age_col in enumerate(age_cols):
                    for k, sex_col in enumerate(sex):
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        pred = self.df_design_no_split.query(query)
                        obs = self.df_region.query(query)

                        # Not showing mortality of zeros
                        obs = obs.loc[obs.meas_value != 10**(-15), ]
                        if self.plot_space == 'linear':
                            axes[i][j].scatter(obs.cd4_mid, obs.meas_value,
                                               color=colors[k], alpha=0.5)
                            axes[i][j].plot(
                                pred.cd4_mid, pred.preds, color='k')
                        elif self.plot_space == 'logit':
                            axes[i][j].scatter(obs.query("source=='systematic'").cd4_mid,
                                               obs.query("source=='systematic'").logit_mean,
                                               color='g', alpha=0.5,
                                               s=1/(self.SCATTER_SIZE*obs.query("source=='systematic'").logit_se**2),
                                               label='Systematic')
                            axes[i][j].scatter(obs.query("source=='IDEA'").cd4_mid,
                                               obs.query("source=='IDEA'").logit_mean,
                                               color='y', alpha=0.5,
                                               s=1/(self.SCATTER_SIZE*obs.query("source=='IDEA'").logit_se**2),
                                               label='IDEA')
                            axes[i][j].plot(
                                pred.cd4_mid, pred.logit_preds, color=colors[k])
                            axes[i][j].fill_between(pred.cd4_mid,
                                                    pred.logit_preds_lower,
                                                    pred.logit_preds_upper,
                                                    color=colors[k], alpha=0.5)
            # Title
            age_titles = {'age_0_019_0_077': 'Late Neonatal', 'age_0_077_0_501': '1-5 months',
                          'age_0_501_1': '6-11 months', 'age_1_2': '12 to 23 months', 'age_2_4': '2 to 4',
                          'age_5_9': '5 to 9', 'age_10_14': '10 to 14', 'age_0_0_019': 'Early Neonatal'}
            for j, age_col in enumerate(age_cols):
                axes[0][j].set_title(age_titles[age_col])

            right_index = len(age_cols) - 1
            for i, time_col in enumerate(time):
                axes[i][right_index].yaxis.set_label_position("right")
                axes[i][right_index].yaxis.tick_right()
                axes[i][right_index].set_ylabel(time_col.strip("time_").replace('_', '-') + ' months')

            abbre_to_region = {v: k for k, v in REGIONS_DICT.items()}
            title = abbre_to_region[region]

            plt.suptitle(f"{title}", fontsize=15)
            fig.tight_layout()
            plt.savefig(os.path.join(self.o_folder,
                        f"{region}_aggregate_source.pdf"))
            plt.close()
        print("Plots for 0-6, 0-12, 0-24 saved at:", os.path.join(self.o_folder, f"{region}_aggregate_source.pdf"))

    def one_page_plots_predictions(self, age_cols=['age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100']):
        """Plot all subsets for one region in one page."""
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_0_12', 'time_0_24']
        colors = ['b', 'r']

        for region in self.regions:
            if self.age_range == 'under_five':
                time = ['time_0_6', 'time_0_12']
            else:
                time = ['time_0_6', 'time_0_24']
             fig, axes = plt.subplots(
                figsize=(12, 12), nrows=len(time), ncols=len(age_cols), sharex=True)
            for i, time_col in enumerate(time):
                for j, age_col in enumerate(age_cols):
                    for k, sex_col in enumerate(sex):
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                       
                        obs = self.df_region.query(query)

                        if self.present_mortality:
                            pred = self.df_design_mort.query(query)\
                                   .drop_duplicates(['cd4_mid', 'time_0_12', 'time_0_24', 'sex_2', 'preds'])
                       
                            pred['preds'] = pred['preds'] * 100
                            pred['preds_lower'] = pred['preds_lower'] * 100
                            pred['preds_upper'] = pred['preds_upper'] * 100
                        else:
                            pred = self.df_design.query(query)\
                                   .drop_duplicates(['cd4_mid', 'time_0_12', 'time_0_24', 'sex_2', 'preds'])

                        # Not showing mortality of zeros
                        obs = obs.loc[obs.meas_value != 10**(-15), ]
                        if self.plot_space == 'linear':
                            axes[i][j].plot(
                                pred.cd4_mid, pred.preds, color=colors[k])
                            axes[i][j].fill_between(pred.cd4_mid,
                                                    pred.preds_lower,
                                                    pred.preds_upper,
                                                    color=colors[k], alpha=0.5)

                            
                            if self.age_range == 'under_five':
                                if self.region == 'ssa':
                                    axes[1,0].set_ylim(bottom=0, top=25)
                                    axes[1,1].set_ylim(bottom=0, top=25)
                                    axes[1,2].set_ylim(bottom=0, top=25)
                                    axes[1,3].set_ylim(bottom=0, top=25)

                                    axes[0,0].set_ylim(bottom=0, top=120)
                                    axes[0,1].set_ylim(bottom=0, top=120)
                                    axes[0,2].set_ylim(bottom=0, top=120)
                                    axes[0,3].set_ylim(bottom=0, top=120)
                                else:
                                    axes[1,0].set_ylim(bottom=0, top=30)
                                    axes[1,1].set_ylim(bottom=0, top=30)
                                    axes[1,2].set_ylim(bottom=0, top=30)
                                    axes[1,3].set_ylim(bottom=0, top=30)

                                    axes[0,0].set_ylim(bottom=0, top=200)
                                    axes[0,1].set_ylim(bottom=0, top=200)
                                    axes[0,2].set_ylim(bottom=0, top=200)
                                    axes[0,3].set_ylim(bottom=0, top=200)

                            else:
                                if self.region == 'ssa':
                                    axes[1,0].set_ylim(bottom=0, top=10)
                                    axes[1,1].set_ylim(bottom=0, top=10)

                                    axes[0,0].set_ylim(bottom=0, top=50)
                                    axes[0,1].set_ylim(bottom=0, top=50)
                                else:
                                    axes[1,0].set_ylim(bottom=0, top=25)
                                    axes[1,1].set_ylim(bottom=0, top=25)

                                    axes[0,0].set_ylim(bottom=0, top=140)
                                    axes[0,1].set_ylim(bottom=0, top=140)

                        elif self.plot_space == 'logit':
                            axes[i][j].scatter(obs.cd4_mid, obs.logit_mean,
                                               color=colors[k], alpha=0.5,
                                               s=1/(self.SCATTER_SIZE*obs.logit_se**2))
                            axes[i][j].plot(
                                pred.cd4_mid, pred.logit_preds, color=colors[k])
                            axes[i][j].fill_between(pred.cd4_mid,
                                                    pred.logit_preds_lower,
                                                    pred.logit_preds_upper,
                                                    color=colors[k], alpha=0.5)
                        # Set the tick label size
                        plt.tick_params(axis='both', which='major', labelsize=12)  # Change 14 to desired size
            # Title
            age_titles = {'age_0_019_0_077': 'Late Neonatal', 'age_0_077_0_501': '1-5 months',
                          'age_0_501_1': '6-11 months', 'age_1_2': '12 to 23 months', 'age_2_4': '2 to 4 years',
                          'age_5_9': '5 to 9 years', 'age_10_14': '10 to 14 years', 'age_0_0_019': 'Early Neonatal'}
            for j, age_col in enumerate(age_cols):

                # axes[0][j].set_title(age_col.strip("age_"))
                axes[0][j].set_title(age_titles[age_col], fontsize=13)

            right_index = len(age_cols) - 1
            if self.age_range == 'under_five':
                time_titles = {'time_0_6': '0 to 6 months', 'time_0_12': '7 to 12 months', 'time_0_24': '13 to 24 months'}
            else:
                time_titles = {'time_0_6': '0 to 6 months', 'time_0_12': '0 to 12 months', 'time_0_24': '13 to 24 months'}
            for i, time_col in enumerate(time):
                axes[i][right_index].yaxis.set_label_position("right")
                axes[i][right_index].set_ylabel(time_titles[time_col], fontsize=13)
            

            # Show x label
            axes[1][0].set_xlabel("CD4 Counts (cells/mm^3)", fontsize=13)
            axes[1][1].set_xlabel("CD4 Counts (cells/mm^3)", fontsize=13)

            if self.age_range == 'under_five':
                axes[1][2].set_xlabel("CD4 Counts (cells/mm^3)", fontsize=13)
                axes[1][3].set_xlabel("CD4 Counts (cells/mm^3)", fontsize=13)


            # Show y label for first column
            axes[0][0].set_ylabel("Mortality rates (per 100 person years)", fontsize=13)
            axes[1][0].set_ylabel("Mortality rates (per 100 person years)", fontsize=13)

            abbre_to_region = {v: k for k, v in REGIONS_DICT.items()}
            title = abbre_to_region[region]

            plt.suptitle(f"{title}", fontsize=15)
            
            fig.tight_layout()
            plt.savefig(os.path.join(self.o_folder,
                        f"{region}_aggregate_predictions.pdf"))
            plt.close()
        print("Plots for 0-6, 7-12, 13-24 saved at:", os.path.join(self.o_folder, f"{region}_aggregate_predictions.pdf"))

    def one_page_plots_out_of_sample(self, age_cols):
        """Plot all subsets for one region in one page."""
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        colors = ['b', 'r']

        for region in self.regions:
            fig, axes = plt.subplots(
                figsize=(12, 12), nrows=len(time), ncols=len(age_cols), sharex=True, sharey=True)
            for i, time_col in enumerate(time):
                for j, age_col in enumerate(age_cols):
                    for k, sex_col in enumerate(sex):
                        # Region-specific model only has one region already
                        if len(self.regions) == 1:
                            query = self.create_query(sex_col, time_col, age_col)
                        else:
                            query = self.create_query(
                                sex_col, time_col, age_col, region)
                        pred = self.df_test.query(query)
                        obs = self.df_test.query(query)
                        # Not showing mortality of zeros
                        if self.plot_space == 'linear':
                            axes[i][j].scatter(obs.cd4_mid, obs.meas_value,
                                               color=colors[k], alpha=0.5)
                            axes[i][j].plot(
                                pred.cd4_mid, pred.preds, color=colors[k])
                        elif self.plot_space == 'logit':
                            axes[i][j].scatter(obs.cd4_mid, obs.logit_mean,
                                               color=colors[k], alpha=0.5,
                                               s=1/(self.SCATTER_SIZE*obs.logit_se**2))
                            axes[i][j].scatter(pred.cd4_mid, pred.logit_preds, color=colors[k], marker='x')
            # Title
            for j, age_col in enumerate(age_cols):
                axes[0][j].set_title(age_col)

            # y-label on the right
            for i, time_col in enumerate(time):
                axes[i][len(age_cols)-1].yaxis.set_label_position("right")
                axes[i][len(age_cols)-1].yaxis.tick_right()
                axes[i][len(age_cols)-1].set_ylabel(time_col.strip("time_"))
            plt.suptitle(f"{region}", fontsize=15)
            fig.tight_layout()
            plt.savefig(os.path.join(self.o_folder,
                        f"{region}_aggregate_out_of_sample.pdf"))
            plt.close()

    def visualize_monotonicity(self, age_cols=['age_15_25', 'age_25_35', 'age_35_45', 'age_45_55', 'age_55_100']):
        """One plot for each subset group."""
        df_plot = self.df_design.copy()
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        colors = ['r', 'b', 'm', 'g', 'y', 'k', 'c', 'aquamarine', 'mediumseagreen', 'xkcd:eggshell']
        pp = PdfPages(os.path.join(self.o_folder, f"visualize_monotonicity.pdf"))
        for region in self.regions:
            for time_col in time:
                col_dict = {'time_7_12': 0, 'time_13_24': 0,
                            'ssa': 0, 'other': 0}
                for col in [region, time_col]:
                    if col in col_dict:
                        col_dict.update({col: 1})
                query = ' and '.join(
                    [f'{k} == {repr(v)}' for k, v in col_dict.items()])
                pred = df_plot.query(query)
                col_dict = {}
                for age in age_cols[1:]:
                    col_dict.update({age: 0})
                # Visualize the prediction of different age groups across sex
                # to confirm the beta monotonicity.
                plt.figure()
                for ix, age_col in enumerate(age_cols[1:]):
                    col_dict.update({age_col: 1})
                    query = ' and '.join([f'{k} == {repr(v)}' for k, v in col_dict.items()])
                    sub_pred = pred.query(query)
                    plt.plot(sub_pred.loc[sub_pred.sex_2 == 0].cd4_mid,
                             sub_pred.loc[sub_pred.sex_2 == 0].preds, '--',
                             color=colors[ix])
                    plt.plot(sub_pred.loc[sub_pred.sex_2 == 1].cd4_mid,
                             sub_pred.loc[sub_pred.sex_2 == 1].preds, '-',
                             label=age_col, color=colors[ix])
                    col_dict.update({age_col: 0})
                # Plot age_15_25
                query = ' and '.join([f'{k} == {repr(v)}' for k, v in col_dict.items()])
                sub_pred = pred.query(query)
                plt.plot(sub_pred.loc[sub_pred.sex_2 == 0].cd4_mid,
                         sub_pred.loc[sub_pred.sex_2 == 0].preds, '--',
                         color=colors[ix+1])
                plt.plot(sub_pred.loc[sub_pred.sex_2 == 1].cd4_mid,
                         sub_pred.loc[sub_pred.sex_2 == 1].preds, '-',
                         label=age_cols[0], color=colors[ix+1])
                plt.title(f"Region: {region}; Time: {time_col.strip('time_')}")
                plt.xlabel("cd4_mid")
                plt.legend()
                pp.savefig()
                plt.close()
        pp.close()

    def load_gbd_results(self):
        """Load GBD 2017 BRADMOD RESULTS"""
        ssa_gbd_file = os.path.join(self.gbd_folder_brad_models, 'ssa.csv')
        other_gbd_file = os.path.join(self.gbd_folder_brad_models, 'other.csv')
        high_gbd_file = os.path.join(self.gbd_folder_brad_models, 'high.csv')
        # Read data
        ssa = pd.read_csv(ssa_gbd_file)
        ssa['super'] = 'ssa'
        other = pd.read_csv(other_gbd_file)
        other['super'] = 'other'
        high = pd.read_csv(high_gbd_file)
        high['super'] = 'high'
        # Concatenate three regions
        gbd = pd.concat([ssa, other, high])
        gbd['time_point'] = "0_6"
        gbd.loc[gbd.durationart == "6to12Mo", 'time_point'] = "7_12"
        gbd.loc[gbd.durationart == "GT12Mo", 'time_point'] = "13_24"
        gbd['age'] = gbd['age'].map(
            lambda x: "{}_{}".format(x.split('-')[0], x.split('-')[1]))
        draws = ['mort{}'.format(i) for i in range(1, 1001)]
        gbd['prob'] = np.mean(gbd.loc[:, draws], axis=1)
        gbd['prob_lo'] = np.quantile(gbd.loc[:, draws], 0.025, axis=1)
        gbd['prob_hi'] = np.quantile(gbd.loc[:, draws], 0.975, axis=1)
        gbd['source'] = "GBD17"
        gbd['cd4_lower'] = gbd['cd4_lower'].map(lambda x: 25 if x == 0 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(lambda x: 75 if x == 50 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(
            lambda x: 150 if x == 100 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(
            lambda x: 225 if x == 200 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(
            lambda x: 300 if x == 250 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(
            lambda x: 425 if x == 350 else x)
        gbd['cd4_lower'] = gbd['cd4_lower'].map(
            lambda x: 750 if x == 500 else x)
        gbd = gbd.rename({'cd4_lower': 'X_cd4_mid'}, axis=1)
        gbd['sex'] = gbd['sex'].astype(str)
        self.gbd_df = gbd.loc[:, ['sex', 'age', 'super', 'time_point', 'X_cd4_mid',
                                  'prob', 'prob_lo', 'prob_hi', 'source']]

    def load_90_models_results(self, sex_col, time_col, age_col, region):
        """Load 90 models results"""
        sex = sex_col.strip('sex_')
        age = age_col.strip('age_')
        time = time_col.strip('time_')
        if time == '13_24':
            time = '12_24'
        infile = f"{FILEPATH}/{region}_res/{region}_{age}_{sex}_{time}.csv"
        gbd = pd.read_csv(infile)
        if 'prob' not in gbd.columns:
            draws = ['draw_{}'.format(i) for i in range(N_DRAWS)]
            gbd['prob'] = np.mean(gbd.loc[:, draws], axis=1)
            gbd['prob_lo'] = np.quantile(gbd.loc[:, draws], 0.025, axis=1)
            gbd['prob_hi'] = np.quantile(gbd.loc[:, draws], 0.975, axis=1)
        gbd = gbd.loc[:, ['X_cd4_mid', 'prob', 'prob_lo', 'prob_hi']]
        return gbd

    def get_90_models_results(self):
        """Concatenate all 90 models results."""
        regions = ['ssa', 'high', 'other']
        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55', 'age_55_100']
        lst = []
        for region in regions:
            for sex_col in sex:
                for time_col in time:
                    for age_col in age:
                        models_90 = self.load_90_models_results(sex_col, time_col, age_col, region)
                        models_90['sex'] = sex_col
                        models_90['super'] = region
                        models_90['time_point'] = time_col
                        models_90['age'] = age_col
                        lst.append(models_90)
        models_90 = pd.concat(lst)
        models_90 = models_90.rename(columns={'X_cd4_mid': 'cd4_mid'})
        return models_90

    def load_UNAIDS_results(self):
        indir_unaids = "FILEPATH/UNAIDS_2023"
        pattern = os.path.join(indir_unaids, "???_HIVonART.csv")
        # Find all matching files
        matching_files = glob.glob(pattern)
        lst = []

        # Check if any file is found and read the first matching file
        if matching_files:
            for infile in matching_files:
                # Extract the '???' part from the filename
                base_name = os.path.basename(infile)  # Get the filename without the directory
                loc = base_name.split('_')[0]    # Extract the part before '_HIVonART.csv'
                df = pd.read_csv(infile)
                df['iso3'] = loc
                lst.append(df)
        else:
            print("No matching files found.")
        df = pd.concat(lst)
        age_convert = {'15-24': '15_25', '25-34': '25_35', '35-44': '35_45', '45-54': '45_55'}
        cd4_convert = {'GT500CD4': 750, '350to500CD4': 425, '250to349CD4': 300, '200to249CD4': 225, 
                       '100to199CD4': 150, '50to99CD4': 75, 'LT50CD4': 25}
        duration_convert = {'LT6Mo': '0_6', '6to12Mo': '7_12', 'GT12Mo': '13_24'}
        df['age'] = df['age'].map(lambda x: age_convert[x])
        df['cd4_mid'] = df['CD4_category'].map(lambda x: cd4_convert[x])
        df['time_point'] = df['duration'].map(lambda x: duration_convert[x])
        # Merge in region
        df_loc = get_region_iso3()
        df = df_loc.loc[:,['iso3', 'model_region']].merge(df, on='iso3')

        # Use the representative country for each region
        df = df.loc[df.iso3.isin(['CAF', 'ZWE', 'CMR', 'ETH'])]

        self.un_df = df.copy()
        
    def compare_results(self, plot_space):
        """Compare results with GBDYEAR."""
        self.load_gbd_results()
        pp = PdfPages(
            os.path.join(self.o_folder, f"{self.region}_compare_{plot_space}.pdf")
        )

        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55', 'age_55_100']

        # For testing
        # sex = ['sex_1']
        # time = ['time_0_6']
        # age = ['age_15_25']
        for region in self.regions:
            for sex_col in sex:
                for time_col in time:
                    for age_col in age:
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        pred = self.df_design.query(query)
                        obs = self.df_region.query(query)
                        obs = obs.query(f"super == '{region}'")
                        query = f"sex == '{sex_col.strip('sex_')}' "\
                            f"and age == '{age_col.strip('age_')}' "\
                            f"and time_point == '{time_col.strip('time_')}' "\
                            f"and super == '{region}'"
                        gbd = self.gbd_df.query(query).sort_values('X_cd4_mid')
                        models_90 = self.load_90_models_results(
                                sex_col, time_col, age_col, region)
                        # Plot observations
                        if plot_space == 'linear':
                            plt.scatter(obs.cd4_mid, obs.meas_value,
                                        s=1/(self.SCATTER_SIZE*obs.logit_se**2))
                            plt.plot(pred.cd4_mid, pred.preds,
                                     color='b', label="Current")
                            plt.fill_between(pred.cd4_mid,
                                             pred.preds_lower,
                                             pred.preds_upper,
                                             color='b',
                                             alpha=0.3)
                            # Plot gbd results
                            plt.plot(gbd.X_cd4_mid, gbd.prob,
                                     color='r', label="GBD2017")
                            plt.fill_between(gbd.X_cd4_mid, gbd.prob_lo,
                                             gbd.prob_hi, color='r', alpha=0.3)
                            # Plot 90 models results
                            plt.plot(models_90.X_cd4_mid, models_90.prob,
                                     color='g', label="90 Models")

                            plt.fill_between(models_90.X_cd4_mid, models_90.prob_lo,
                                             models_90.prob_hi, color='g', alpha=0.3)
                        else:
                            plt.scatter(obs.cd4_mid, obs.logit_mean,
                                        s=1/(self.SCATTER_SIZE*obs.logit_se**2))
                            plt.plot(pred.cd4_mid, logit(pred.preds),
                                     color='b', label="Current")
                            plt.fill_between(pred.cd4_mid,
                                             logit(pred.preds_lower),
                                             logit(pred.preds_upper),
                                             color='b',
                                             alpha=0.3)
                            # Plot gbd results
                            plt.plot(gbd.X_cd4_mid, logit(gbd.prob),
                                     color='r', label="GBD2017")
                            plt.fill_between(gbd.X_cd4_mid, logit(gbd.prob_lo),
                                             logit(gbd.prob_hi), color='r', alpha=0.3)
                            # Plot 90 models results
                            plt.plot(models_90.X_cd4_mid, logit(models_90.prob),
                                     color='g', label="90 Models")

                            plt.fill_between(models_90.X_cd4_mid, logit(models_90.prob_lo),
                                             logit(models_90.prob_hi), color='g', alpha=0.3)
                        plt.title(f"Region: {region}; Sex: {sex_col.strip('sex_')}; "
                                      f"Age: {age_col.strip('age_')}; Time: {time_col.strip('time_')}")
                        plt.xlabel("cd4_mid")
                        plt.ylabel("Mortality")
                        plt.legend()
                        pp.savefig()
                        plt.close()
        pp.close()


    def compare_UNAIDS_results(self, plot_space):
        """Compare results with UNAIDS."""
        self.load_UNAIDS_results()

        pp = PdfPages(
            os.path.join(self.o_folder, f"IHME_compare_UNAIDS.pdf")
        )

        sex = ['sex_1', 'sex_2']
        time = ['time_0_6', 'time_7_12', 'time_13_24']
        age = ['age_15_25', 'age_25_35',
               'age_35_45', 'age_45_55']
        colors = ['r', 'b', 'y', 'm', 'c']

        for region in self.regions:
            for sex_col in sex:
                for time_col in time:
                    for age_col in age:
                        query = self.create_query(
                            sex_col, time_col, age_col, region)
                        pred = self.df_design.query(query).sort_values('cd4_mid')
                        obs = self.df_region.query(query)
                        obs = obs.query(f"super == '{region}'")
                        plt.scatter(obs.cd4_mid, obs.meas_value,
                                        s=1/(self.SCATTER_SIZE*obs.logit_se**2))
                        plt.plot(pred.cd4_mid, pred.preds,
                                         color='g', label="GBD2023_3 models")

                        models_90 = self.load_90_models_results(sex_col, time_col, age_col, region)

                        # Plot 90 models results
                        plt.plot(models_90.X_cd4_mid, models_90.prob, color='k', label="GBD2021_90 models")
                        
                        for index, model_region in enumerate(['CSA', 'ESA', 'SSA', 'WSA']):
                            query = f"sex == {sex_col.strip('sex_')} "\
                                f"and age == '{age_col.strip('age_')}' "\
                                f"and time_point == '{time_col.strip('time_')}' "\
                                f"and model_region == '{model_region}'"
                            UNAIDS = self.un_df.query(query).sort_values('cd4_mid')
                            # Plot UNAIDS results
                            plt.plot(UNAIDS.cd4_mid, UNAIDS.mort,
                                     color=colors[index], label=f"UNAIDS_{model_region}")
                        plt.title(f"Region: {region}; Sex: {sex_col.strip('sex_')}; "
                                      f"Age: {age_col.strip('age_')}; Time: {time_col.strip('time_')}")
                        plt.xlabel("CD4")
                        plt.ylabel("Mortality")
                        plt.legend()
                        pp.savefig()
                        plt.close()
        pp.close()


    def get_coef_uncertainty(self):
        beta_lower = np.percentile(self.beta_samples, 2.5, axis=0)
        beta_upper = np.percentile(self.beta_samples, 97.5, axis=0)
        gamma_lower = np.percentile(self.gamma_samples, 2.5, axis=0)
        gamma_upper = np.percentile(self.gamma_samples, 97.5, axis=0)
        beta_dict = {'variable': self.me_model.cov_names + ['gamma'],
                     'coeffs_lower': np.round(beta_lower.tolist() + gamma_lower.tolist(), 3),
                     'coeffs_upper': np.round(beta_upper.tolist() + gamma_upper.tolist(), 3)}
        df = pd.DataFrame(data=beta_dict)
        return df

    def save_model(self):
        """Save the model object, coefficients and fits."""
        # Save coefficients
        beta_dict = {'variable': self.me_model.cov_names,
                     'coeffs_estimate': np.round(self.me_model.beta_soln, 3)}
        if len(self.me_model.cov_names) < len(self.me_model.beta_soln):
            df = self.me_model.summary()[0]
            df.to_csv(os.path.join(self.o_folder, f"{self.region}_coefficients.csv"),
                header=True)
        else:
            df = pd.DataFrame(data=beta_dict)
            df = df.append({'variable': 'gamma',
                            'coeffs_estimate': np.round(self.me_model.gamma_soln, 3)},
                           ignore_index=True)
            df = df.merge(self.get_coef_uncertainty()).transpose()
            df.to_csv(os.path.join(self.o_folder, f"{self.region}_coefficients.csv"),
                header=False)
        # Save model
        o_folder = Path(os.path.join(self.o_folder, 'mrbrt_model'))
        if not o_folder.exists():
            o_folder.mkdir()        
        with open(os.path.join(o_folder, f"{self.region}_me_model.pkl"), "wb") as f_write:
            dill.dump(self.me_model, f_write)
        with open(os.path.join(o_folder, f"{self.region}_signal_model.pkl"), "wb") as f_write:
            dill.dump(self.signal_model, f_write)
        with open(os.path.join(o_folder, f"{self.region}_model.pkl"), "wb") as f_write:
            dill.dump(self, f_write)

        # Save model fits
        self.df_design.to_csv(os.path.join(self.o_folder, f"{self.region}_fits.csv"),
                              index=False)
        print("Beta solution: ", np.round(self.me_model.beta_soln, 3))
        print("Gamma solution: ", np.round(self.me_model.gamma_soln, 3))


    # @staticmethod
    def create_query(self, *args, country_specific=False):
        """args is a list of column. eg. subset of [sex_col, time_col, age_col, region]"""
        col_dict = {'sex_2': 0, 'time_7_12': 0, 'time_13_24': 0, 'age_25_35': 0,
                    'age_35_45': 0, 'age_45_55': 0, 'age_55_100': 0}
        if country_specific:
            pass
        else:
            # For region specific model, no need to include region dummy variable
            if len(self.regions) == 3:
                col_dict.update({'ssa': 0, 'other': 0})
            else:
                pass
        
        for col in args:
            if col in col_dict:
                col_dict.update({col: 1})
        query = ' and '.join(
            [f'{k} == {repr(v)}' for k, v in col_dict.items()])
        return query

    @staticmethod
    def unique_study_id(df):
        """Create study id for each observation."""
        if np.isnan(df.subcohort_id):
            return df.nid
        else:
            return f"{df.nid}_{df.subcohort_id}"

    @staticmethod
    def one_hot_encoding(df, col):
        return pd.get_dummies(df, columns=[col], prefix='', prefix_sep='')

    @staticmethod
    def reverse_one_hot_encoding(df_one_hot):
        """Reverse one-hot encoding."""
        def get_time(row):
            if row['time_7_12'] == 1:
                return 'time_7_12'
            elif row['time_13_24'] == 1:
                return 'time_13_24'
            else:
                return 'time_0_6'

        def get_sex(row):
            if row['sex_2'] == 1:
                return 'sex_2'
            else:
                return 'sex_1'

        def get_age(row):
            for col in ['age_25_35', 'age_35_45', 'age_45_55', 'age_55_100']:
                if row[col] == 1:
                    return col
            else:
                return 'age_15_25'

        def get_region(row):
            for col in ['ssa', 'other']:
                if row[col] == 1:
                    return col
            else:
                return 'high'
        df = df_one_hot.copy()
        df['time_point'] = df.apply(lambda row: get_time(row), axis=1)
        df['sex'] = df.apply(lambda row: get_sex(row), axis=1)
        df['age'] = df.apply(lambda row: get_age(row), axis=1)
        df['super'] = df.apply(lambda row: get_region(row), axis=1)
        return df

    @staticmethod
    def label_age(x):
        """Return age name for each row data."""
        age = '15_25'
        for col in ['age_25_35', 'age_35_45', 'age_45_55', 'age_55_100']:
            if x[col] == 1:
                age = col.split('age_')[1]
        return f"{age}"

    @staticmethod
    def label_time(x):
        """Return time name for each row data."""
        time = '0_6'
        for col in ['time_7_12', 'time_13_24']:
            if x[col] == 1:
                time = col.split('time_')[1]
        return f"{time}"

    @staticmethod
    def label_sex(x):
        """Return sex name for each row data."""
        sex = 'male'
        if x['sex_2'] == 1:
            sex = 'female'
        return f"{sex}"

    def region_specific_covariates(self):
        """Return regional mean covariates for each year."""
        df_loc = OnART.load_gbd_loc()
        df_loc.loc[:, 'super_region'] = df_loc.loc[:,'super_region_name'].map(
            lambda x: 'Other' if x not in ['Sub-Saharan Africa', 'High-income'] else x
        )
        region_covs = df_loc.merge(self.df_covs, on='location_name')
        region_covs = region_covs.groupby(['super_region','year_id']).mean().reset_index()
        return region_covs

    @staticmethod
    def assign_region_covariates(transform_covariates, region_covs, df_design):
        """Assign the mean covariates for each region to design dataframe."""
        if transform_covariates:
            df_design.loc[df_design.ssa==1, 'haqi'] = 0
            df_design.loc[df_design.other==1, 'haqi'] = 0
            df_design.loc[(df_design.ssa==0) & (df_design.other==0), 'haqi'] = 0
        else:
            df_design.loc[df_design.ssa==1, 'haqi'] = \
                region_covs.query("super_region=='Sub-Saharan Africa' and year_id==2019").mean_value.values[0]
            df_design.loc[df_design.other==1, 'haqi'] = \
                region_covs.query("super_region=='Other' and year_id==2019").mean_value.values[0]
            df_design.loc[(df_design.ssa==0) & (df_design.other==0), 'haqi'] = \
                region_covs.query("super_region=='High-income' and year_id==2019").mean_value.values[0]
        return df_design

    def plot_coefficients(self, df, dim):
        """Plot the coefficients for sub-models based on 
           the combination of region, sex, and age."""
        plt.figure(figsize=(10,8))
        sns.scatterplot(data=df, x='variable', y='coeffs_estimate', hue=dim, alpha=0.6, s=60)
        plt.xlabel("Variable", fontsize=15)
        plt.ylabel("Coefficient estimates", fontsize=15)
        plt.legend(loc='lower right', frameon=False, fontsize=15)
        plt.tick_params(axis='both', which='major', labelsize=15, bottom=False, left=False)
        plt.savefig(os.path.join(self.o_folder, f"{dim}_coefficients.pdf"))
        plt.close()

    def run_sub_models(self):
        sex = ['sex_1', 'sex_2']
        age = ['age_55_100', 'age_45_55', 'age_15_25', 'age_25_35', 'age_35_45']
        time = ['time_7_12', 'time_13_24', 'time_0_6']
        regions = ['ssa', 'high', 'other']
        df_all = self.df.copy()
        lst_coef = []
        for region in regions:
            for sex_col in sex:
                for age_col in age:
                    for time_col in time:
                        name = f"{region}_{sex_col}_{age_col}_{time_col}"
                        query = f"sex=='{sex_col}' and age=='{age_col}' and super=='{region}' and time_point=='{time_col}'"
                        self.df = df_all.query(query)
                        try:
                            self.fit_models(region)
                            self.predict_signal()
                            self.get_coef_samples()
                            self.predict_final()
                            self.plot_model_fits(suffix=name)
                            beta_dict = {'variable': self.me_model.cov_names,
                                         'coeffs_estimate': np.round(self.me_model.beta_soln, 3)}
                            df_coef = pd.DataFrame(data=beta_dict)
                            df_coef = df_coef.append({'variable': 'gamma',
                                                      'coeffs_estimate': np.round(*self.me_model.gamma_soln, 3)},
                                            ignore_index=True)
                            df_coef['region'] = region
                            df_coef['sex'] = sex_col
                            df_coef['age'] = age_col
                            lst_coef.append(df_coef)
                        except:
                            continue
        df_coef = pd.concat(lst_coef)
        df_coef.to_csv(os.path.join(self.o_folder, f"all_coefficients.csv"), index=False)

        for dim in ['region', 'sex', 'age']:
            self.plot_coefficients(df_coef, dim)
        self.subset_predictions()

    def run(self, region=None, plot_space='logit'):
        """Run the model"""

        self.load_data()

        if self.explore_sub_models:
            # If run sub models (divided over group combinations), need to tweak dem_covs; 
            # otherwise would encounter singularity matrix error.
            self.dem_covs = []
            self.run_sub_models()
        else:
            self.fit_models(region)
            self.predict_signal()
            self.get_coef_samples()
            self.predict_final()
            self.save_outputs()
            self.plot_model_fits()
            self.subset_predictions()
            self.plot_design_predictions(plot_space=plot_space)
            self.compare_results(plot_space=plot_space)
            self.compare_results(plot_space='linear')
            self.compare_UNAIDS_results(plot_space='linear')
            self.save_model()
