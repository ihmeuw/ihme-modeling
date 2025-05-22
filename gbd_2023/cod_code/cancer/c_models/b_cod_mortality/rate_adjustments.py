import pandas as pd
import numpy as np
from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.population import (
    add_population,
    add_envelope,
)
from cod_prep.utils import (
    report_if_merge_fail, print_log_message, report_duplicates
)
from cancer_estimation.py_utils import common_utils as utils 
from cancer_estimation._database import cdb_utils as cdb
from db_queries import get_cause_metadata


class NonZeroFloorer(CodProcess):
    """APPLY NON-ZERO FLOOR OF 1 DEATH PER 10,000,000"""
    conf = Configurator('standard')
    draws = range(0, conf.get_resource('uncertainty_draws'))
    cf_draw_cols = ['cf_draw_{}'.format(draw) for draw in draws]

    def __init__(self, df):
        self.df = df
        self.merge_cols = ['sex_id', 'age_group_id', 'cause_id']
        self.cf_col = 'cf_final'
        if 'cf_draw_0' in self.df:
            self.cf_cols = [self.cf_col] + self.cf_draw_cols
        else:
            self.cf_cols = [self.cf_col]
        self.min_possible_val = 1e-50


    def format_nzf(self, nzf_df, cmdf): 
        # merge acause column 
        nzf_df = pd.merge(nzf_df, cmdf[['acause','cause_id']], 
                        on='cause_id', how='left')
        return nzf_df


    def get_computed_dataframe(self, pop_df, env_df, cause_hierarchy):
        """Calculate mortality rates and replace cause fractions, as needed.
        """
        orig_cols = list(self.df.columns)
        age_aggs = self.df[self.df.age_group_id.isin([22, 27])]
        self.df = self.df[~self.df.age_group_id.isin([22, 27])]
        self.merge_pop_env(pop_df, env_df)
        self.merge_nonzero_mad_info(cause_hierarchy)
        self.make_min_floor()
        self.make_replace_cf()
        for col in self.cf_cols:
            self.replace_cf(col)
        self.diag_df = self.df
        null_cfs = self.df.loc[self.df[self.cf_cols].isnull().any(axis=1)]
        if len(null_cfs) > 0:
            raise AssertionError(
                "Found null rates in the data: \n{}".format(null_cfs)
            )
        self.df = self.df[orig_cols + ['rate','floor','floor_applied']]
        self.df = self.df.append(age_aggs)
        # find lowest non-zero value that is in the dataframe and check that
        # it is not lower than lowest non-zero floor value
        data_min_val = self.df.loc[self.df['cf_final']>0, 'cf_final'].min() 
        assert data_min_val >= self.min_possible_val, \
            "Data min value [{}] was lower than non-zero floor min " \
            "value [{}]".format(data_min_val, self.min_possible_val)
        return self.df
    

    def load_nonzero_floor(self, cmdf): 
        nzf_df = pd.read_csv(utils.get_path(process='cod_mortality', key='nonzero_floor_values'))
        return(nzf_df)


    def make_replace_cf(self):
        """Replace cause fractions based on mortality rates.
        """
        self.df.loc[self.df['floor'].isnull(), 'floor'] = self.df['min_floor']
        # there are so many checks before this that it would be very surprising
        # if this line does anything, but its another round of safety to make
        # sure that cause fractions arent being replaced with null
        self.df.loc[self.df['floor'].isnull(), 'floor'] = 0
        self.df['cf_replace'] = self.df['floor'].copy()


    def replace_cf(self, check_cf_col):
        """Mark where to replace CF values with the floor
        """
        self.df['rate'] = ((self.df[check_cf_col] * self.df['mean_env']) /
                           self.df['population'])
        self.df['floor_applied'] = 0 
        cf_over_0 = self.df[check_cf_col] > 0
        rate_less_than_floor = self.df[check_cf_col] < self.df['floor']
        threshold = 0.01
        data_with_rate_less_than_floor = self.df[rate_less_than_floor]
        # cod_mortality, nonzero_floor_workspace
        if len(data_with_rate_less_than_floor) > (threshold * len(self.df)):
            print(f"About to replace more than {threshold} of {check_cf_col} with floor values.")
            report_path = f'{utils.get_path(process="cod_mortality", key="nonzero_floor_workspace")}/values_replaced_with_floors.csv'
            utils.ensure_dir(report_path)
            print(f"Saving the relevant data to {report_path}.")
            data_with_rate_less_than_floor.to_csv(report_path)
        self.df.loc[
            cf_over_0 & rate_less_than_floor,
            check_cf_col] = self.df['cf_replace']
        self.df.loc[cf_over_0 & rate_less_than_floor,
            'floor_applied'] = 1 


    def make_min_floor(self):
        """Set min floor to the minimum cf of any rows floor by cause."""
        self.df['min_floor'] = self.df.groupby(
            'cause_id', as_index=False)['floor'].transform('min')
        missing_floor = self.df['min_floor'].isnull()
        nonzero_cf = self.df[self.cf_col] > 0
        assert len(self.df[nonzero_cf & missing_floor]) == 0

    def merge_pop_env(self, pop_df, env_df):
        if 'population' not in self.df.columns:
            self.df = add_population(self.df, add_cols=['population'],
                                     pop_df=pop_df)
        if 'mean_env' not in self.df.columns:
            self.df = add_envelope(self.df, add_cols=['mean_env'],
                                   env_df=env_df)


    def merge_nonzero_mad_info(self, cmdf):
        """Read in the floor input and merge onto main dataframe."""
        nonzero_mad = self.load_nonzero_floor(cmdf)
        nonzero_mad = self.format_nzf(nonzero_mad, cmdf)
        nonzero_mad_cols = self.merge_cols + ['floor']
        nonzero_mad = nonzero_mad[nonzero_mad_cols]
        self.min_possible_val = nonzero_mad['floor'].min()
        nonzero_mad = nonzero_mad.drop_duplicates()
        # ensure no floor values are missing
        self.check_for_missing_floor_values(nonzero_mad)
            
        self.df = self.df.merge(nonzero_mad, how='left', on=self.merge_cols)

    def check_for_missing_floor_values(self, nonzero_mad):
        test = self.df.merge(nonzero_mad, how='left', on=self.merge_cols, validate="m:1")
        if test.floor.isnull().any() == True:
            null_floor_selector = test['floor'].isnull()
            null_floor_rows = test[null_floor_selector]
            test = test[~null_floor_selector]
            print("Missing floor values have been detected.")
            report_path = f'{utils.get_path(process="cod_mortality", key="nonzero_floor_workspace")}/data_missing_floor_values.csv'
            utils.ensure_dir(report_path)
            print(f"Writing data with missing floor values to {report_path}")
            null_floor_rows.to_csv(report_path)
            print("Continuing without the data that's missing floor values.")


    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print("You requested the diag dataframe before it was ready,"
                  " returning an empty dataframe.")
            return pd.DataFrame()
