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
        self.merge_cols = ['year_id', 'sex_id', 'age_group_id', 'cause_id']
        self.cf_col = 'cf_final'
        if 'cf_draw_0' in self.df:
            self.cf_cols = [self.cf_col] + self.cf_draw_cols
        else:
            self.cf_cols = [self.cf_col]
        self.min_possible_val = 1e-50

    def _check_all_floors_exist(self, nzf_df): 
        ''' Check that all expected cancers, ages, and years, are present and have
            nonzero floor values 
        '''
        def _remove_ages_less_than(a, b): 
            '''
            '''
            orig_list = a.copy() 
            for val in orig_list: 
                if b ==5 & val in [2,3,4]:
                    continue 
                if val < b: 
                    a.remove(val)
            return a


        print("CHECKING FOR ALL CAUSES, AGES, and YEARS...")
        # create cause_list 
        db_link = cdb.db_api(db_connection_name='cancer_db')
        gbd_id = utils.get_gbd_parameter('current_gbd_round')
        registry_entity = db_link.get_table('registry_input_entity')
        registry_entity = registry_entity.loc[registry_entity['gbd_round_id'].eq(gbd_id) & 
                            registry_entity['is_active'].eq(1),]
        cancer_metadata = registry_entity[['acause','cause_id','yll_age_start','yll_age_end']]
        causes_checklist = registry_entity['acause'].unique().tolist()  
        
        # exceptions for nonzero floors 
        causes_checklist.remove('neo_nmsc_bcc')
        causes_checklist.remove('neo_ben_intest')
        causes_checklist.remove('neo_ben_utr')
        causes_checklist.remove('neo_ben_other')
        causes_checklist.remove('neo_ben_brain')
        causes_checklist.remove('_gc')

        # create year_list 
        year_start = utils.get_gbd_parameter('min_year_cod')
        year_end = utils.get_gbd_parameter('max_year') # + 1 for GBD2020 
        year_checklist = list(range(year_start, year_end))
        
        # sex &  age_id checklist 
        age_id_checklist =[5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 30, 31, 32, 235, 2, 3, 4] #age_ids for 0-95 ages 
        sex_checklist = [1,2]

        # print any causes/years/sexes that are expected and missing  
        for cancer in causes_checklist: 
            print('working on...{}'.format(cancer))
            subset = nzf_df.loc[nzf_df['acause'].eq(cancer), ]
            age_start = int(cancer_metadata.loc[cancer_metadata['acause'].eq(cancer), 'yll_age_start'])
            age_start = (age_start / 5) + 5 # conversion from age to GBD age_group_id
            if len(subset) == 0: 
                print('MISSING CAUSE... {} '.format(cancer))
            missing_ages = set(age_id_checklist) - set(subset['age_group_id'].unique().tolist())
            missing_ages = list(missing_ages)
            missing_ages = _remove_ages_less_than(missing_ages, age_start)
            if len(missing_ages) > 0: 
                print('missing the following ages for {}: {}'.format(cancer,missing_ages))
            missing_sexes = set(sex_checklist) - set(subset['sex_id'].unique().tolist())
            if len(missing_sexes) > 0: 
                print('missing the following sexes for {}: {}'.format(cancer,missing_sexes))
            missing_years = set(year_checklist) - set(subset['year_id'].unique().tolist())
            if len(missing_years) > 0: 
                print('missing the following years for {}: {}'.format(cancer,missing_years))
        return 

    def format_nzf(self, nzf_df, cmdf): 
        '''
        '''
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
        self.df = self.df[orig_cols + ['rate','floor']]
        self.df = self.df.append(age_aggs)
        # find lowest non-zero value that is in the dataframe and check that
        # it is not lower than lowest non-zero floor value
        data_min_val = self.df.loc[self.df['cf_final']>0, 'cf_final'].min() 
        assert data_min_val >= self.min_possible_val, \
            "Data min value [{}] was lower than non-zero floor min " \
            "value [{}]".format(data_min_val, self.min_possible_val)
        return self.df
    
    def convert_nonzero_mad(self, df, cmdf):
        # add cause_id
        cmdf = cmdf[['acause', 'cause_id']]
        df = df.merge(cmdf, how='left', on='acause')
        # add id to cols
        df = df.rename(columns={'year': 'year_id',
                                'sex': 'sex_id', 'age': 'age_group_id'})
        # convert age
        age_to_id_map = {1: 5, 5: 6, 10: 7, 15: 8,
                         20: 9, 25: 10, 30: 11, 35: 12,
                         40: 13, 45: 14, 50: 15, 55: 16,
                         60: 17, 65: 18, 70: 19, 75: 20,
                         80: 30, 85: 31, 90: 32, 95: 235,
                         91: 2, 93: 3, 94: 4
                         }
        df['age_group_id'] = df['age_group_id'].map(age_to_id_map)
        df = df.drop('acause', axis=1)

        return df


    def compile_nonzero_floor(self, cmdf): 
        '''
        For GBD2019, new floor values were generated for cancer causes that had 
        updated age restrictions, or was a new modeled cause. This function takes
        the original nonzero floor values, and appends all updated values 
        '''
        work_dir = utils.get_path(process='cod_mortality', key='nonzero_floor_workspace')
        orig_nzf = pd.read_csv(utils.get_path(process='cod_mortality', key='orig_nonzero_file'))

        # convert age_group_ids to comply with GBD's
        formatted_orig_nzf = self.convert_nonzero_mad(orig_nzf, cmdf)

        # load nonzero floor values with new age restrictions, and that were new causes 
        # for this GBD cycle 
        new_age_rstrct_df = pd.read_csv('{}/nonzero_floor_new_age_restrictions.csv'.format(work_dir))
        new_causes_df = pd.read_csv('{}/nonzero_new_causes.csv'.format(work_dir))

        # append all nonzero values together 
        comp_nzf = formatted_orig_nzf.append(new_age_rstrct_df)
        comp_nzf = comp_nzf.append(new_causes_df)

        return comp_nzf


    def make_replace_cf(self):
        """Replace cause fractions based on mortality rates.

        If the rate is over 0 and less than the floor, then the cause
        fractions are replaced with floor * pop / mean_env
        """
        self.df.loc[self.df['floor'].isnull(), 'floor'] = self.df['min_floor']
        self.df.loc[self.df['floor'].isnull(), 'floor'] = 0
        self.df['cf_replace'] = (
            (self.df['floor'] * self.df['population']) / self.df['mean_env']
        )

    def replace_cf(self, check_cf_col):
        # Replace the CF with the rate-adjusted CF if the
        # rate is less than the floor and greater than zero
        self.df['rate'] = ((self.df[check_cf_col] * self.df['mean_env']) /
                           self.df['population'])
        cf_over_0 = self.df[check_cf_col] > 0
        rate_less_than_floor = self.df['rate'] < self.df['floor']
        self.df.loc[
            cf_over_0 & rate_less_than_floor,
            check_cf_col] = self.df['cf_replace']

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

    def fill_na_floors(self, df):
        if df.floor.isnull().any():
            median = np.median(df[~df.floor.isnull()].floor)
            df.loc[df['floor'].isnull(), 'floor'] = median
        return df

    def merge_nonzero_mad_info(self, cmdf):
        """Read in the floor input and merge onto main dataframe."""
        # load nonzero floor values 
        nonzero_mad = self.compile_nonzero_floor(cmdf)
        nonzero_mad = self.format_nzf(nonzero_mad, cmdf)
        self._check_all_floors_exist(nonzero_mad) # checks that all age_groups/cancer/year/sex exist 
        nonzero_mad_cols = self.merge_cols + ['floor']
        nonzero_mad = nonzero_mad[nonzero_mad_cols]
        self.min_possible_val = nonzero_mad['floor'].min()
        self.df = self.df.merge(nonzero_mad, how='left', on=self.merge_cols)
        # ensure no floor values are missing 
        assert self.df.floor.isnull().any() == False, "null floor values exist"
        report_if_merge_fail(self.df, 'floor', self.merge_cols)

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print("You requested the diag dataframe before it was ready,"
                  " returning an empty dataframe.")
            return pd.DataFrame()
