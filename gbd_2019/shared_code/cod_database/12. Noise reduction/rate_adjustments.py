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
        # initialize this to something crazy small, then adjust later when
        # nonzero floor file is read in
        self.min_possible_val = 1e-50

    def get_computed_dataframe(self, pop_df, env_df, cause_hierarchy):
        """Calculate mortality rates and replace cause fractions, as needed.

        Make death rates and calculate the cf as if the rate were 2
        MADs below the "global" median. Every cause in the floor file
        is checked to ensure non-zero values in any non-restricted age-sex.
        So, just check and make sure there is something there for the cause,
        filling in zeroes where missing if the cause is present in the floor
        file (will break if there is a cause not present)
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
        self.df = self.df[orig_cols]
        self.df = self.df.append(age_aggs)
        # find lowest non-zero value that is in the dataframe and check that
        # it is not lower than lowest non-zero floor value
        data_min_val = self.df[self.df > 0][self.cf_cols].min().min()
        assert data_min_val >= self.min_possible_val, \
            "Data min value [{}] was lower than non-zero floor min " \
            "value [{}]".format(data_min_val, self.min_possible_val)
        return self.df

    def make_replace_cf(self):
        """Replace cause fractions based on mortality rates.

        If the rate is over 0 and less than the floor, then the cause
        fractions are replaced with floor * pop / mean_env
        """
        self.df.loc[self.df['floor'].isnull(), 'floor'] = self.df['min_floor']
        # there are so many checks before this that it would be very surprising
        # if this line does anything, but its another round of safety to make
        # sure that cause fractions arent being replaced with null
        self.df.loc[self.df['floor'].isnull(), 'floor'] = 0
        self.df['cf_replace'] = (
            (self.df['floor'] * self.df['population']) / self.df['mean_env']
        )
        self.min_possible_val = self.df.cf_replace.min()

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

        # make sure 2017-2018 are still missing
        missing_years = [2017, 2018]
        assert df.loc[df['year_id'].isin(missing_years)].floor.isnull().values.all()
        df = df.loc[~df['year_id'].isin(missing_years)]

        # We have determined that the floor is missing values for:
        # (1) certain cause/age/sexes in 2016 - we will use the 2015 floor to fill in these values
        # (2) certain cause/age/sexes across the entire time series - really nothing we
        # can do short of resetting the floor
        merge_cols = ['cause_id', 'age_group_id', 'sex_id']
        report_duplicates(df, merge_cols + ['year_id'])
        new_floor = pd.merge(
            df.loc[df.year_id == 2016].copy(), df.loc[df.year_id == 2015].copy(),
            how='outer', on=merge_cols, suffixes=('', '_2015')
        )
        new_floor = new_floor.fillna({'floor': new_floor['floor_2015']})\
            .loc[:, merge_cols + ['year_id', 'floor']]
        df = df.loc[df.year_id != 2016]\
            .append(new_floor, ignore_index=True, sort=True)
        # If anything else is still missing, make sure it's missing for the entire time
        # series - otherwise we should write something more sophisticated to fill it in
        assert df.assign(floor_null=df.floor.isnull())\
            .groupby(merge_cols + ['floor_null'])['year_id'].apply(
            lambda x: set(x) == set(range(1980, 2017))).all()

        # copy 2016 to 2017, 2018
        for year in missing_years:
            df = df.append(
                df.loc[df.year_id == 2016].copy().assign(year_id=year), ignore_index=True
            )

        # Due to age restriction changes since last round, we now have data in cause/age
        # groups where we had no floor in GBD 2017
        # Add in a nonzero floor created based on GBD 2019 data for these cause/age groups
        new_cause_ages = pd.read_csv(self.conf.get_resource('nonzero_floor_new_age_restrictions'))\
            .drop('borrow_age_group_id', axis='columns')
        assert new_cause_ages.notnull().values.all()
        df = df.append(new_cause_ages, sort=True)

        # no duplicates
        df = df.loc[df.floor.notnull()]
        report_duplicates(df, ['year_id', 'cause_id', 'age_group_id', 'sex_id'])

        return df

    def fill_na_floors(self, df):
        if df.floor.isnull().any():
            median = np.median(df[~df.floor.isnull()].floor)
            df.loc[df['floor'].isnull(), 'floor'] = median
        return df

    def merge_nonzero_mad_info(self, cmdf):
        """Read in the floor input and merge onto main dataframe."""
        nonzero_mad = pd.read_csv(self.conf.get_resource("nonzero_floor_mad"))
        nonzero_mad = self.convert_nonzero_mad(nonzero_mad, cmdf)
        nonzero_mad_cols = self.merge_cols + ['floor']
        nonzero_mad = nonzero_mad[nonzero_mad_cols]
        self.df = self.df.merge(nonzero_mad, how='left', on=self.merge_cols)
        if self.df.floor.isnull().any():
            self.df = self.df.groupby(
                ['year_id', 'sex_id', 'cause_id']).apply(self.fill_na_floors)
        if self.df.floor.isnull().any():
            trouble_causes = self.df[self.df.floor.isnull()].cause_id.unique()
            filler = np.median(self.df[~self.df.floor.isnull()].floor)
            print_log_message("using nonzero filler because"
                              " of these causes: {}".format(trouble_causes))
            self.df.floor = self.df.floor.fillna(filler)
        self.df.loc[self.df.cause_id == 975, 'floor'] = 1e-50

        report_if_merge_fail(self.df, 'floor', self.merge_cols)

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print("You requested the diag dataframe before it was ready,"
                  " returning an empty dataframe.")
            return pd.DataFrame()
