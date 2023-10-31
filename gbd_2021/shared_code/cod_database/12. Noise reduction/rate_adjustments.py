import pandas as pd
import numpy as np

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.population import (
    add_population,
    add_envelope,
)
from cod_prep.utils import (
    report_if_merge_fail, print_log_message, report_duplicates, expand_to_u5_age_detail
)


class NonZeroFloorer(CodProcess):
    conf = Configurator('standard')
    draws = list(range(0, conf.get_resource('uncertainty_draws')))
    cf_draw_cols = ['cf_draw_{}'.format(draw) for draw in draws]

    def __init__(self, df, track_flooring):
        self.df = df
        self.track_flooring = track_flooring
        self.merge_cols = ['sex_id', 'age_group_id', 'cause_id']
        self.cf_col = 'cf_final'
        if 'cf_draw_0' in self.df:
            self.cf_cols = [self.cf_col] + self.cf_draw_cols
        else:
            self.cf_cols = [self.cf_col]

    def get_computed_dataframe(self, pop_df, env_df, cause_hierarchy, data_type_id):
        """Calculate mortality rates and replace cause fractions, as needed.
        """

        orig_cols = list(self.df.columns)
        age_aggs = self.df[self.df.age_group_id.isin([22, 27])]
        self.df = self.df[~self.df.age_group_id.isin([22, 27])]
        if data_type_id in [9, 10]:
            self.drop_non_observed_cause_age_sexes(data_type_id)
        self.merge_nonzero_floor_info(cause_hierarchy)
        for col in self.cf_cols:
            self.replace_cf(col)
        self.diag_df = self.df
        null_cfs = self.df.loc[self.df[self.cf_cols].isnull().any(axis=1)]
        if len(null_cfs) > 0:
            raise AssertionError(
                "Found null rates in the data: \n{}".format(null_cfs)
            )
        if self.track_flooring:
            orig_cols += ['cf_final_pre_floor', 'floor_flag']
        self.df = self.df[orig_cols]
        self.df = self.df.append(age_aggs)
        if self.track_flooring:
            self.df.fillna(value={'cf_final_pre_floor': self.df['cf_final'], 'floor_flag': 0},
                inplace=True)
        return self.df

    def replace_cf(self, check_cf_col):
        cf_over_0 = self.df[check_cf_col] > 0
        cf_less_than_floor = self.df[check_cf_col] < self.df['floor']
        if (check_cf_col == 'cf_final') and self.track_flooring:
            self.df['floor_flag'] = 0
            self.df[check_cf_col + '_pre_floor'] = self.df[check_cf_col]
            self.df.loc[
                cf_over_0 & cf_less_than_floor,
                'floor_flag'] = 1
        self.df.loc[
            cf_over_0 & cf_less_than_floor,
            check_cf_col] = self.df['floor']

    def convert_nonzero_mad(self, df, cmdf):
        # add cause_id
        cmdf = cmdf[['acause', 'cause_id']]
        df = df.merge(cmdf, how='left', on='acause')
        df = df.loc[df.cause_id.notnull()]
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

        new_cause_ages = pd.read_csv(self.conf.get_resource('nonzero_floor_new_age_restrictions'))\
            .drop('borrow_age_group_id', axis='columns')
        assert new_cause_ages.notnull().values.all()
        df = df.append(new_cause_ages, sort=True)

        df = df.pivot_table(
            index=['cause_id', 'age_group_id', 'sex_id'], columns='year_id', values='floor')
        for year_id in range(1980, self.conf.get_id("year_end")):
            if year_id not in df.columns:
                df[year_id] = np.NaN
        df = df.sort_values(axis='columns', by='year_id')\
            .fillna(method='ffill', axis=1)\
            .reset_index()\
            .melt(
                id_vars=['cause_id', 'age_group_id', 'sex_id'],
                var_name='year_id', value_name='floor')

        df = expand_to_u5_age_detail(df)

        # no duplicates
        assert df.notnull().values.all()
        df = df.astype({
            'cause_id': int,
            'age_group_id': int,
            'sex_id': int,
            'year_id': int,
            'floor': float
        })
        report_duplicates(df, ['year_id', 'cause_id', 'age_group_id', 'sex_id'])
        return df

    def check_nonzero_floors(self, df):
        assert df.notnull().values.all()
        df = df.astype({
            'cause_id': int,
            'age_group_id': int,
            'sex_id': int,
            'floor': float
        })
        report_duplicates(df, ['cause_id', 'age_group_id', 'sex_id'])
        return df

    def fill_na_floors(self, df):
        if df.floor.isnull().any():
            median = np.median(df[~df.floor.isnull()].floor)
            df.loc[df['floor'].isnull(), 'floor'] = median
        return df

    def drop_non_observed_cause_age_sexes(self, data_type_id):
        drop_file = pd.read_csv(self.conf.get_resource('cause_age_sex_drops'))
        self.df = self.df.merge(drop_file, on=['cause_id', 'age_group_id', 'sex_id'],
            how='left', indicator=True
        )
        if data_type_id == 9:
            bad_drops_df = self.df.loc[(self.df.cf_agg > 0) & (self.df._merge == 'both')][[
                'cause_id', 'age_group_id', 'sex_id'
            ]].drop_duplicates()
            if len(bad_drops_df) > 0:
                print(bad_drops_df)
                raise AssertionError(
                    "Trying to drop these cause/age/sexes with" \
                    "non-zero observations before noisereduction. The " \
                    "non-zero floor likely needs reran."
                )
        self.df = self.df.loc[~(self.df._merge == 'both')]
        self.df = self.df.drop('_merge', axis=1)

    def merge_nonzero_floor_info(self, cmdf):
        """Read in the floor input and merge onto main dataframe."""
        nonzero_floor = pd.read_csv(self.conf.get_resource("nonzero_floor"))
        nonzero_floor = self.check_nonzero_floors(nonzero_floor)
        nonzero_floor_cols = self.merge_cols + ['floor']
        nonzero_floor = nonzero_floor[nonzero_floor_cols]
        self.df = self.df.merge(nonzero_floor, how='left', on=self.merge_cols)
        if self.df.floor.isnull().any():
            self.df = self.df.groupby(
                ['sex_id', 'cause_id']).apply(self.fill_na_floors)
        if self.df.floor.isnull().any():
            trouble_causes = self.df[self.df.floor.isnull()].cause_id.unique()
            filler = np.median(self.df[~self.df.floor.isnull()].floor)
            print_log_message("using nonzero filler because"
                              " of these causes: {}".format(trouble_causes))
            self.df.floor = self.df.floor.fillna(filler)
        report_if_merge_fail(self.df, 'floor', self.merge_cols)

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        try:
            return self.diag_df
        except AttributeError:
            print("You requested the diag dataframe before it was ready,"
                  " returning an empty dataframe.")
            return pd.DataFrame()
