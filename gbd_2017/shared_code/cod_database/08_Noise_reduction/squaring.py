import itertools

import pandas as pd

from cod_process import CodProcess
from configurator import Configurator
from cod_prep.utils import (
    report_duplicates, report_if_merge_fail, enforce_asr
)
from cod_prep.downloaders.locations import add_location_metadata


class Squarer(CodProcess):
    """Square data before noise reduction."""

    conf = Configurator('standard')

    def __init__(self, cause_meta_df, age_meta_df,
                 location_meta_df=None, dhs=False):

        self.geo_cols = ['location_id', 'site_id']

        self.core_index = [
            'geo_group', 'year_id', 'age_group_id', 'sex_id', 'cause_id'
        ]

        # cause fraction columns
        self.cf_cols = ['cf', 'cf_raw', 'cf_corr', 'cf_rd']

        # all value columns
        self.val_cols = self.cf_cols + ['sample_size']

        # cached metadata
        self.cause_hierarchy = cause_meta_df
        self.age_meta_df = age_meta_df
        if location_meta_df is not None:
            self.location_meta_df = location_meta_df

        # flag for whether or not this is squaring DHS data
        self.dhs = dhs

    def get_computed_dataframe(self, df):
        """Return computations."""
        self.draw_cols = [col for col in df.columns if col.startswith('draw')]
        self.val_cols = self.val_cols + self.draw_cols
        self.id_cols = [col for col in df.columns if col not in self.val_cols]
        # add country level id for DHS data
        if self.dhs:
            df = self.add_iso3(df)
            iso3_dict = self.get_iso3_dict(df)

        # add indicator column for geographic group
        df = self.add_geo_group_col(df)

        geo_groups = df['geo_group'].unique()
        geo_dfs = []
        for geo_group in geo_groups:
            geo_df = df.loc[df['geo_group'] == geo_group]
            # dictionary of unique values of these data
            if not self.dhs:
                data_dict = Squarer.get_data_dict(geo_df, self.core_index)
            else:
                data_dict = self.get_dhs_data_dict(geo_df, iso3_dict)
            geo_df = self.square_data(geo_df, data_dict)
            geo_dfs.append(geo_df)
        df = pd.concat(geo_dfs, ignore_index=True)
        del(geo_dfs)
        return df

    @staticmethod
    def get_data_dict(df, core_index):

        df = df.groupby(core_index, as_index=False).sum()
        report_duplicates(df, core_index)

        # make data dictionary
        data_dict = {}
        for dem_col in core_index:
            dem_col_values = list(df[dem_col].unique())
            data_dict[dem_col] = dem_col_values
        return data_dict

    def get_dhs_data_dict(self, geo_df, iso3_dict):

        data_dict = {}
        for dem_col in ['cause_id', 'geo_group']:
            dem_col_values = list(geo_df[dem_col].unique())
            data_dict[dem_col] = dem_col_values

        iso3 = geo_df.iso3.unique()
        assert len(iso3) == 1
        iso3 = iso3[0]
        iso3_years = iso3_dict[iso3][0]
        iso3_ages = iso3_dict[iso3][1]

        age_group_id_50_54 = 15
        if age_group_id_50_54 in iso3_ages:
            iso3_ages.remove(age_group_id_50_54)

        data_dict.update({'sex_id': [2], 'age_group_id': iso3_ages,
                          'year_id': iso3_years})

        return data_dict

    def get_iso3_dict(self, df):

        iso3_dict = {}
        for iso3 in df.iso3.unique():
            iso3_df = df.loc[df['iso3'] == iso3]
            years = list(iso3_df.year_id.unique())
            ages = list(iso3_df.age_group_id.unique())
            iso3_dict[iso3] = (years, ages)
        return iso3_dict

    def add_iso3(self, df):
        """Add iso3 to incoming dataframe."""
        df = add_location_metadata(df, 'ihme_loc_id',
                                   location_meta_df=self.location_meta_df)
        df['iso3'] = df['ihme_loc_id'].str[0:3]
        df.drop(['ihme_loc_id'], axis=1, inplace=True)
        return df

    def add_geo_group_col(self, df):

        self.geo_groups = df[self.geo_cols].drop_duplicates().reset_index()
        self.geo_groups = self.geo_groups.rename(
            columns={'index': 'geo_group'})
        # ensure that index is unique
        assert len(set(self.geo_groups.index)) == len(self.geo_groups)
        df = df.merge(self.geo_groups, on=self.geo_cols, how='left')
        report_if_merge_fail(df, 'geo_group', self.geo_cols)
        df.drop(self.geo_cols, axis=1, inplace=True)
        return df

    def square_data(self, df, data_dict):

        groups_cols = list(self.id_cols)
        groups_cols.remove('cause_id')
        for col in self.geo_cols:
            groups_cols.remove(col)
        groups_cols.append('geo_group')
        # split data into groups
        groups = df.groupby(groups_cols, as_index=False).mean()
        groups_cols.append('sample_size')
        groups = groups[groups_cols]

        # make a completely square dataframe based on data_dict
        rows = itertools.product(*data_dict.values())
        square_df = pd.DataFrame.from_records(rows, columns=data_dict.keys())

        # don't add rows that break age sex restrictions
        square_df = enforce_asr(
            square_df, self.cause_hierarchy, self.age_meta_df
        )

        # merge that with the data
        merge_cols = list(square_df.columns)
        merge_cols.remove('cause_id')
        square_df = square_df.merge(groups, how='outer', on=merge_cols)
        for cf_col in self.cf_cols:
            square_df[cf_col] = 0

        for draw_col in self.draw_cols:
            square_df[draw_col] = 0

        square_df['sample_size'] = square_df['sample_size'].fillna(0)
        df = df.append(square_df, ignore_index=True)

        # return values associated with each geo_group to data
        df = df.merge(self.geo_groups, on='geo_group', how='left')
        report_if_merge_fail(df, 'location_id', 'geo_group')
        df = df.drop('geo_group', axis=1)

        df = df.sort_values(by=['cf'] + self.draw_cols, ascending=False)
        df = df.drop_duplicates(subset=self.id_cols, keep='first')
        return df
