"""Adjust maternal deaths and create maternal_hiv observations.

Use calculated proportions of maternal that is hiv positive maternal,
hiv positive hiv to adjust maternal parent and create maternal_hiv observations

Notes:
    Note on sequencing: This is placed after cause aggregation so that the
    percentage that is attributable to maternal_hiv in VA/VR is calculated off
    of the aggregate of all maternal causes.
"""

import os
import pandas as pd

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import add_age_metadata
from cod_prep.downloaders import add_nid_metadata
pd.options.mode.chained_assignment = None


class HIVMatPAFs(CodProcess):

    calc_cf_col = 'cf'
    all_cf_cols = ['cf', 'cf_raw', 'cf_corr', 'cf_rd']

    def __init__(self):
        self.configurator = Configurator('standard')
        self.cache_dir = self.configurator.get_directory('db_cache')
        self.maternal_hiv_props_path = \
            self.configurator.get_directory('maternal_hiv_props')
        # self.need_subnational_props = [51, 16, 86, 214, 165]

    def get_computed_dataframe(self, df, cause_meta_df, location_meta_df):
        restricted_maternal_df = \
            self.restrict_to_maternal_data(df, cause_meta_df)
        if restricted_maternal_df is None:
            # nothing to do if there is no maternal data to adjust
            return df
        appended_pafs = self.append_maternal_pafs(
            restricted_maternal_df.year_id.unique())
        # no longer need this step since new PAFs have been created
        # extra step to fix missing sub national proportions
        # appended_pafs = self.duplicate_national_props(appended_pafs, location_meta_df)
        merged_data = \
            self.merge_data_and_proportions(restricted_maternal_df,
                                            appended_pafs)
        percent_maternal = self.generate_percentages(merged_data)
        split_maternal = self.generate_splits(percent_maternal)
        hiv_cfs = self.create_maternal_hiv_cfs(split_maternal)
        cleaned = self.clean_adjusted_data(hiv_cfs)
        final = \
            self.append_adjusted_orig(df, restricted_maternal_df, cleaned)
        group_cols = [
            col for col in final.columns if col not in
            self.all_cf_cols and col not in ['sample_size']
        ]
        final = final.groupby(group_cols,
                              as_index=False).agg(
            {'sample_size': 'mean', 'cf': 'sum',
             'cf_raw': 'sum', 'cf_corr': 'sum',
             'cf_rd': 'sum'}
        )
        return final

    def restrict_to_maternal_data(self, df, cause_meta_df):
        """Restrict incoming dataframe to only maternal data."""
        df = df.copy()
        # get age start and age end for maternal ages
        maternal_metadata = cause_meta_df.loc[cause_meta_df['cause_id'] == 366]
        age_start = maternal_metadata['yll_age_start']
        assert len(age_start) == 1
        age_start = age_start.iloc[0]
        age_end = maternal_metadata.yll_age_end
        assert len(age_end) == 1
        age_end = age_end.iloc[0]

        data = add_age_metadata(df,
                                add_cols=['simple_age'],
                                merge_col='age_group_id',
                                force_rerun=False,
                                block_rerun=True,
                                cache_results=False,
                                cache_dir=self.cache_dir)
        data.rename(columns={'simple_age': 'age'}, inplace=True)
        maternal_data = data.loc[(df['cause_id'] == 366) &
                                 (data['age'] >= age_start) &
                                 (data['age'] <= age_end) &
                                 (data['sex_id'] == 2) &
                                 (data['year_id'] >= 1980)]
        maternal_data.drop('age', axis=1, inplace=True)
        if len(maternal_data) == 0:
            return None
        else:
            return maternal_data

    def append_maternal_pafs(self, years):
        """Read in proportions."""
        props = pd.DataFrame()
        for year in years:
            year = int(year)
            props_path = "{}/maternal_hiv_props_{}.csv".format(self.maternal_hiv_props_path, year)
            data = pd.read_csv(props_path)
            props = props.append(data)
        props = props.rename(columns={'year': 'year_id'})
        return props

    def duplicate_national_props(self, props_df, loc_df):
        """Duplicate national proportions and fill sub national proportions.

        Note: necessary in countries that we are now modeling sub nationally,
        but since we weren't before there aren't any sub national proportions
        for maternal hiv (yet).
        """
        subnational = loc_df.loc[
            loc_df['level'] > 3, ['location_id', 'parent_id',
                                  'level', 'path_to_top_parent']]

        # Russia sub nationals are level 5 while other countries are level 4
        subnational.loc[
            subnational['level'] == 5, 'parent_id'] = \
            subnational['path_to_top_parent'].str.split(',').str[3].astype(int)

        # only keep rows with the needed sub national locations
        subnational = subnational.loc[
            subnational['parent_id'].isin(self.need_subnational_props)]

        # drop level 4 sub national location_ids for Russia
        subnational = subnational.loc[~(
            (subnational['parent_id'] == 62) &
            (subnational['level'] == 4)
        )]
        subnational = subnational[['location_id', 'parent_id']]
        subnational.rename(
            columns={'location_id': 'child_location_id',
                     'parent_id': 'location_id'}, inplace=True)

        # create sub national maternal_hiv proportions from national
        subnational = props_df.merge(subnational, on='location_id')
        subnational.drop('location_id', axis=1, inplace=True)
        subnational.rename(columns={'child_location_id': 'location_id'},
                           inplace=True)
        props_df = pd.concat([props_df, subnational])
        assert not props_df.duplicated().any(), 'please check maternal'\
            ' proportions, there are duplicates'
        return props_df

    def merge_data_and_proportions(self, data, props):
        """Merge restricted maternal data and proportions."""
        merged_data = data.merge(props,
                                 on=['location_id',
                                     'age_group_id',
                                     'year_id'], how='left')
        assert merged_data.notnull().values.all(), 'maternal proportions '\
            'were not successfully merged with incoming data'
        return merged_data

    def generate_percentages(self, df):
        """Create new 'pct_maternal column'.

        This is to prepare for calculating maternal hiv cause fractions
        """
        df['pct_maternal'] = 1 - df['pct_hiv'] - df['pct_maternal_hiv']
        df.loc[df['pct_maternal'].isnull(), 'pct_maternal'] = 1
        df.loc[df['pct_hiv'].isnull(), 'pct_hiv'] = 0
        df.loc[df['pct_maternal_hiv'].isnull(), 'pct_maternal_hiv'] = 0
        assert all(x > 0 for x in df['pct_maternal'])
        assert df[
            ['pct_maternal', 'pct_hiv', 'pct_maternal_hiv']
        ].notnull().values.any(), 'there are missing percentages'
        assert all(abs(df['pct_maternal'] +
                       df['pct_hiv'] +
                       df['pct_maternal_hiv']) -
                   1) < .0001
        # proportion of maternal that is aggravated by hiv
        # cannot be above 13% based on USERNAME's meta-analysis; otherwise
        # this would suggest the percentage of maternal deaths that were
        # hiv positive is >1
        assert (df['pct_maternal_hiv_vr'] <= .13).all()
        # maternal_hiv should not yet exist
        assert not (df['cause_id'] == 741).any()
        return df

    ''' '''

    def generate_splits(self, df):
        """Create a column to indicate how the data should be split.

        (depends on source type)
        """
        df = add_nid_metadata(df,
                              add_cols='data_type_id',
                              block_rerun=True,
                              cache_dir=self.cache_dir,
                              force_rerun=False,)
        df.loc[df['data_type_id'].isin([7, 5]), 'split_maternal'] = 1
        df.loc[df['split_maternal'].isnull(), 'split_maternal'] = 0
        df.loc[df['split_maternal'] == 0, 'pct_maternal'] = 1
        df.loc[df['split_maternal'] == 0,
               'pct_maternal_hiv'] = df['pct_maternal_hiv_vr']
        df.loc[df['split_maternal'] == 0, 'pct_hiv'] = 0
        df.drop('pct_maternal_hiv_vr', axis=1, inplace=True)
        return df

    def create_maternal_hiv_cfs(self, df):
        """Create cause fractions for maternal hiv."""
        df = df.copy()

        maternal_hiv_df = df.copy()
        maternal_hiv_df['cf'] = maternal_hiv_df['cf'] * \
            maternal_hiv_df['pct_maternal_hiv']
        maternal_hiv_df['cause_id'] = 741
        maternal_hiv_df['cf_raw'] = 0
        maternal_hiv_df['cf_corr'] = 0
        maternal_hiv_df['cf_rd'] = 0

        maternal_df = df.copy()
        maternal_df['cf'] = maternal_df['cf'] * maternal_df['pct_maternal']
        maternal_df['cause_id'] = 366
        df = pd.concat([maternal_hiv_df, maternal_df], ignore_index=True)

        return df

    def clean_adjusted_data(self, df):
        """Clean up adjusted data to add on to the original dataset.

        Add maternal_hiv to maternal, keep the maternal_hiv,
        split_maternal 0 observations and call them maternal
        """
        va_vr = df.loc[df['split_maternal'] == 0]
        if len(va_vr) > 0:
            assert set([741, 366]) == set(va_vr.cause_id.unique())
            va_vr = va_vr.loc[va_vr['cause_id'] != 366]
            va_vr['cause_id'] = 366
        df = pd.concat([df, va_vr], ignore_index=True)
        df = df.groupby(
            ['nid', 'extract_type_id', 'location_id', 'year_id', 'site_id',
             'age_group_id', 'sex_id', 'sample_size', 'cause_id'],
            as_index=False
        )[self.all_cf_cols].sum()

        # it is possible that, using this method, cause fractions exceed 1.
        # this is meaningless and breaks noise reduction, so cap it
        # make sure that cf isn't something absurd, though
        assert (df['cf'] < 1.1).all()
        df.loc[df['cf'] > 1, 'cf'] = 1

        return df

    def append_adjusted_orig(self, orig, maternal_data, adjusted):
        """Remove original maternal data and append on adjusted."""
        data = orig.merge(maternal_data, how='left', indicator=True)
        data = data.loc[data['_merge'] != 'both']
        data.drop('_merge', axis=1, inplace=True)
        data = data.append(adjusted, ignore_index=True)
        return data
