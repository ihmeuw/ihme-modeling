import pandas as pd
from configurator import Configurator
from cod_process import CodProcess

from cod_prep.downloaders import (
    add_cause_metadata,
    add_location_metadata,
    get_country_level_location_id,
    add_envelope,
    add_population
)
from cod_prep.utils.misc import report_if_merge_fail, report_duplicates
pd.options.mode.chained_assignment = None


class CauseAggregator(CodProcess):

    cf_cols = ['cf', 'cf_rd', 'cf_corr', 'cf_raw']

    def __init__(self, df, cause_meta_df, source):
        self.df = df
        self.cause_meta_df = cause_meta_df
        self.source = source
        self.injuries_sources = [
            "UNODC_Homicides", "UN_CTS_Homicides",
            "Various_RTI", "GSRRS_Bloomberg_RTI"
        ]

    def get_computed_dataframe(self):
        """Return computations."""
        if self.source in self.injuries_sources:
            df = self.level_3_aggregate()
        else:
            df = self.simple_aggregate()
        agg_dict = {}
        for col in self.cf_cols:
            agg_dict.update({col: 'sum'})
        agg_dict.update({'sample_size': 'mean'})
        df = df.groupby(['nid', 'extract_type_id', 'location_id', 'year_id',
                         'site_id', 'age_group_id', 'sex_id', 'cause_id'],
                        as_index=False).agg(agg_dict)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        # important to run this full method first
        df = self.get_computed_dataframe()
        # cause metadata was changed in the process, so merge that on again
        # add on cause metadata to see parent_ids and cause levels
        df = add_cause_metadata(df, ['parent_id', 'level'],
                                merge_col='cause_id',
                                cause_meta_df=self.cause_meta_df)
        return df

    def simple_aggregate(self):
        """Aggregate causes."""
        df = add_cause_metadata(
            self.df,
            ['secret_cause', 'parent_id', 'level'],
            merge_col='cause_id',
            cause_meta_df=self.cause_meta_df
        )
        # quick check that there are no secret causes
        secret_causes = df.loc[df['secret_cause'] == 1]
        if len(secret_causes) > 0:
            raise AssertionError(
                "The following secret causes are still "
                "in the data: \n{}".format(secret_causes['cause_id'].unique()))
        cause_levels = sorted(range(2, 6, 1), reverse=True)
        
        for level in cause_levels:
            level_df = df[df['level'] == level]
            if len(level_df) > 0:
                # replace the cause_id with the parent_id
                level_df['cause_id'] = level_df['parent_id']
                level_df['level'] = df['level'] - 1
                level_df.drop('parent_id', axis=1, inplace=True)

                level_df = add_cause_metadata(
                    level_df,
                    ['parent_id'],
                    merge_col=['cause_id'],
                    cause_meta_df=self.cause_meta_df
                )
                # add in deaths by each level
                df = pd.concat([level_df, df], ignore_index=True)
        return df

    def level_3_aggregate(self):

        df = add_cause_metadata(
            self.df,
            ['secret_cause', 'parent_id', 'level'],
            merge_col='cause_id',
            cause_meta_df=self.cause_meta_df
        )
        # quick check that there are no secret causes
        secret_causes = df.loc[df['secret_cause'] == 1]
        if len(secret_causes) > 0:
            raise AssertionError(
                "The following secret causes are still "
                "in the data: \n{}".format(secret_causes['cause_id'].unique()))

        for level in [5, 4, 3]:
            level_df = df[df['level'] == level]
            if len(level_df) > 0:
                # replace the cause_id with the parent_id
                level_df['cause_id'] = level_df['parent_id']
                level_df['level'] = level_df['level'] - 1
                level_df.drop('parent_id', axis=1, inplace=True)
                # add parent_id back in for the newly changed cause_id
                # tried with mapping and this was faster
                level_df = add_cause_metadata(
                    level_df,
                    ['parent_id'],
                    merge_col=['cause_id'],
                    cause_meta_df=self.cause_meta_df
                )
                # add in deaths by each level
                df = pd.concat([level_df, df], ignore_index=True)
        return df


class LocationAggregator(CodProcess):
    """Aggregate Location."""

    val_cols = ['deaths', 'deaths_rd', 'deaths_corr', 'deaths_raw']

    def __init__(self, df, location_meta_df):
        self.df = df
        self.location_meta_df = location_meta_df
        self.conf = Configurator('standard')
        self.nid_replacements = self.conf.get_resource('nid_replacements')

    def get_computed_dataframe(self, type='simple'):
        """Return computations."""
        if type != 'simple':
            df = self.aggregate_locations()
        else:
            df = self.simple_aggregate()
        df = self.change_nid_for_aggregates(df)
        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics."""
        pass

    def simple_aggregate(self):
        """Aggregate location_ids to country level."""
        df = self.df.copy()
        country_location_ids = \
            get_country_level_location_id(df.location_id.unique(),
                                          self.location_meta_df)
        df = df.merge(country_location_ids, how='left', on='location_id')
        report_if_merge_fail(df, 'country_location_id', ['location_id'])
        df = df[df['location_id'] != df['country_location_id']]
        df['location_id'] = df['country_location_id']
        df = df.drop(['country_location_id'], axis=1)

        # want to collapse site_id for national level
        group_cols = [
            col for col in df.columns if col not in self.val_cols
        ]
        group_cols.remove('site_id')
        df = df.groupby(group_cols, as_index=False)[self.val_cols].sum()

        # set site_id for national aggregates (cannot be missing)
        df['site_id'] = 2

        # append national aggregates to the incoming dataframe
        df = df.append(self.df)

        return df

    def aggregate_locations(self):
        """Aggregate sub national location_ids up to the country level."""
        df = add_location_metadata(
            self.df,
            ['location_id', 'path_to_top_parent', 'level'],
            merge_col='location_id',
            location_meta_df=self.location_meta_df
        )
        # don't need larger than country level aggregation
        loc_levels = [x for x in df['level'].unique() if df['level'] >= 3]
        loc_levels.sort()
        loc_levels.reverse()
        for level in loc_levels:

            level_df = df[df['level'] == level]
            if len(level_df) > 0:
                # replace the location with the parent_id
                level_df['location_id'] = level_df['parent_id']
                level_df['level'] = df['level'] - 1
                level_df.drop('parent_id', axis=1, inplace=True)
 
                level_df = add_location_metadata(
                    level_df,
                    ['parent_id'],
                    merge_col=['location_id'],
                    location_meta_df=self.location_meta_df
                )
                # add in deaths by each level
                df = df.append(level_df, ignore_index=True)
            return df

    def change_nid_for_aggregates(self, df):
        """Change NIDs for aggregates."""
        nid_df = pd.read_csv(self.nid_replacements)
        nid_df.rename(columns={
            'match_location_id': 'location_id',
            'Old NID': 'nid',
            'NID': 'new_nid'},
            inplace=True)
        nid_df = nid_df[['location_id', 'nid', 'new_nid']]
        start_length = len(df)
        df = df.merge(nid_df, how='left', on=['location_id', 'nid'])
        # replace 103215 nid with the ones in the nid_df
        df.loc[df['new_nid'].notnull(), 'nid'] = df['new_nid']
        df = df.drop('new_nid', axis=1)
        # assert nid is not missing
        if df['nid'].isnull().any():
            raise AssertionError('There are observations with missing nids')
        # assert 103215 is not present
        if len(df.loc[df['nid'] == 103215]) > 0:
            raise AssertionError('There are observations with nid 103215')
        # make sure no observations have been dropped
        end_length = len(df)
        if start_length != end_length:
            raise AssertionError(
                'Observations have either '
                'been added or dropped: {}'.format(start_length - end_length)
            )
        return df

    def china_aggregate(self):
        raise NotImplementedError

    def india_aggregate(self):

        raise NotImplementedError

    def utla_aggregate(self):

        raise NotImplementedError

    def us_aggregate(self):

        raise NotImplementedError


class AgeAggregator(CodProcess):
    """Aggregate age groups, creating all ages and age-standardized groups."""

    def __init__(self, df, pop_df, env_df, age_weight_df):
        self.df = df
        self.pop_df = pop_df
        self.env_df = env_df
        self.age_weight_df = age_weight_df
        self.cf_final_col = ['cf_final']
        self.draw_cols = [x for x in self.df.columns if 'cf_draw_' in x]
        if len(self.draw_cols) > 0:
            self.cf_final_col = self.draw_cols + ['cf_final']
        self.cf_cols = ['cf_raw', 'cf_rd', 'cf_corr'] + self.cf_final_col
        self.deaths_cols = ['deaths' + x.split('cf')[1] for x in self.cf_cols]
        self.id_cols = ['nid', 'extract_type_id', 'location_id', 'site_id',
                        'year_id', 'age_group_id', 'sex_id', 'cause_id']

    def get_computed_dataframe(self):
        """Return computations."""
        df = self.df.copy()

        all_age_df = self.make_all_ages_group(df)

        # get map of age_group_id: weights
        age_weight_dict = self.age_weight_df.drop_duplicates(
            ['age_group_id', 'age_group_weight_value']
        ).set_index('age_group_id')['age_group_weight_value'].to_dict()
        # create age standardized age group
        age_standard_df = self.make_age_standardized_group(df, age_weight_dict)

        assert len(age_standard_df) == len(all_age_df), \
            "Age standardized and all ages dataframes are different lengths"

        # append incoming df, all ages, and age standardized
        df = pd.concat([self.df, all_age_df, age_standard_df],
                       ignore_index=True)

        report_duplicates(df, self.id_cols)

        return df

    def make_deaths(self, df):
        if 'mean_env' not in df.columns:
            df = add_envelope(df, env_df=self.env_df)
            report_if_merge_fail(df, 'mean_env', ['sex_id', 'age_group_id',
                                                  'year_id', 'location_id'])
        for cf_col in self.cf_cols:
            df['deaths' + cf_col.split('cf')[1]] = df[cf_col] * df['mean_env']

        df = df.drop('mean_env', axis=1)

        return df

    def make_cause_fractions(self, df):
        if 'mean_env' not in df.columns:
            df = add_envelope(df, env_df=self.env_df)
            report_if_merge_fail(df, 'mean_env', ['sex_id', 'age_group_id',
                                                  'year_id', 'location_id'])
        for col in self.deaths_cols:
            df['cf' + col.split('deaths')[1]] = df[col] / df['mean_env']
        df = df.drop('mean_env', axis=1)
        return df

    def make_all_ages_group(self, df):
        df = df.copy()
        df = self.make_deaths(df)
        df = df.drop(self.cf_cols, axis=1)
        df['age_group_id'] = 22
        df = df.groupby(
            self.id_cols, as_index=False
        )[self.deaths_cols + ['sample_size']].sum()
        df = self.make_cause_fractions(df)
        df = df.drop(self.deaths_cols, axis=1)

        report_duplicates(df, self.id_cols)

        return df

    def make_age_standardized_group(self, df, age_weight_dict):
        df = df.copy()
        # get the age weights
        df['weight'] = df['age_group_id'].map(age_weight_dict)
        report_if_merge_fail(df, 'weight', 'age_group_id')

        df = add_population(df, pop_df=self.pop_df)
        report_if_merge_fail(df, "population", ['sex_id', 'age_group_id',
                                                'year_id', 'location_id'])
        df = add_envelope(df, env_df=self.env_df)
        report_if_merge_fail(df, "mean_env", ['sex_id', 'age_group_id',
                                              'year_id', 'location_id'])

        # age standardized deaths rates = deaths / population * weight
        for col in self.cf_cols:
            df[col] = (
                (df[col] * df['mean_env']) / df['population']
            ) * df['weight']

        df['age_group_id'] = 27
        df = df.drop(['weight', 'population', 'mean_env'], axis=1)
        df = df.groupby(
            self.id_cols, as_index=False
        )[self.cf_cols + ['sample_size']].sum()

        report_duplicates(df, self.id_cols)

        return df

    def get_diagnostic_dataframe(self):
        """Return diagnostics"""
        pass
