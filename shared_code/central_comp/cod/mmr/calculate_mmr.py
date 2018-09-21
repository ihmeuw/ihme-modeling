import pandas as pd
import sys
import os

from db_queries import (get_covariate_estimates, get_population,
                        get_location_metadata)
from transmogrifier.gopher import draws
from adding_machine.summarizers import get_estimates


class MMR(object):
    def __init__(self, cause_id, year_id, out_dir):
        self.cause_id = [cause_id]
        self.year_id = [year_id]
        self.sex_id = [2]
        self.age_group_ids = range(7, 16)
        self.aggregated_age_group_ids = {24: range(8, 15), 169: range(7, 16),
                                         162: range(8, 10)}
        self.draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
        self.index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        self.out_dir = out_dir

    def output_mmr(self):
        codcorrect_draws = self.pull_codcorrect_draws()
        asfr = self.pull_asfr()
        pop = self.pull_pop()
        live_births = self.calculate_live_births(pop, asfr)
        loc_aggregated_live_births = AggregateLocations(
            [35, 40], live_births, ['live_births']).aggregate()
        all_aggregated_live_births = self.aggregate_ages(
            loc_aggregated_live_births)
        age_aggregated_codcorrect_draws = self.aggregate_ages(codcorrect_draws,
                                                              self.index_cols +
                                                              ['cause_id'])
        mmr_draws = self.calculate_mmr(age_aggregated_codcorrect_draws,
                                       all_aggregated_live_births)
        mmr_summaries = self.summarize(mmr_draws)
        self.save_mmr(mmr_draws, mmr_summaries)

    def add_upload_cols(self, df):
        df['measure_id'] = 25
        df['metric_id'] = 3
        df['cause_id'] = int(self.cause_id[0])
        return df

    def pull_codcorrect_draws(self):
        codcorrect_df = draws(gbd_ids={'cause_ids': self.cause_id},
                              year_ids=self.year_id,
                              source='codcorrect',
                              sex_ids=self.sex_id,
                              measure_ids=[1])
        codcorrect_df = codcorrect_df.ix[
            codcorrect_df.age_group_id.isin(
                self.age_group_ids + self.aggregated_age_group_ids.keys())]
        return codcorrect_df[self.index_cols + ['cause_id'] + self.draw_cols]

    def pull_asfr(self):
        asfr_df = get_covariate_estimates(covariate_id=13)
        asfr_df = asfr_df.ix[
            (asfr_df.age_group_id.isin(self.age_group_ids)) &
            (asfr_df.year_id.isin(self.year_id)) &
            (asfr_df.sex_id.isin(self.sex_id))]
        return asfr_df[self.index_cols + ['mean_value']]

    def pull_pop(self):
        pop_df = get_population(age_group_id=self.age_group_ids,
                                location_id=-1,
                                location_set_id=35,
                                year_id=self.year_id,
                                sex_id=self.sex_id)
        return pop_df[self.index_cols + ['population']]

    def calculate_live_births(self, pop, asfr):
        live_births = pop.merge(asfr, on=self.index_cols, how='inner')
        live_births['live_births'] = (live_births['mean_value'] *
                                      live_births['population'])
        return live_births[self.index_cols + ['live_births']]

    def aggregate_ages(self, to_agg_df, index_cols=None):
        if not index_cols:
            index_cols = self.index_cols
        agg_ages = []
        to_agg_df = to_agg_df.ix[~to_agg_df[
            'age_group_id'].isin(self.aggregated_age_group_ids.keys())]
        for agg_age in self.aggregated_age_group_ids.keys():
            temp = to_agg_df.ix[
                to_agg_df['age_group_id'].isin(
                    self.aggregated_age_group_ids[agg_age])].copy(deep=True)
            temp['age_group_id'] = agg_age
            temp = temp.groupby(index_cols).sum().reset_index()
            agg_ages.append(temp)
        agg_ages_df = pd.concat(agg_ages)
        return pd.concat([to_agg_df, agg_ages_df]).reset_index(drop=True)

    def calculate_mmr(self, deaths, live_births):
        mmr_df = pd.merge(deaths, live_births, on=self.index_cols, how='inner')
        for col in self.draw_cols:
            mmr_df[col] = (mmr_df[col] / mmr_df['live_births']) * 100000
        mmr_df = mmr_df[self.index_cols + ['cause_id'] + self.draw_cols]
        mmr_df = self.add_upload_cols(mmr_df)
        return mmr_df

    def summarize(self, mmr_draws):
        summaries = get_estimates(mmr_draws)
        summaries.drop('median', axis=1, inplace=True)
        summaries.rename(columns={'mean': 'val'}, inplace=True)
        return summaries

    def save_mmr(self, mmr_draws, mmr_summaries):
        mmr_draws.to_hdf(self.out_dir + r'/FILEPATH.h5'.format(
                         self.year_id[0], self.cause_id[0]),
                         key='draws', mode='w', format='table',
                         data_columns=self.index_cols + ['cause_id'])
        sort_cols = ['measure_id', 'year_id', 'location_id', 'sex_id',
                     'age_group_id', 'cause_id', 'metric_id']
        mmr_summaries.sort_values(by=sort_cols, inplace=True)
        mmr_summaries = mmr_summaries[sort_cols + ['val', 'upper', 'lower']]
        mmr_summaries.to_csv(self.out_dir + r'FILEPATH.csv'
                             .format(self.year_id[0], self.cause_id[0]),
                             index=False, encoding='utf-8')


class AggregateLocations(object):
    def __init__(self, location_set_ids, to_agg_df, data_cols):
        self.location_set_ids = location_set_ids
        self.to_agg_df = to_agg_df
        self.scalar_dir = ('FILEPATH')
        self.data_cols = data_cols
        self.index_cols = [col for col in self.to_agg_df
                           if col not in self.data_cols]

    def aggregate(self):
        aggregated_df_list = []
        df = self.to_agg_df.copy(deep=True)
        for lsid in self.location_set_ids:
            loc_df = self.pull_hierarchy(lsid)
            if lsid == 35:
                scalar_df = self.load_regional_scalars(
                    self.get_region_locs(loc_df))
            most_detailed = self.get_most_detailed(loc_df)
            self.check_missing_locations(df, loc_df, most_detailed)

            df = self.format_data(df)
            data = df.copy(deep=True)
            data = data.ix[data['location_id'].isin(most_detailed)]
            data = pd.merge(data, loc_df, on='location_id', how='left')
            max_level = data['level'].max()
            print max_level

            data = self.format_data(data)
            # loop through hierarchy levels and aggregate
            for level in xrange(max_level, 0, -1):
                print "Level:", level
                data = pd.merge(data, loc_df[['location_id',
                                              'level',
                                              'parent_id']],
                                on='location_id',
                                how='left')
                temp = data.ix[data['level'] == level].copy(deep=True)
                # if we're at the region level, use regional scalars
                if lsid == 35:
                    if level == 2:
                        temp = pd.merge(temp, scalar_df, on=['location_id',
                                                             'year_id',
                                                             'age_group_id',
                                                             'sex_id'],
                                        how='inner')
                        for col in self.data_cols:
                            temp[col] = (temp[col] * temp['scaling_factor'])
                        temp.drop('scaling_factor', axis=1, inplace=True)
                temp['location_id'] = temp['parent_id']
                print temp['location_id'].unique()
                temp = self.format_data(temp)
                temp = temp.groupby(self.index_cols).sum().reset_index()
                data = pd.concat([self.format_data(data), temp]
                                 ).reset_index(drop=True)
            final = data.copy(deep=True)
            if lsid == 40:
                final = self.keep_aggregates_only(final, lsid)
            final = final.drop_duplicates()
            aggregated_df_list.append(final)
        return pd.concat(aggregated_df_list)

    def pull_hierarchy(self, location_set_id):
        loc_df = get_location_metadata(location_set_id)
        return loc_df[['location_id', 'level', 'parent_id', 'most_detailed',
                       'location_type_id', 'path_to_top_parent']]

    def get_region_locs(self, loc_df):
        return (loc_df[loc_df["location_type_id"] == 6]
                ['location_id'].tolist())

    def load_regional_scalars(self, region_locs):
        scalar_list = []
        folders = os.listdir(self.scalar_dir)
        folders = filter(lambda a: 'archive' not in a, folders)
        folders = [int(f) for f in folders]
        folders.sort()
        inner_folder = int(folders[-1])
        scalar_dir = '{}/{}'.format(self.scalar_dir, inner_folder)

        for geo in region_locs:
            for year in range(1990, 2017):
                scalar_df = pd.read_stata('FILEPATH.dta'
                                          % (scalar_dir, geo, year),
                                          convert_categoricals=False,
                                          preserve_dtypes=False)
                scalar_list.append(scalar_df)
        scalars = pd.concat(scalar_list)
        return scalars

    def get_most_detailed(self, loc_df):
        return (loc_df.ix[loc_df['most_detailed'] == 1]['location_id']
                .drop_duplicates().tolist())

    def check_missing_locations(self, df, loc_df, most_detailed):
        check_df = df.merge(loc_df,
                            on='location_id',
                            how='left')
        check_df_most_detailed = (check_df[check_df.most_detailed == 1]
                                  ['location_id'].drop_duplicates().tolist())
        if len(set(most_detailed) - set(check_df_most_detailed)) > 0:
            return ValueError('The following locations are missing from draws '
                              '%s' % (', '.join(
                                  [str(x) for x in list(set(most_detailed) -
                                                        set(check_df_most_detailed))])))

    def format_data(self, df):
        df[self.index_cols] = df[self.index_cols].astype(int)
        keep_columns = self.index_cols + self.data_cols
        return df[keep_columns]

    def keep_aggregates_only(self, aggregated_df, location_set_id):
        loc_df = self.pull_hierarchy(location_set_id).query("most_detailed==1")
        loc_df['exists'] = 1
        df = aggregated_df.copy(deep=True)
        df = df.merge(loc_df[['location_id', 'exists']],
                      on='location_id', how='outer')
        df = df.loc[df.exists != 1]
        df.drop('exists', axis=1, inplace=True)
        return df


if __name__ == '__main__':
    cause_id, year_id, out_dir = sys.argv[1:4]
    year_id = int(year_id)
    cause_id = int(cause_id)
    MMR(cause_id, year_id, out_dir).output_mmr()
