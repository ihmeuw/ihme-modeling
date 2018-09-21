import pandas as pd
import sys
import glob

from adding_machine.summarizers import pct_change, get_estimates


class ARC(object):
    def __init__(self, cause_id, in_dir, out_dir):
        self.cause_id = cause_id
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                           'measure_id', 'metric_id', 'cause_id']
        self.draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
        self.year_dict = self.create_year_dict()

    def output_arc(self):
        mmr_draws = self.pull_mmr_draws()
        arc_draws = self.calculate_pct_change(mmr_draws)
        arc_draws = self.calculate_significance(arc_draws)
        arc_summaries = self.summarize(arc_draws)
        self.save_arc(arc_summaries)

    def read_draws(self, filepath):
        return pd.read_hdf(filepath, 'draws',
                           where='age_group_id=[24, 162, 169]')

    def pull_mmr_draws(self):
        files = glob.glob(self.in_dir + '*_{}.h5'.format(self.cause_id))
        mmr_list = map(self.read_draws, files)
        return pd.concat(mmr_list)

    def create_year_dict(self):
        year_dict = {}
        for year in range(1990, 2016):
            year_dict[year] = [year + 1]
            if year in [1990, 1995, 2000, 2005, 2010, 2013]:
                year_dict[year].append(2016)
                if year < 2000:
                    year_dict[year].append(2000)
        return year_dict

    def calculate_pct_change(self, mmr_draws):
        change_list = []
        for start_year in self.year_dict.keys():
            for end_year in self.year_dict[start_year]:
                df = mmr_draws.ix[mmr_draws.year_id.isin([start_year, end_year]
                                                         )].copy(deep=True)
                change_list.append(pct_change(df, start_year, end_year,
                                              change_type='arc'))
        return pd.concat(change_list)

    def calculate_significance(self, arc_draws):
        metric_dict = {6: 0, 7: .0554}
        for metric in metric_dict:
            sig_df = arc_draws.copy(deep=True)
            sig_df['count'] = (sig_df[self.draw_cols] <
                               metric_dict[metric]).sum(1)
            for col in self.draw_cols + ['pct_change_means']:
                sig_df[col] = (1 - (sig_df['count'] / 1000))
            sig_df['metric_id'] = metric
            sig_df.drop(['count'], axis=1, inplace=True)
            arc_draws = arc_draws.append(sig_df)
        arc_draws.reset_index(inplace=True)
        return arc_draws

    def summarize(self, arc_draws):
        self.index_cols.extend(['year_start_id', 'year_end_id'])
        self.index_cols.remove('year_id')
        summaries_mean = arc_draws[self.index_cols + ['pct_change_means']]
        summaries = get_estimates(arc_draws[self.index_cols + self.draw_cols])
        summaries = summaries.merge(summaries_mean, on=self.index_cols)
        summaries.rename(columns={'pct_change_means': 'val'}, inplace=True)
        return summaries

    def save_arc(self, arc_summaries):
        sort_cols = ['measure_id', 'year_start_id', 'year_end_id',
                     'location_id', 'sex_id', 'age_group_id', 'cause_id',
                     'metric_id']
        arc_summaries.sort_values(by=sort_cols, inplace=True)
        arc_summaries = arc_summaries[sort_cols + ['val', 'upper', 'lower']]
        arc_summaries.to_csv(self.out_dir + r'/summaries/summaries_{}.csv'
                             .format(self.cause_id), index=False,
                             encoding='utf-8')


if __name__ == '__main__':
    cause_id, out_dir = sys.argv[1:3]
    cause_id = int(cause_id)
    in_dir = out_dir.replace("multi_year", "") + "/single_year/draws/"
    ARC(cause_id, in_dir, out_dir).output_arc()
