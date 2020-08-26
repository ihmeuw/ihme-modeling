"""
Calculate_arc was originally created to generate annualized
rate of change, a measure of change in MMR output YoY.
"""

import argparse
import logging
import sys
import glob
import os

import pandas as pd

from core_maths.summarize import (get_summary, pct_change)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ARC(object):
    def __init__(self, cause_id, in_dir, out_dir):
        self.cause_id = cause_id
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                           'measure_id', 'metric_id', 'cause_id']
        self.draw_cols = ['draw_{}'.format(i) for i in range(1000)]
        self.year_tuples = self.create_year_tuples()

    def output_arc(self):
        mmr_draws = self.pull_mmr_draws()
        arc_draws = self.calculate_pct_change(mmr_draws)
        arc_draws = self.calculate_significance(arc_draws)
        arc_summaries = self.summarize_draws(arc_draws)
        # if cause_id is total maternal disorders, copy results to all cause
        # and output results for both
        tot_maternal = 366
        all_cause = 294
        extra_causes = [294, 295, 962]
        if self.cause_id==tot_maternal:
            temp_draws = mmr_draws.copy()
            for extra_cause_id in extra_causes:
                temp_draws.loc[:,'cause_id'] = extra_cause_id
                all_summaries = arc_summaries.copy()
                all_summaries.loc[:,'cause_id'] = extra_cause_id
                self.save_arc(extra_cause_id, all_summaries)
        self.save_arc(self.cause_id, arc_summaries)

        if self.cause_id==tot_maternal:
            all_summaries = arc_summaries.copy()
            for extra_cause_id in extra_causes:
                all_summaries.loc[:,'cause_id'] = extra_cause_id
                self.save_arc(extra_cause_id, all_summaries)
        self.save_arc(self.cause_id, arc_summaries)

    def read_draws(self, filepath):
        return pd.read_hdf(filepath, 'draws',
                           where='age_group_id=[22, 24, 162, 169]')

    def pull_mmr_draws(self):
        logger.info(f'pulling mmr draws from {self.in_dir}')
        files = glob.glob(self.in_dir + '*_{}.h5'.format(self.cause_id))
        mmr_list = map(self.read_draws, files)
        return pd.concat(mmr_list)

    def create_year_tuples(self):
        year_tuples = (
            (1990, 2019),
            (2010, 2019),
            (1990, 2010)
        )
        return year_tuples
    
    def create_arc_year_dict(self):
        """DEPRECATED: for calculating arc years"""
        year_dict = {}
        for year in range(1990, 2017):
            year_dict[year] = [year + 1]
            if year in [1990, 1995, 2000, 2005, 2010, 2013]:
                year_dict[year].append(2017)
                if year < 2000:
                    year_dict[year].append(2000)

    def calculate_pct_change(self, mmr_draws):
        logger.info(f'calculating percent change')
        change_list = []
        for start_year, end_year in self.year_tuples:
            df = mmr_draws.loc[mmr_draws.year_id.isin(
                    [start_year, end_year]),:].copy(deep=True)
            change_list.append(pct_change(df, start_year, end_year,
                time_col='year_id', data_cols=self.draw_cols))
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

    def summarize_draws(self, arc_draws):
        self.index_cols.extend(['year_start_id', 'year_end_id'])
        self.index_cols.remove('year_id')
        summaries_mean = arc_draws[self.index_cols + ['pct_change_means']]
        summaries = get_summary(arc_draws, self.draw_cols)
        summaries.drop(['median','index','pct_change_means'], axis=1,
            inplace=True)
        summaries = summaries.merge(summaries_mean, on=self.index_cols)
        summaries.rename(columns={'pct_change_means': 'val'}, inplace=True)
        return summaries

    def save_arc(self, cause_id, arc_summaries):
        logger.info(
            f'saving annualized rate of change to {self.out_dir}/summaries')
        sort_cols = ['measure_id', 'year_start_id', 'year_end_id',
                     'location_id', 'sex_id', 'age_group_id', 'cause_id',
                     'metric_id']
        arc_summaries.sort_values(by=sort_cols, inplace=True)
        arc_summaries = arc_summaries[sort_cols + ['val', 'upper', 'lower']]
        arc_summaries.to_csv(
            os.path.join(self.out_dir, "summaries",
                f"summaries_{cause_id}.csv"),
            index=False, encoding='utf-8')


if __name__ == '__main__':
    os.umask(0o022)
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("cause_id", type=int)
    parser.add_argument("out_dir", type=str)
    args = parser.parse_args()
    logger.info(f'calculating arc with args {vars(args)}')

    cause_id = int(args.cause_id)
    in_dir = args.out_dir.replace("multi_year", "") + "/single_year/draws/"
    ARC(cause_id, in_dir, args.out_dir).output_arc()
