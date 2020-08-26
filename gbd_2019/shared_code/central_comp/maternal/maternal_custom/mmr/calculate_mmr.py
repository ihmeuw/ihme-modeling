"""
calculate_mmr.py is a file resposible for creating MMR 
(where MMR = ((maternal deaths / births) * 100,000) for a specific
maternal cause and year. These outputs are saved at:

FILEPATH

The process is roughly as follows (and happens in the document_mmr() method):

    0. on initialization, finalize demographics such as:
        a. location_ids
        b. aggregated_age_group_ids
    1. pull codcorrect count-space draws for cause and year
    2. pulling live births from files in constants dirs
    3. aggregating codcorrect draws and live births by specified age groups
    5. calculating by-demographic MMR with the provided equation
    6. calculating summaries using core_maths.summarize.get_summary
    7. saving the dataframe and summaries in the output directory
"""

import argparse
import logging
import os
import pathlib
from typing import Dict, List

import pandas as pd

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from core_maths.summarize import get_summary
from db_queries import (
    get_covariate_estimates, get_location_metadata, get_population)
from draw_sources.draw_sources import DrawSource, DrawSink
from db_tools.ezfuncs import query
from gbd.decomp_step import decomp_step_id_from_decomp_step
from get_draws.api import get_draws
from hierarchies import dbtrees

import mmr_constants

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MMR(object):
    def __init__(
        self, cause_id, year_id,
        out_dir, cod_process_v,
        decomp_step, gbd_round_id,
        location_set_ids=mmr_constants.AGGREGATE_LOCATION_SET_IDS):

        self.cause_id = cause_id
        self.year_id = year_id
        self.out_dir = out_dir
        self.cod_process_v = cod_process_v
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id

        self.sex_id = [2]
        self.age_group_ids = list(range(7, 16))
        self.location_set_ids = location_set_ids
        self.location_ids = self.get_location_ids()
        self.aggregated_age_group_ids: Dict[int: List[int]] = {
            ag_id: [_id.id for _id in dbtrees.agetree(ag_id).leaves()]
            for ag_id in mmr_constants.AGGREGATE_AGE_GROUP_IDS
        }
        self.draw_cols = ['draw_{}'.format(i) for i in list(range(0, 1000))]
        self.index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        self.live_birth_col = mmr_constants.Columns.LIVE_BIRTH_VALUE_COL

    def get_location_ids(self):
        location_ids = []
        for lsid in self.location_set_ids:
            df = get_location_metadata(location_set_id=lsid,
                gbd_round_id=self.gbd_round_id)
            ls = df.location_id.unique().tolist()
            location_ids = location_ids + ls
        location_ids = list(set(location_ids))
        return list(set(location_ids))

    def output_mmr(self):
        logger.info("Output MMR")
        codcorrect_draws = self.pull_codcorrect_draws()
        loc_aggregated_live_births = self.get_births()
        all_aggregated_live_births = self.aggregate_ages(
            loc_aggregated_live_births)
        age_aggregated_codcorrect_draws = self.aggregate_ages(
            codcorrect_draws, self.index_cols + ['cause_id'])
        mmr_draws = self.calculate_mmr(age_aggregated_codcorrect_draws,
                                       all_aggregated_live_births)
        mmr_summaries = self.summarize_draws(mmr_draws)
        # if cause_id is total maternal disorders, copy results to all cause
        # (294), Maternal and NN disorders (962), and Group1 NCDS (295)
        tot_maternal = 366
        extra_causes = [294, 295, 962]
        if self.cause_id==tot_maternal:
            temp_draws = mmr_draws.copy()
            for extra_cause_id in extra_causes:
                temp_draws.loc[:,'cause_id'] = extra_cause_id
                all_summaries = mmr_summaries.copy()
                all_summaries.loc[:,'cause_id'] = extra_cause_id
                self.save_mmr(extra_cause_id, temp_draws, all_summaries)
        self.save_mmr(self.cause_id, mmr_draws, mmr_summaries)

    def add_upload_cols(self, df):
        logger.info("Add upload columns")
        df.loc[:, 'measure_id'] = 25
        df.loc[:, 'metric_id'] = 3
        df.loc[:, 'cause_id'] = int(self.cause_id)
        return df

    def pull_codcorrect_draws(self):
        logger.info("Pulling codcorrect draws...")
        codcorrect_df = get_draws(
            'cause_id', self.cause_id,
            year_id=self.year_id,
            source='codcorrect',
            sex_id=self.sex_id,
            measure_id=[1],
            location_id=self.location_ids,
            version_id=self.cod_process_v,
            decomp_step=self.decomp_step,
            gbd_round_id=self.gbd_round_id)
        logger.info("Successfully pulled codcorrect draws.")
        codcorrect_df = codcorrect_df.loc[
            codcorrect_df.age_group_id.isin(
                list(self.age_group_ids) +
                list(self.aggregated_age_group_ids.keys())),:]
        return codcorrect_df[self.index_cols + ['cause_id'] + self.draw_cols]

    def get_births(self):
        """
        Live births are saved in a hdf5 file keyed by year. Note that
        variable 'self.out_dir' is actually down the directory tree from
        the constants directory, so we use the '.parent' attribute of
        the path object.
        """
        all_live_births_filepath = (
            pathlib.Path(self.out_dir).parent /
            'constants' /
            mmr_constants.ALL_LIVE_BIRTHS_FILENAME)

        return pd.read_hdf(
            all_live_births_filepath,
            key=mmr_constants.ALL_LIVE_BIRTHS_FORMAT_KEY.format(self.year_id)
        )

    def aggregate_ages(self, to_agg_df, index_cols=None):
        logger.info("Aggregating ages")
        if not index_cols:
            index_cols = self.index_cols
        agg_ages = []
        to_agg_df = to_agg_df.loc[~to_agg_df[
            'age_group_id'].isin(self.aggregated_age_group_ids.keys())]
        for agg_age in self.aggregated_age_group_ids.keys():
            temp = to_agg_df.loc[
                to_agg_df['age_group_id'].isin(
                    self.aggregated_age_group_ids[agg_age])].copy(deep=True)
            temp['age_group_id'] = agg_age
            temp = temp.groupby(index_cols).sum().reset_index()
            agg_ages.append(temp)
        agg_ages_df = pd.concat(agg_ages)
        return pd.concat([to_agg_df, agg_ages_df]).reset_index(drop=True)

    def calculate_mmr(self, deaths, live_births):
        logger.info("Calculating MMR")
        mmr_df = pd.merge(deaths, live_births, on=self.index_cols, how='left',
            indicator=True)
        assert (mmr_df._merge=="both").all()
        for col in self.draw_cols:
            mmr_df.loc[:, col] = (
                mmr_df[col] / mmr_df[self.live_birth_col]) * 100000
        mmr_df = mmr_df[self.index_cols + ['cause_id'] + self.draw_cols]
        mmr_df = self.add_upload_cols(mmr_df)
        return mmr_df

    def summarize_draws(self, mmr_draws):
        logger.info("Summarizing MMR draws")
        summaries = get_summary(mmr_draws, self.draw_cols)
        summaries.drop('median', axis=1, inplace=True)
        summaries.rename(columns={'mean': 'val'}, inplace=True)
        return summaries

    def save_mmr(self, cause_id, mmr_draws, mmr_summaries):
        logger.info(
            f"Saving MMR draws to {self.out_dir}/draws/"
            f"draws_{self.year_id}_{cause_id}.h5")
        mmr_draws.to_hdf(
            os.path.join(self.out_dir,"draws",
                f"draws_{self.year_id}_{cause_id}.h5"),
            key="draws", mode="w", format="table",
            data_columns=self.index_cols + ["cause_id"]
        )
        sort_cols = ['measure_id', 'year_id', 'location_id', 'sex_id',
                     'age_group_id', 'cause_id', 'metric_id']
        mmr_summaries.sort_values(by=sort_cols, inplace=True)
        mmr_summaries = mmr_summaries[sort_cols + ['val', 'upper', 'lower']]
        logger.info(
            f"Saving MMR summaries to {self.out_dir}/summaries/"
            f"summaries_{self.year_id}_{cause_id}.csv"
        )
        mmr_summaries.to_csv(
            os.path.join(self.out_dir, "summaries",
                f"summaries_{self.year_id}_{cause_id}.csv"),
            index=False, encoding='utf-8'
        )


if __name__ == '__main__':
    os.umask(0o022)
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("cause_id", type=int)
    parser.add_argument("year_id", type=int)
    parser.add_argument("out_dir", type=str)
    parser.add_argument("cod_process_v", type=int)
    parser.add_argument("decomp_step", type=str)
    parser.add_argument("gbd_round_id", type=int)
    args = parser.parse_args()

    # run MMR calculation
    MMR(args.cause_id, args.year_id, args.out_dir, args.cod_process_v,
        args.decomp_step, args.gbd_round_id).output_mmr()
