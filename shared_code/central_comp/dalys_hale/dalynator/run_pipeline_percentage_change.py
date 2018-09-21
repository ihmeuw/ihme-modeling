import gc
import logging
import time
import pandas as pd
from functools import partial

import gbd.constants as gbd

import dalynator.write_summaries_multi as write_sum
import dalynator.get_input_args as get_input_args
from dalynator.compute_percentage_change import ComputePercentageChange
from dalynator.data_container import DataContainer
from dalynator.run_pipeline_burdenator import aggregate_ages, \
    aggregate_sexes, back_calc_pafs, convert_to_rates, MPGlobals

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE pipeline percentage change complete"
logger = logging.getLogger(__name__)


def compute_aggregates(df, n_draws, tool_name):
    """WARNING: THIS ALTERS DF!"""
    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))

    agga = partial(aggregate_ages, index_cols=index_cols,
                   draw_cols=draw_cols)
    aggs = partial(aggregate_sexes, index_cols=index_cols,
                   draw_cols=draw_cols)

    # Aggregate sex/age in numbers
    df = pd.concat([df, aggs(df)])
    df = pd.concat([df, agga(df)])

    # Calculate rate space
    rate_df = convert_to_rates(df)

    # Back-calculated paf summaries, but only for the burdenator
    if "burdenator" in tool_name:
        paf_df = back_calc_pafs(df, n_draws)
        paf_df['metric_id'] = gbd.metrics.PERCENT
        # Stitch them all together
        df = pd.concat([df, rate_df, paf_df])
    else:
        logger.debug("Do not back-calculate PAFs for the Dalynator")

    return df


class PercentageChangePipeline(object):

    def __init__(self, args):
        self.args = args
        start_time = time.time()
        MPGlobals.logger = logger
        logger.info(
            "START percentage-change pipeline at {} on location {}, "
            "start-year {}," "end-year {}, measure_id {}" .format(
                start_time, args.location_id, args.start_year, args.end_year,
                args.measure_id))
        self.start_time = start_time

    def set_drawdir_filenames(self, draw_dir=None,
                              start_year_fn=None, end_year_fn=None):
        if not draw_dir:
            self.draw_dir = "{}/draws/{}".format(
                self.args.out_dir, self.args.location_id)
        else:
            self.draw_dir = draw_dir

        if not start_year_fn:
            self.start_year_fn = get_input_args.calculate_output_filename(
                self.draw_dir, self.args.measure_id, self.args.location_id,
                self.args.start_year)
        else:
            self.start_year_fn = start_year_fn

        if not end_year_fn:
            self.end_year_fn = get_input_args.calculate_output_filename(
                self.draw_dir, self.args.measure_id, self.args.location_id,
                self.args.end_year)
        else:
            self.end_year_fn = end_year_fn

    def read_draws(self, where=None):

        if not where:
            where = "metric_id == {}".format(gbd.metrics.NUMBER)

        self.start_year_df = pd.read_hdf(self.start_year_fn, where=where)
        self.end_year_df = pd.read_hdf(self.end_year_fn, where=where)

    def instantiate_data_containers(self, cache_dir=None):
        if not cache_dir:
            self.cache_dir = '{}/cache'.format(self.args.out_dir)
        else:
            self.cache_dir = cache_dir

        self.data_container_start = DataContainer(
            location_id=self.args.location_id,
            year_id=self.args.start_year,
            gbd_round_id=self.args.gbd_round_id,
            cache_dir=self.cache_dir,
            n_draws=self.args.n_draws)
        self.data_container_end = DataContainer(
            location_id=self.args.location_id,
            year_id=self.args.end_year,
            gbd_round_id=self.args.gbd_round_id,
            cache_dir=self.cache_dir,
            n_draws=self.args.n_draws)

    def compute_aggregates(self):
        MPGlobals.data_container = self.data_container_start
        self.start_year_df = compute_aggregates(
            self.start_year_df, self.args.n_draws, self.args.tool_name)
        MPGlobals.data_container = self.data_container_end
        self.end_year_df = compute_aggregates(
            self.end_year_df, self.args.n_draws, self.args.tool_name)
        logger.debug("percentage-change pipeline computed start and end"
                     " aggregates")

    def compute_percentage_change(self, df=None):
        if not df:
            df = pd.concat([self.start_year_df, self.end_year_df])
        self.df_shape = df.shape
        self.draw_cols = list(df.filter(like='draw').columns)
        self.index_cols = list(set(df.columns) - set(self.draw_cols))

        cpc = ComputePercentageChange(df, self.args.start_year,
                                      self.args.end_year,
                                      self.index_cols, self.draw_cols)
        self.pcp_df = cpc.get_data_frame()
        logger.debug("percentage-change pipeline computed percentage change")

    def collect_garbage(self):
        # Clean up a little, summarization can eat up memory
        del self.start_year_df
        del self.end_year_df
        del self.data_container_start
        del self.data_container_end
        gc.collect()

    def write_summaries(self, csv_dir=None):
        ''' Calculate and write out the year summaries as CSV files'''
        if not csv_dir:
            self.csv_dir = "{}/draws/{}/upload/".format(
                self.args.out_dir, self.args.location_id)
        else:
            self.csv_dir = csv_dir

        write_sum.write_summaries_multi(
            self.args.location_id, self.args.start_year, self.args.end_year,
            self.csv_dir, self.pcp_df, self.index_cols)

        end_time = time.time()
        elapsed = end_time - self.start_time
        logger.info("{} at {}, elapsed seconds= {}".format(SUCCESS_LOG_MESSAGE,
                                                           end_time, elapsed))


def run_pipeline_percentage_change(args):
    pct = PercentageChangePipeline(args)
    pct.set_drawdir_filenames()
    pct.read_draws()
    pct.instantiate_data_containers()
    pct.compute_aggregates()
    pct.compute_percentage_change()
    pct.collect_garbage()
    pct.write_summaries()
    return pct.df_shape


def main():
    parser = get_input_args.construct_parser_pct_change()
    args = get_input_args.get_args_pct_change(parser)

    run_pipeline_percentage_change(args)


if __name__ == "__main__":
    main()
