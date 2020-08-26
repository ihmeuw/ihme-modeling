import gc
import logging
import time
from functools import partial

import gbd.constants as gbd
import pandas as pd

from dalynator import data_container as dc
from dalynator import get_input_args as get_input_args
from dalynator import write_summaries_multi as write_sum
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_pct_change import ComputePctChange
from dalynator.data_container import DataContainer
from dalynator.tasks.run_pipeline_burdenator_most_detailed import \
    aggregate_ages, aggregate_sexes, back_calc_pafs, convert_to_rates, \
    MPGlobals
from dalynator.compute_summaries import MetricConverter

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE pipeline pct change complete"
logger = logging.getLogger('dalynator.tasks.run_pipeline_pct_change')


def compute_aggregates(df, n_draws, tool_name):
    """WARNING: THIS ALTERS DF!"""
    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))

    agga = partial(aggregate_ages, index_cols=index_cols,
                   draw_cols=draw_cols)
    aggs = partial(aggregate_sexes, index_cols=index_cols,
                   draw_cols=draw_cols)

    # Aggregate sex/age in numbers
    df = pd.concat([df, aggs(df)], sort=True)
    df = pd.concat([df, agga(df)], sort=True)

    if "burdenator" in tool_name:
        # Calculate rate space
        rate_df = convert_to_rates(df)
        dc.add_star_id(df)
        # Convert risk attr burden in count space to pct space (%)
        pct_df = back_calc_pafs(df, n_draws)
        df = pd.concat([df, rate_df, pct_df], sort=True)
    else:
        df = MetricConverter(df, to_rate=True, to_percent=True,
                             data_container=MPGlobals.data_container).get_data_frame()
    return df


class PctChangePipeline(object):

    def __init__(self, args):
        self.args = args
        start_time = time.time()
        MPGlobals.logger = logger
        logger.info(
            "START pct-change pipeline at {} on location {}, "
            "start-year {}," "end-year {}, measure_id {}" .format(
                start_time, args.location_id, args.start_year, args.end_year,
                args.measure_id))
        self.start_time = start_time

    def set_drawdir_filenames(self, draw_dir=None,
                              start_year_fn=None, end_year_fn=None):
        if not draw_dir:
            self.draw_dir = "FILEPATH".format(
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

        md_ages = self.data_container_start['age_spans'].age_group_id

        sydf = pd.read_hdf(self.start_year_fn, where=where)
        if "burdenator" in self.args.tool_name:
            dc.add_star_id(sydf)
            # add cause envelope without risks to back calc pafs
            sydf = pd.concat([sydf, self.read_all_cause(
                self.args.start_year)],
                             sort=True)
        # Only keep most-detailed age/sexes
        sydf = sydf[sydf['sex_id'] != gbd.sex.BOTH]
        sydf = sydf[sydf.age_group_id.isin(md_ages)]
        self.start_year_df = sydf

        eydf = pd.read_hdf(self.end_year_fn, where=where)
        if "burdenator" in self.args.tool_name:
            dc.add_star_id(eydf)
            # add cause envelope without risks to back calc pafs
            eydf = pd.concat([eydf, self.read_all_cause(self.args.end_year)],
                             sort=True)
        # Only keep most-detailed age/sexes
        eydf = eydf[eydf['sex_id'] != gbd.sex.BOTH]
        eydf = eydf[eydf.age_group_id.isin(md_ages)]
        self.end_year_df = eydf

    def instantiate_data_containers(self, cache_dir=None):
        if not cache_dir:
            self.cache_dir = 'FILEPATH'.format(self.args.out_dir)
        else:
            self.cache_dir = cache_dir

        self.data_container_start = DataContainer(
            {'location_id': self.args.location_id,
             'year_id': self.args.start_year},
            gbd_round_id=self.args.gbd_round_id,
            decomp_step=self.args.decomp_step,
            cache_dir=self.cache_dir,
            n_draws=self.args.n_draws)
        self.data_container_end = DataContainer(
            {'location_id': self.args.location_id,
             'year_id': self.args.end_year},
            gbd_round_id=self.args.gbd_round_id,
            decomp_step=self.args.decomp_step,
            cache_dir=self.cache_dir,
            n_draws=self.args.n_draws)

    def read_all_cause(self, year_id):
        ''' pull cause envelope for given year
        this will only be called when running the burdenator as it is
        needed to back calculate pafs (used as the denominator)
        and generate multi year (pct change) estimates in pct space '''
        cause_data_container = DataContainer(
            {'location_id': self.args.location_id,
             'year_id': year_id},
            gbd_round_id=self.args.gbd_round_id,
            decomp_step=self.args.decomp_step,
            cache_dir=self.cache_dir,
            n_draws=self.args.n_draws,
            cod_dir=self.args.cod_dir,
            cod_pattern=self.args.cod_pattern,
            epi_dir=self.args.epi_dir)
        if self.args.measure_id == gbd.measures.DEATH:
            cause_df = cause_data_container['death']
        elif self.args.measure_id == gbd.measures.YLL:
            cause_df = cause_data_container['yll']
        elif self.args.measure_id == gbd.measures.YLD:
            cause_df = cause_data_container['yld']
        elif self.args.measure_id == gbd.measures.DALY:
            # Get YLLs and YLDs
            yll_df = cause_data_container['yll']
            yld_df = cause_data_container['yld']
            yld_df = yld_df.loc[yld_df.measure_id == gbd.measures.YLD]
            # Compute DALYs
            draw_cols = list(yld_df.filter(like='draw').columns)
            index_cols = list(set(yld_df.columns) - set(draw_cols))
            daly_df = ComputeDalys(yll_df, yld_df, draw_cols, index_cols)
            cause_df = daly_df.get_data_frame()
        cause_df['rei_id'] = gbd.risk.TOTAL_ATTRIBUTABLE
        cause_df['star_id'] = gbd.star.ANY_EVIDENCE_LEVEL
        return cause_df.loc[cause_df['metric_id'] == gbd.metrics.NUMBER]

    def compute_aggregates(self):
        MPGlobals.data_container = self.data_container_start
        self.start_year_df = compute_aggregates(
            self.start_year_df, self.args.n_draws, self.args.tool_name)
        MPGlobals.data_container = self.data_container_end
        self.end_year_df = compute_aggregates(
            self.end_year_df, self.args.n_draws, self.args.tool_name)
        logger.debug("pct-change pipeline computed start and end"
                     " aggregates")

    def compute_pct_change(self, df=None):
        if not df:
            df = pd.concat([self.start_year_df, self.end_year_df])
        self.df_shape = df.shape
        self.draw_cols = list(df.filter(like='draw').columns)
        self.index_cols = list(set(df.columns) - set(self.draw_cols))

        cpc = ComputePctChange(df, self.args.start_year, self.args.end_year,
                               self.index_cols, self.draw_cols)
        self.pcp_df = cpc.get_data_frame()
        logger.debug("pct-change pipeline computed pct change")

    def collect_garbage(self):
        # Clean up a little, summarization can eat up memory
        del self.start_year_df
        del self.end_year_df
        del self.data_container_start
        del self.data_container_end
        gc.collect()

    def write_summaries(self, csv_dir=None):
        """ Calculate and write out the year summaries as CSV files"""
        if not csv_dir:
            self.csv_dir = "FILEPATH".format(
                self.args.out_dir, self.args.location_id)
        else:
            self.csv_dir = csv_dir

        write_sum.write_summaries_multi(
            self.args.location_id, self.args.start_year, self.args.end_year,
            self.csv_dir, self.pcp_df, self.index_cols,
            self.args.write_out_star_ids, self.args.dual_upload)

        end_time = time.time()
        elapsed = end_time - self.start_time
        logger.info("{} at {}, elapsed seconds= {}".format(SUCCESS_LOG_MESSAGE,
                                                           end_time, elapsed))


def run_pipeline_pct_change(args):
    pct = PctChangePipeline(args)
    pct.set_drawdir_filenames()
    pct.instantiate_data_containers()
    pct.read_draws()
    pct.compute_aggregates()
    pct.compute_pct_change()
    pct.collect_garbage()
    pct.write_summaries()
    return pct.df_shape


def main():
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_pct_change()
    args = get_input_args.get_args_pct_change(parser)

    run_pipeline_pct_change(args)


if __name__ == "__main__":
    main()
