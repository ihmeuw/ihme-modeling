import contextlib
import json
import logging
import os
import time

import pandas as pd
from aggregator.aggregators import AggMemEff
from aggregator.operators import Sum
from dataframe_io.io_queue import IOQueue
from draw_sources.draw_sources import SourceSinkPair
from gbd import constants as gbd

from dalynator import get_input_args
from dalynator import makedirs_safely as mkds
from dalynator.computation_element import ComputationElement
from dalynator.data_container import DataContainer
from dalynator.data_container import remove_unwanted_stars

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

logger = logging.getLogger("dalynator.tasks.run_pipeline_burdenator_loc_agg")


def create_default_scalars(location_id, year_id):
    """
    Returns regional scalars of 1.0.
    """
    df = pd.DataFrame({'location_id': [location_id], 'year_id': [year_id],
                      'scaling_factor': [1.0]})
    return df


def apply_regional_scalars(df, regional_scalar_path, region_locs, value_cols,
                           year_id):
    current_loc = df.location_id.unique().item()
    if current_loc in region_locs:

        merge_cols = ['location_id', 'year_id']
        path = 'FILEPATH'.format(p=regional_scalar_path)
        try:
            scalars = pd.read_hdf(
                path, 'scalars', where=(["'location_id'=={} & 'year_id'=={}"
                                        .format(current_loc, year_id)]))
        except FileNotFoundError:
            scalars = create_default_scalars(current_loc, year_id)
            logger.info(
                "Defaulting to 1.0 for pop scalars loc {loc}, year {yr}, at "
                "{path}".format(path=path, loc=current_loc, yr=year_id))

        scalars['year_id'] = year_id
        df = df.merge(scalars, on=merge_cols, how='left')
        df['scaling_factor'].fillna(1, inplace=True)
        newvals = df[value_cols].values * df[['scaling_factor']].values
        newvals = pd.DataFrame(newvals, index=df.index, columns=value_cols)
        df.drop(value_cols, axis=1, inplace=True)
        df = df.join(newvals)
        df[merge_cols] = df[merge_cols].astype(int)
        df.drop('scaling_factor', axis=1, inplace=True)
    return df


class LocationAggregator(ComputationElement):

    def __init__(self, location_set_id, year_id, rei_id, sex_id, measure_id,
                 gbd_round_id, decomp_step, n_draws, data_root, region_locs,
                 write_out_star_ids):
        self.location_set_id = location_set_id
        self.year_id = year_id
        self.rei_id = rei_id
        self.sex_id = sex_id
        self.measure_id = measure_id
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.n_draws = n_draws
        self.data_root = data_root
        self.region_locs = region_locs
        self.data_container = DataContainer(
            {'location_set_id': self.location_set_id,
             'year_id': self.year_id,
             'sex_id': self.sex_id},
            n_draws=self.n_draws, gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            cache_dir=os.path.join(self.data_root, 'cache'))
        self.loctree_list = self.data_container[
            'location_hierarchy_{}'.format(self.location_set_id)]
        logger.debug(f"Location Set ID: {self.location_set_id} and LocTree: {self.loctree_list}")

        self.in_dir = os.path.join(self.data_root, 'draws')
        self.out_dir = os.path.join(self.data_root, 'loc_agg_draws/burden')
        mkds.makedirs_safely(self.out_dir)
        self.write_out_star_ids = write_out_star_ids

        # Remove old aggregates in case jobs failed in the middle
        for loctree in self.loctree_list:
            aggregates = [n.id for n in loctree.nodes
                          if n not in loctree.leaves()]
            for loc in aggregates:
                filename = ('FILEPATH'
                            .format(o=self.out_dir, lo=loc, me=self.measure_id,
                                    m=self.measure_id, y=self.year_id,
                                    loc=loc, r=self.rei_id, s=self.sex_id))
                logger.debug("Deleting potentially pre-existing loc-agg file"
                             "{e}: '{f}'".format(e=os.path.exists(filename),
                                             f=filename))
                with contextlib.suppress(FileNotFoundError):
                    os.remove(filename)

        self.index_cols = ['measure_id', 'metric_id', 'sex_id', 'cause_id',
                           'rei_id', 'year_id', 'age_group_id']
        self.value_cols = ['draw_{}'.format(i) for i in range(self.n_draws)]
        self.draw_filters = {'metric_id': gbd.metrics.NUMBER,
                             'rei_id': self.rei_id,
                             'sex_id': self.sex_id,
                             'measure_id': self.measure_id,
                             'year_id': self.year_id}

        self.operator = self.get_operator()
        self.draw_source, self.draw_sink = self.get_draw_source_sink()

    def get_operator(self):
        return Sum(
            index_cols=self.index_cols,
            value_cols=self.value_cols)

    def get_draw_source_sink(self):
        ss = SourceSinkPair()
        in_pattern = ('FILEPATH'
                      .format(measure_id=self.measure_id,
                              year_id=self.year_id))
        out_pattern = ('FILEPATH')

        draw_cols = ["draw_{}".format(i) for i in range(self.n_draws)]
        draw_source = ss.draw_source(
            params={'draw_dir': self.in_dir,
                    'file_pattern': in_pattern,
                    'h5_tablename': '{n}_draws'.format(n=self.n_draws),
                    'data_cols': draw_cols,
                    'index_cols': self.index_cols})
        draw_sink = ss.draw_sink(
            params={'draw_dir': self.out_dir,
                    'file_pattern': out_pattern,
                    'h5_tablename': '{n}_draws'.format(n=self.n_draws)})
        draw_sink.add_transform(
            apply_regional_scalars,
            regional_scalar_path=os.path.join(self.data_root, 'cache'),
            region_locs=self.region_locs, value_cols=self.value_cols,
            year_id=self.year_id)
        draw_sink.add_transform(
            remove_unwanted_stars,
            write_out_star_ids=self.write_out_star_ids)
        return draw_source, draw_sink

    def get_dataframe(self):
        start_time = time.time()
        logger.info("START aggregate locations, time = {}".format(start_time))

        for loctree in self.loctree_list:
            logger.info(f"Starting aggregation on tree with root location_id "
                        f"{loctree.root.id}")
            AggMemEff(self.draw_source, self.draw_sink, self.index_cols,
                      'location_id', self.operator, chunksize=2
                      ).run(loctree, include_leaves=False, n_processes=8,
                            draw_filters=self.draw_filters)

        end_time = time.time()
        logger.info("location aggregation complete, time = {}"
                    .format(end_time))
        elapsed = end_time - start_time
        logger.info("DONE location agg pipeline at {}, "
                    "elapsed seconds= {}".format(end_time, elapsed))
        logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_burdenator_loc_agg()
    args = get_input_args.get_args_burdenator_loc_agg(parser)

    LocationAggregator(args.location_set_id, args.year_id, args.rei_id,
                       args.sex_id, args.measure_id, args.gbd_round_id,
                       args.decomp_step, args.n_draws, args.data_root,
                       args.region_locs, args.write_out_star_ids).get_dataframe()


if __name__ == "__main__":
    main()
