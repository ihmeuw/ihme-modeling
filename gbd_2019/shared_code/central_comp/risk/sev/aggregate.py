import argparse
import logging
import os
import pandas as pd
from functools import partial

from aggregator.aggregators import AggMemEff
from aggregator.operators import WtdSum
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import standard_write_func
from hierarchies.dbtrees import loctree


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sev_version_id', type=int)
    parser.add_argument('--rei_id', type=int)
    parser.add_argument('--location_set_id', type=int, nargs='+')
    parser.add_argument('--n_draws', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)

    args = parser.parse_args()
    sev_version_id = args.sev_version_id
    rei_id = args.rei_id
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    n_draws = args.n_draws
    location_set_id = args.location_set_id

    return (sev_version_id, rei_id, gbd_round_id, decomp_step, n_draws, location_set_id)


if __name__ == '__main__':
    (sev_version_id, rei_id, gbd_round_id, decomp_step, n_draws,
     location_set_id) = parse_arguments()

    drawdir = f'FILEPATH/sev/{sev_version_id}/draws'

    # set up source and sink
    source_params = {'draw_dir': drawdir,
                     'file_pattern': '{rei_id}/{location_id}.csv'}
    source = DrawSource(source_params)
    sink_params = {'draw_dir': drawdir,
                   'file_pattern': '{rei_id}/{location_id}.csv'}
    sink = DrawSink(sink_params, write_func = partial(standard_write_func, index=False))

    index_cols = ['rei_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id',
                  'metric_id']
    draw_cols = ['draw_{}'.format(i) for i in range(n_draws)]

    for lsid in location_set_id:

        population = pd.read_hdf("FILEPATH/population.h5")

        # aggregation operator
        operator = WtdSum(index_cols=index_cols,
                          value_cols=draw_cols,
                          weight_df=population,
                          weight_name='population',
                          merge_cols=['location_id', 'year_id',
                                      'age_group_id', 'sex_id'])
        # run aggregation
        aggregator = AggMemEff(
            draw_source=source,
            draw_sink=sink,
            index_cols=index_cols,
            aggregate_col='location_id',
            operator=operator)

        if lsid == 40:
            loc_trees = loctree(location_set_id=lsid,
                                gbd_round_id=gbd_round_id,
                                return_many=True)
            for tree in loc_trees:
                aggregator.run(tree, draw_filters={'rei_id': rei_id})
        else:
            loc_tree = loctree(location_set_id=lsid,
                               gbd_round_id=gbd_round_id)
            aggregator.run(loc_tree,
                           draw_filters={'rei_id': rei_id})
