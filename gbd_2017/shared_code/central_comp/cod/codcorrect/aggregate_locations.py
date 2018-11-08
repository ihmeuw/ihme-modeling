import argparse
import logging
import numpy as np
import os
import pandas as pd
import sys

from aggregator.aggregators import AggMP, AggMemEff
from aggregator.operators import Sum
from db_queries import get_location_metadata
from draw_sources.draw_sources import DrawSource, DrawSink
import gbd.constants as GBD
from hierarchies.dbtrees import loctree

from codcorrect.core import read_json
import codcorrect.log_utilities as cc_log_utils


def parse_args():
    """
    Parse command line arguments.

    Arguments:
        output_version_id (int)
        df_type (string)
        measure_id (int)
        location_set_id (int or int[])
        year_id (int[])

    Returns:
        Tuple of all 5 input arguments in order.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=int)
    parser.add_argument("--df_type", type=str)
    parser.add_argument("--measure_id", type=int)
    parser.add_argument("--location_set_id", type=int)
    parser.add_argument("--year_id", type=int)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    df_type = args.df_type
    measure_id = args.measure_id
    location_set_id = args.location_set_id
    year_id = args.year_id

    return output_version_id, df_type, measure_id, location_set_id, year_id


def read_helper_files(parent_dir):
    """
    Return the config dictionary from json.

    Arguments:
        parent_dir (str):

    Returns:
        A dictionary containing the configuration specifications for this run.
    """
    return read_json(os.path.join(parent_dir, 'FILEPATH'))


def create_scalars_template():
    df_sex = pd.DataFrame({'sex_id': [1, 2, 3]})
    df_age = pd.DataFrame({
        'age_group_id': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                          12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                          30, 31, 32, 235]})
    df_sex['scaling_factor'] = 1.0
    df_age['scaling_factor'] = 1.0
    df_scalars_template = df_sex.merge(df_age, how='outer')
    return df_scalars_template


def create_default_scalars(location_id, year_id):
    """
    """
    df = create_scalars_template().copy(deep=True)
    df['location_id'] = location_id
    df['year_id'] = year_id
    return df


def apply_regional_scalars(df, region_locs, parent_dir):
    """
    Arguments:
        df (pd.DataFrame): data to transform.
        region_locs (int[]): list of region location ids to apply scalars on.
    """
    value_cols = ['draw_{}'.format(i) for i in range(1000)]
    merge_cols = ['location_id', 'year_id']
    for loc in df.location_id.unique().tolist():
        if loc in region_locs:
            scalars = []
            for year_id in df.year_id.unique().tolist():
                year_loc_pair = pd.read_hdf(
                    os.path.join(parent_dir, '_temp/region_scalars.h5'),
                    key='draws',
                    where=['location_id=={}'.format(loc),
                           'year_id=={}'.format(year_id)])
                scalars.append(year_loc_pair)
            scalars = pd.concat(scalars)
            df = df.merge(scalars, on=merge_cols, how='left')
            df['mean'].fillna(1, inplace=True)
            newvals = df[value_cols].values * df[['mean']].values
            newvals = pd.DataFrame(newvals, index=df.index, columns=value_cols)
            df.drop(value_cols, axis=1, inplace=True)
            df = df.join(newvals)
            df[merge_cols] = df[merge_cols].astype(int)
            df.drop('mean', axis=1, inplace=True)
    return df


def transform_add_measure(df, measure_id):
    df['measure_id'] = measure_id
    return df


if __name__ == '__main__':
    # Get command line arguments
    version_id, df_type, measure_id, location_set_id, year_id = parse_args()
    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')
    # Start logging
    cc_log_utils.setup_logging(log_dir, 'agg_location', str(version_id),
                               df_type, str(measure_id), str(location_set_id),
                               str(year_id))

    measure_id = int(measure_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files.")
        config = read_helper_files(parent_dir)
        # Read in config variables
        index_cols = config['index_columns']
        draw_cols = config['data_columns']
        sex_id = config['eligible_sex_ids']

        # Create draw source/sink
        logging.info("Creating draw source and sink.")
        draw_dir = os.path.join(parent_dir, 'aggregated/{}'.format(df_type))
        input_pattern = '{measure_id}_{location_id}_{year_id}.h5'
        source_config = {'draw_dir': draw_dir, 'file_pattern': input_pattern}
        draw_source = DrawSource(source_config)

        output_pattern = '{measure_id}_{location_id}_{year_id}.h5'
        sink_config = {'draw_dir': draw_dir, 'file_pattern': output_pattern,
                       'h5_tablename': 'draws'}
        draw_sink = DrawSink(sink_config)

        # Apply regional scalar transform
        region_locs = get_location_metadata(gbd_round_id=GBD.GBD_ROUND_ID,
                                            location_set_id=35)
        region_locs = region_locs[region_locs.level == 2].location_id.tolist()
        draw_sink.add_transform(
            apply_regional_scalars, region_locs=region_locs,
            parent_dir=parent_dir
        )
        draw_sink.add_transform(
            transform_add_measure, measure_id=measure_id
        )

        # create operator
        logging.info("Reading regional scalars from flatfiles.")
        index_cols = [col for col in index_cols if col != 'location_id']
        operator = Sum(index_cols, draw_cols)

        # Aggregate
        logging.info("Instantiate aggregator.aggregators.AggMemEff.")
        aggregator = AggMemEff(
            draw_source=draw_source,
            draw_sink=draw_sink,
            index_cols=index_cols,
            aggregate_col='location_id',
            operator=operator,
            chunksize=2)

        logging.info("Create location tree(s).")
        is_sdi_set = False
        if location_set_id == 40:
            is_sdi_set = True
        tree = loctree(location_set_id=location_set_id,
                       gbd_round_id=GBD.GBD_ROUND_ID,
                       return_many=is_sdi_set)
        logging.info("Run aggregator.")
        for t in np.atleast_1d(tree):
            aggregator.run(t,
                           draw_filters={'measure_id': measure_id,
                                         'year_id': year_id},
                           n_processes=8)

        logging.info("All done!")
    except Exception:
        logging.exception("Uncaught exception in aggregate_locations.py: {}"
                          .format(sys.exc_info()[0]))
        sys.exit(1)
