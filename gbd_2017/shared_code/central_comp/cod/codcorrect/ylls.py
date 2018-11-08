import argparse
import logging
import os
import pandas as pd
import sys

from draw_sources.draw_sources import DrawSink, DrawSource

from codcorrect.core import read_json
import codcorrect.log_utilities as cc_log_utils


"""
    This script calculates YLL's from deaths and pred_ex
"""


def parse_args():
    """Parse command line arguments

    Arguments are output_version_id and location_id

    Returns both arguments, in that order
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id

    return output_version_id, location_id


def read_helper_files(parent_dir):
    """Read in and return helper DataFrames.

        Returns: DataFrame containing config file.
    """
    # Config file
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))
    return config


def read_rescaled_draw_files(parent_dir, location_id):
    """Reads in rescaled draw files."""
    params = {'draw_dir': os.path.join(parent_dir, 'aggregated/rescaled'),
              'file_pattern': '1_{location_id}_{year_id}.h5',}
    source = DrawSource(params)
    return source.content(filters={'location_id': location_id})


def read_shocks_draw_files(parent_dir, location_id):
    """Reads in rescaled draw files."""
    params = {'draw_dir': os.path.join(parent_dir, 'aggregated/shocks'),
              'file_pattern': '1_{location_id}_{year_id}.h5'}
    source = DrawSource(params)
    return source.content(filters={'location_id': location_id})


def read_pred_ex(parent_dir, location_id, index_columns):
    """Reads in pred_ex from the cached."""
    loc_id = int(location_id)
    pred_ex = pd.read_csv(os.path.join(parent_dir, '_temp/pred_ex.csv'))
    pred_ex = pred_ex[pred_ex.location_id.isin([loc_id])]
    pred_ex['pred_ex'] = pred_ex['pred_ex'].astype(float)
    pred_ex = pred_ex[index_columns + ['pred_ex']]
    return pred_ex


def calc_ylls(pred_ex_df, death_df, data_columns, index_columns):
    """Multiplies pred_ex by every death draw, indexed by loc/year/age/sex."""
    ylls = pd.merge(death_df, pred_ex_df, on=index_columns, how='left')
    for data_column in data_columns:
        ylls[data_column] = ylls[data_column] * ylls['pred_ex']
    ylls = ylls.drop('pred_ex', axis=1)
    return ylls


def add_measure_id_to_sink(df, measure_id=4):
    df['measure_id'] = measure_id
    return df


def save_all_draws(parent_dir, ylls, yll_shocks, location_id, index_columns,
                   measure_id=4):
    # Save yll data
    agg_rescaled_params = {
        'draw_dir': os.path.join(parent_dir, 'aggregated/rescaled'),
        'file_pattern': '{measure_id}_{location_id}_{year_id}.h5',
        'h5_tablename': 'draws'
    }
    rescaled_sink = DrawSink(agg_rescaled_params)
    rescaled_sink.add_transform(add_measure_id_to_sink,
                                measure_id=measure_id)
    rescaled_sink.push(ylls, append=False)

    agg_shocks_params = {
        'draw_dir': os.path.join(parent_dir, 'aggregated/shocks'),
        'file_pattern': '{measure_id}_{location_id}_{year_id}.h5',
        'h5_tablename': 'draws'
    }
    shocks_sink = DrawSink(agg_shocks_params)
    shocks_sink.add_transform(add_measure_id_to_sink,
                              measure_id=measure_id)
    shocks_sink.push(yll_shocks, append=False)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location_id = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'ylls', output_version_id, location_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config = read_helper_files(parent_dir)
        envelope_version_id = config['envelope_version_id']

        # Read in config variables
        index_columns = config['index_columns']
        index_columns.remove('measure_id')
        yll_index_columns = list(set(index_columns) - set(['cause_id']))
        data_columns = config['data_columns']

        # Read in rescaled draw files
        logging.info("Reading in cause/loc aggregated with shock+hiv draws")
        deaths = read_rescaled_draw_files(parent_dir, location_id)

        # Read in shock draw files
        logging.info("Reading in cause/loc aggregated shock draws")
        shocks = read_shocks_draw_files(parent_dir, location_id)

        # Read in pred ex file
        logging.info('Reading in pred_ex values from Mortality team '
                     'with envelope {}'.format(envelope_version_id))
        pred_ex = read_pred_ex(parent_dir, location_id, yll_index_columns)

        # Calculate YLLs
        logging.info("Calculating YLLs for non-shocks")
        ylls = calc_ylls(pred_ex, deaths, data_columns, yll_index_columns)
        logging.info("Calculating YLLs for shocks")
        yll_shocks = calc_ylls(pred_ex, shocks, data_columns,
                               yll_index_columns)

        # Saving data
        logging.info("Saving data")
        save_all_draws(parent_dir, ylls, yll_shocks, location_id,
                       index_columns)

        print('All done!')
        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in ylls.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
