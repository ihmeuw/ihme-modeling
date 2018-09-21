import sys

import pandas as pd
import logging
import argparse

from db_queries import get_life_table
from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws
from codcorrect.io import save_hdf
import codcorrect.log_utilities as l


"""
    This script calculates YLL's from deaths and pred_ex
"""


def parse_args():
    '''
        Parse command line arguments

        Arguments are output_version_id and location_id

        Returns both arguments, in that order
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id

    return output_version_id, location_id


def read_helper_files(parent_dir):
    """ Read in and return helper DataFrames.

        Returns:
        DataFrame containing config file
    """

    # Config file
    config = read_json(parent_dir + r'FILEPATH.json')

    return config


def read_rescaled_draw_files(parent_dir, location_id):
    """ Reads in rescaled draw files """
    draw_filepath = parent_dir + r'FILEPATH.h5'
    data = read_hdf_draws(draw_filepath, location_id).reset_index(drop=True)
    return data


def read_shocks_draw_files(parent_dir, location_id):
    """ Reads in rescaled draw files """
    draw_filepath = parent_dir + r'FILEPATH.h5'
    data = read_hdf_draws(draw_filepath, location_id).reset_index(drop=True)
    return data


def read_pred_ex(location_id, index_columns, lifetable_version_id):
    """ Reads in pred_ex from the Mortality team """
    pred_ex = get_life_table(location_id=location_id,
                             life_table_parameter_id=6,
                             process_version_map_id=lifetable_version_id)
    pred_ex.rename(columns={'mean': 'pred_ex'}, inplace=True)
    pred_ex['pred_ex'] = pred_ex['pred_ex'].astype(float)
    pred_ex = pred_ex[index_columns + ['pred_ex']]
    return pred_ex


def calc_ylls(pred_ex_df, death_df, data_columns, index_columns):
    """ Multiplies pred_ex by every death draw, indexed by loc/year/age/sex """
    ylls = pd.merge(death_df, pred_ex_df, on=index_columns, how='left')
    for data_column in data_columns:
        ylls.eval('{d} = {d} * pred_ex'.format(d=data_column), inplace=True)
    ylls = ylls.drop('pred_ex', axis=1)
    return ylls


def save_all_draws(parent_dir, index_columns, ylls, yll_shocks):
    # Save yll data
    outdir = parent_dir + r'FILEPATH.h5'
    save_hdf(ylls, outdir, key='draws', mode='w',
             format='table', data_columns=index_columns)
    outdir = parent_dir + r'FILEPATH.h5'
    save_hdf(yll_shocks, outdir, key='draws', mode='w',
             format='table', data_columns=index_columns)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location_id = parse_args()

    # Set paths
    pred_ex_dir = 'FILEPATH'
    parent_dir = 'FILEPATH'
    log_dir = parent_dir + r'/logs'

    # Start logging
    l.setup_logging(log_dir, 'ylls', output_version_id, location_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config = read_helper_files(parent_dir)

        # Read in config variables
        index_columns = config['index_columns']
        yll_index_columns = list(set(index_columns) - set(['cause_id']))
        data_columns = config['data_columns']
        lifetable_version_id = config['lifetable_version_id']

        # Read in rescaled draw files
        logging.info("Reading in cause/loc aggregated with shock+hiv draws")
        deaths = read_rescaled_draw_files(parent_dir, location_id)

        # Read in shock draw files
        logging.info("Reading in cause/loc aggregated shock draws")
        shocks = read_shocks_draw_files(parent_dir, location_id)

        # Read in pred ex file
        logging.info('Reading in pred_ex values from Mortality team '
                     'from %s' % pred_ex_dir)
        pred_ex = read_pred_ex(location_id, yll_index_columns,
                               lifetable_version_id)

        # Calculate YLLs
        logging.info("Calculating YLLs for non-shocks")
        ylls = calc_ylls(pred_ex, deaths, data_columns, yll_index_columns)
        logging.info("Calculating YLLs for shocks")
        yll_shocks = calc_ylls(pred_ex, shocks, data_columns,
                               yll_index_columns)

        # Saving data
        logging.info("Saving data")
        save_all_draws(parent_dir, index_columns, ylls, yll_shocks)

        print 'All done!'
        logging.info('All done!')
    except:
        logging.exception('uncaught exception in ylls.py: {}'
                          .format(sys.exc_info()[0]))
