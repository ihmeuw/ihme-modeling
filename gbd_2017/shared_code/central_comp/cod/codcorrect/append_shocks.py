import argparse
import logging
import os
import pandas as pd
import sys

from draw_sources.draw_sources import DrawSource

from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws, save_hdf
import codcorrect.log_utilities as cc_log_utils


"""
This script reads in rescaled ylls and rescaled deaths, and appends yll and
death shocks to them, respectively"""


def parse_args():
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location = args.location_id

    return output_version_id, location


def read_helper_files(parent_dir, location):
    """ Read in and return helper DataFrames.

        Returns:
        DataFrame config, most_detailed_location bool
    """

    # Config file
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))

    location_hierarchy = pd.read_csv(
        os.path.join(parent_dir, '_temp/location_hierarchy.csv'))
    estimate_locations = location_hierarchy.loc[
        location_hierarchy['is_estimate'] == 1, 'location_id'].tolist()

    if int(location) in estimate_locations:
        most_detailed_location = True
    else:
        most_detailed_location = False

    return config, most_detailed_location


def append_shocks(rescaled_data, shock_data, index_cols, data_cols):
    """ Given a deaths/yll dataframe, append shocks to it """
    df = pd.concat([rescaled_data, shock_data]).reset_index(drop=True)
    df = df[index_cols + data_cols].groupby(index_cols).sum().reset_index()
    return df


def save_all_outputs(parent_dir, new_rescaled, new_rescaled_yll, location):
    """Saves deaths and ylls with appended shocks to the final output location
    for CoDCorrect."""
    logger = logging.getLogger('append_shocks.save_all_outputs')

    try:
        logging.info("Saving new rescaled+shocks deaths for loc {}"
                     .format(location))
        filepath = parent_dir + r'/draws/1_{l_id}.h5'.format(l_id=location)
        save_hdf(new_rescaled, filepath, data_columns=index_cols)

        logging.info("Saving new rescaled+shocks ylls for loc {}"
                     .format(location))
        filepath = parent_dir + r'/draws/4_{l_id}.h5'.format(l_id=location)
        save_hdf(new_rescaled_yll, filepath, data_columns=index_cols)
    except Exception as e:
        logger.exception('Failed to save all outputs: {}'.format(e))


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'append_shocks', output_version_id,
                               location)

    try:
        # Read in helper files\
        logging.info("Reading in helper files")
        config, most_detailed = read_helper_files(parent_dir, location)

        # Read in config variables
        index_cols = config['index_columns']
        data_cols = config['data_columns']
        years = config['eligible_year_ids']
        sexes = config['eligible_sex_ids']

        # Read in all inputs
        logging.info("Reading in all inputs for {}".format(location))
        rescaled_dir = os.path.join(parent_dir, 'aggregated/rescaled')

        shock_dir = os.path.join(parent_dir, 'aggregated/shocks')
        input_file_pattern = '{measure_id}_{location_id}_{year_id}.h5'

        logging.info("Rescaled draws...")
        rescaled_params = {
            'draw_dir': rescaled_dir,
            'file_pattern': input_file_pattern
        }
        rescaled_ds = DrawSource(rescaled_params)
        rescaled = rescaled_ds.content(filters={'location_id': location,
                                                'measure_id': 1})

        logging.info("Shock draws...")
        shock_params = {
            'draw_dir': shock_dir,
            'file_pattern': input_file_pattern
        }
        shock_ds = DrawSource(shock_params)
        shocks = shock_ds.content(filters={'location_id': location,
                                           'measure_id': 1})

        logging.info("Rescaled YLL draws...")
        rescaled_yll_params = {
            'draw_dir': rescaled_dir,
            'file_pattern': input_file_pattern
        }
        rescaled_yll_ds = DrawSource(rescaled_yll_params)
        rescaled_yll = rescaled_yll_ds.content(
            filters={'location_id': location, 'measure_id': 4})

        logging.info("Rescaled YLL shock draws...")
        shock_yll_params = {
            'draw_dir': shock_dir,
            'file_pattern': input_file_pattern
        }
        shock_yll_ds = DrawSource(shock_yll_params)
        shock_yll = shock_yll_ds.content(
            filters={'location_id': location, 'measure_id': 4})

        # Append all rescaled with shocks
        logging.info("Appending shocks to rescaled deaths for {}"
                     .format(location))
        new_rescaled = append_shocks(rescaled, shocks, index_cols, data_cols)

        logging.info("Appending shocks to rescaled ylls for {}"
                     .format(location))
        new_rescaled_yll = append_shocks(rescaled_yll, shock_yll, index_cols,
                                         data_cols)

        # Save
        logging.info("Saving all outputs of append_shocks for loc {}"
                     .format(location))
        save_all_outputs(parent_dir, new_rescaled, new_rescaled_yll, location)

        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in append_shocks.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
