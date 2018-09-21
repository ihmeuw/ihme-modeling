import sys

import pandas as pd
import logging
import argparse
import glob
import functools
from multiprocessing import Pool

from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws, save_hdf
import codcorrect.log_utilities as l


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
        DataFrame config
    """

    # Config file
    config = read_json(parent_dir + r'FILEPATH.json')

    location_hierarchy = pd.read_csv(parent_dir +
                                     r'FILEPATH.csv')
    estimate_locations = location_hierarchy.ix[
        location_hierarchy['is_estimate'] == 1, 'location_id'].tolist()

    if int(location) in estimate_locations:
        most_detailed_location = True
    else:
        most_detailed_location = False

    return config, most_detailed_location


def append_shocks(rescaled_data, shock_data, index_cols, data_cols):
    df = pd.concat([rescaled_data, shock_data]).reset_index(drop=True)
    df = df[index_cols + data_cols].groupby(index_cols).sum().reset_index()
    return df


def read_most_detailed(shock_dir, rescaled_dir, location):
    logging.info("Reading in shock and rescaled deaths for {}"
                 .format(location))
    shocks = read_hdf_draws(r'FILEPATH.h5')
    rescaled = read_hdf_draws(r'FILEPATH.h5')
    logging.info("Reading in shock and rescaled ylls for {}"
                 .format(location))
    shocks_yll = read_hdf_draws(r'FILEPATH.h5')
    rescaled_yll = read_hdf_draws(r'FILEPATH.h5')
    return rescaled, shocks, rescaled_yll, shocks_yll


def read_aggregated(directory, measure, location):
    pool = Pool(6)
    files = glob.glob(directory + 'FILEPATH')
    read_hdf = functools.partial(read_hdf_draws, location_id=location)
    df_list = pool.map(read_hdf, files)
    pool.close()
    pool.join()
    return pd.concat(df_list)


def read_all_inputs(shock_dir, rescaled_dir, location, most_detailed):
    logger = logging.getLogger('append_shocks.read_all_inputs')
    try:
        if most_detailed:
            rescaled, shocks, rescaled_yll, shocks_yll = read_most_detailed(
                shock_dir, rescaled_dir, location)
        else:
            rescaled = read_aggregated(rescaled_dir, 1, location)
            shocks = read_aggregated(shock_dir, 1, location)
            rescaled_yll = read_aggregated(rescaled_dir, 4, location)
            shocks_yll = read_aggregated(shock_dir, 4, location)
        return rescaled, shocks, rescaled_yll, shocks_yll
    except Exception as e:
        logger.exception('Failed to read in all inputs: {}'.format(e))


def save_all_outputs(parent_dir, new_rescaled, new_rescaled_yll):
    logger = logging.getLogger('append_shocks.save_all_outputs')
    try:
        logging.info("Saving new rescaled+shocks deaths for loc {}"
                     .format(location))
        filepath = parent_dir + r'FILEPATH.h5'.format(l_id=location)
        save_hdf(new_rescaled, filepath, data_columns=index_cols)

        logging.info("Saving new rescaled+shocks ylls for loc {}"
                     .format(location))
        filepath = parent_dir + r'FILEPATH.h5'.format(l_id=location)
        save_hdf(new_rescaled_yll, filepath, data_columns=index_cols)
    except Exception as e:
        logger.exception('Failed to save all outputs: {}'.format(e))


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location = parse_args()

    # Set paths
    parent_dir = r'FILEPATH'
    log_dir = parent_dir + r'FILEPATH'
    shock_dir = r'FILEPATH'
    rescaled_dir = r'FILEPATH'

    # Start logging
    l.setup_logging(log_dir, 'append_shocks', output_version_id,
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
        rescaled, shocks, rescaled_yll, shocks_yll = read_all_inputs(
            shock_dir, rescaled_dir, location, most_detailed)

        # Append all rescaled with shocks
        logging.info("Appending shocks to rescaled deaths for {}"
                     .format(location))
        new_rescaled = append_shocks(rescaled, shocks, index_cols, data_cols)

        logging.info("Appending shocks to rescaled ylls for {}"
                     .format(location))
        new_rescaled_yll = append_shocks(rescaled_yll, shocks_yll, index_cols,
                                         data_cols)

        # Save
        logging.info("Saving all outputs of append_shocks for loc {}"
                     .format(location))
        save_all_outputs(parent_dir, new_rescaled, new_rescaled_yll)

        logging.info('All done!')
    except:
        logging.exception('uncaught exception in append_shocks.py: {}'
                          .format(sys.exc_info()[0]))
