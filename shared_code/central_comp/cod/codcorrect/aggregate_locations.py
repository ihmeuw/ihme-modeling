import sys

import argparse
import logging

from codcorrect.core import read_json
import codcorrect.log_utilities as l
from adding_machine import agg_locations as al


"""
    This script aggregates up the location hierarchy
"""


def parse_args():
    '''
        Parse command line arguments

        Arguments are output_version_id, df_type, measure_id

        Returns all 3 arguments as a tuple, in that order
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--df_type", type=str)
    parser.add_argument("--measure_id", type=str)
    parser.add_argument("--location_set_id", type=str)
    parser.add_argument("--year_ids", nargs='+')

    args = parser.parse_args()
    output_version_id = args.output_version_id
    df_type = args.df_type
    measure = args.measure_id
    location_set_id = args.location_set_id
    years = [int(y) for y in args.year_ids[0].split(" ")]

    return output_version_id, df_type, measure, location_set_id, years


def read_helper_files(parent_dir):
    """ Read in and return helper DataFrames.

        Returns:
        DataFrame containing cause hierarchy used for aggregation
    """

    # Config file
    config = read_json(parent_dir + r'/FILEPATH.json')
    return config


def aggregate_locations():
    al.agg_all_locs_mem_eff(
        drawdir=in_dir, stagedir=out_dir, location_set_id=location_set_id,
        index_cols=index_columns, year_ids=years, sex_ids=sexes,
        measure_id=int(measure), draw_filters={}, include_leaves=False,
        operator='wtd_sum',
        weight_col='scaling_factor',
        normalize=False,
        custom_file_pattern=input_file_pattern,
        output_file_pattern=output_file_pattern,
        poolsize=8)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, df_type, measure, location_set_id, years = parse_args()

    # Set paths
    parent_dir = r'FILEPATH'
    log_dir = parent_dir + r'/logs'
    in_dir = r'FILEPATH'
    out_dir = in_dir

    # Start logging
    l.setup_logging(log_dir, 'agg_location', output_version_id, df_type,
                    measure, location_set_id, "".join(str(yr) for yr in years))

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config = read_helper_files(parent_dir)

        # Read in config variables
        index_columns = config['index_columns']
        sexes = config['eligible_sex_ids']

        if measure == "1":
            input_file_pattern = 'FILEPATH.h5'
            output_file_pattern = 'FILEPATH.h5'
        else:
            input_file_pattern = 'FILEPATH.h5'
            output_file_pattern = 'FILEPATH.h5'

        logging.info("Calling adding_machine's agg_locations")
        aggregate_locations()
        logging.info('All done!')
    except:
        logging.exception('uncaught exception in aggregate_locations.py: {}'
                          .format(sys.exc_info()[0]))
