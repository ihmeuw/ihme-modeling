import pandas as pd
import logging
import argparse

from imported_cases.core import get_restricted_locations_for_cause
from imported_cases.core import get_cod_data_for_cause_location
from imported_cases.core import generate_distribution, make_square_data
import imported_cases.log_utilities as l
from save_results.models import CoDInput


"""
    This script does the following:
      -Reads in age/sex restricted data from CoD database
      -Generates distributions for data-rich countries
      -Saves file for upload

"""


def parse_args():
    '''
        Parse command line arguments

        Arguments are cause_id
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("cause_id", type=int)

    args = parser.parse_args()
    cause_id = args.cause_id

    return cause_id


if __name__ == '__main__':

    # Get command line arguments
    cause_id = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = r'FILEPATH'

    # Start logging
    l.setup_logging(log_dir, 'generate_imported_cases', str(cause_id))

    try:
        # Get restricted locations
        logging.info("Get restricted locations")
        restricted_locations = get_restricted_locations_for_cause(cause_id)

        # Get CoD data for cause
        logging.info("Get CoD data for cause")
        if cause_id == 361:
            fetch_cause_id = 360
        elif cause_id == 348:
            fetch_cause_id = 347
        else:
            fetch_cause_id = cause_id
        data = get_cod_data_for_cause_location(fetch_cause_id,
                                               restricted_locations)
        data['cause_id'] = cause_id

        if len(data) > 0:
            # Generate beta distribution for data
            logging.info("Get beta distribution for data")
            data = generate_distribution(data)

            # Generate square data
            logging.info("Generate square data")
            square_data = make_square_data(cause_id)

            # Combine imported cases data and square data
            logging.info("Combine imported cases data and square data")
            data = pd.concat([data, square_data]).reset_index(drop=True)
            data = data.groupby(['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id']
                                ).sum().reset_index()

            # Saving data
            logging.info("Saving data")
            data.to_csv(parent_dir + '/FILEPATH.csv', index=False)

            CoDInput(cause_id, [1, 2], "New imported cases", parent_dir,
                     model_version_type_id=7, mark_best=True)

        logging.info('All done!')
    except:
        logging.exception('Uncaught exception in generate_imported_cases.py')
