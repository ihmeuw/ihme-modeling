import argparse
from datetime import datetime
import functools
import logging
from multiprocessing import Pool
import os
import pandas as pd
import sys

from db_queries import get_cod_data, get_demographics
from save_results import save_results_cod

from imported_cases.core import (generate_distribution,
                                 get_cause_specific_locations,
                                 get_data_rich_locations,
                                 make_square_data)
import imported_cases.log_utilities as log_utils


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('version_id', type=int)
    parser.add_argument('--cause_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--output_dir', type=str)

    args = parser.parse_args()
    return (
      args.version_id, args.cause_id, args.decomp_step,
      args.gbd_round_id, args.output_dir
    )


def pretty_now():
    return datetime.now().strftime("[%m/%d/%Y %H:%M:%S]")


if __name__ == '__main__':
    # Unpack arguments
    version_id, cause_id, decomp_step, gbd_round_id, output_dir = parse_args()

    # Set directories
    outdir = os.path.join(output_dir, '{}'.format(cause_id))
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    logdir = 'FILEPATH'

    log_utils.setup_logging(logdir,
                            'generate_imported_cases_{}'.format(cause_id))

    try:
        logging.info("{} Special cause considerations".format(pretty_now()))

        logging.info("{} Get restricted locations".format(pretty_now()))
        if cause_id == 562:
            locations = get_data_rich_locations(4)
            age_groups = list(range(2, 8))
        else:
            locations = get_cause_specific_locations(cause_id,
                                                     gbd_round_id)
            age_groups = list(range(2, 21)) + [30, 31, 32, 235]

        logging.info("{} Get CoD data for cause".format(pretty_now()))
        data = get_cod_data(cause_id,
                            age_group_id=age_groups,
                            location_id=locations,
                            gbd_round_id=gbd_round_id,
                            decomp_step=decomp_step)
        data = data[['location_id', 'year', 'sex', 'age_group_id', 'cf',
                     'deaths', 'sample_size']]
        data.rename(columns={'year': 'year_id', 'sex': 'sex_id'}, inplace=True)
        data['cause_id'] = cause_id

        if len(data) > 0:
            logging.info("{} Get beta distribution for data"
                         .format(pretty_now()))

            data_by_year = []
            for year in data.year_id.unique():
                data_by_year.append(data[data.year_id == year])

            beta_distribution_helper = functools.partial(generate_distribution)

            pool = Pool(38)
            distributions = pool.map(beta_distribution_helper, data_by_year)
            pool.close()
            pool.join()

            data = pd.concat(distributions)

            logging.info("{} Generate square data".format(pretty_now()))
            # Maybe not for opioids
            square_data = make_square_data(cause_id, age_groups, gbd_round_id)

            logging.info("{} Combine imported cases and square data"
                         .format(pretty_now()))
            data = pd.concat([data, square_data]).reset_index(drop=True)
            data = data.groupby(['location_id', 'year_id', 'sex_id',
                                 'age_group_id', 'cause_id']
                                ).sum().reset_index()

            logging.info("{} Saving Data".format(pretty_now()))
            data.to_csv(os.path.join(outdir, 'to_upload.csv'), index=False)
            save_results_cod(outdir,
                             'to_upload.csv',
                             cause_id,
                             "New imported cases",
                             decomp_step=decomp_step,
                             gbd_round_id=gbd_round_id,
                             model_version_type_id=7,
                             mark_best=True,
                             db_env='prod')

        logging.info("{} All Done".format(pretty_now()))
    except Exception:
        logging.exception("{} Uncaught exception in generate_imported_cases"
                          .format(pretty_now()))
        sys.exit(1)
