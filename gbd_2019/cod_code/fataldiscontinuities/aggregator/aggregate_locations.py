import argparse
import logging
import os
import pandas as pd
import sys
 
from shock_aggregator.core import read_json
from shock_aggregator.io import read_hdf_draws, save_hdf
from shock_aggregator.error_check import save_diagnostics
import shock_aggregator.log_utilities as sa_log


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_version_id', type=str)
    parser.add_argument('--location_id', type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id

    return output_version_id, location_id


def read_helper_files(parent_dir, location_id):
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))

    location_hierarchy = pd.read_csv(
        os.path.join(parent_dir, '_temp/location_hierarchy.csv'))
    child_locations = location_hierarchy.loc[
        ((location_hierarchy['parent_id'] == int(location_id)) &
         (location_hierarchy['location_id'] != int(location_id))),
        'location_id'].drop_duplicates().tolist()

    return config, child_locations


def aggregate_location(data, location_id, index_columns):
    logger = logging.getLogger('aggregate_locations.aggregate_locations')
    try:
        data['location_id'] = location_id

        data = data.groupby(index_columns).sum().reset_index()
    except Exception as e:
        logger.exception('Failed to aggregate location: {}'.format(e))
        sys.exit()

    return data


def read_child_location_draw_files(parent_dir, location_id, child_locations,
                                   index_columns):
    logger = logging.getLogger(
        'aggregate_locations.read_child_location_draw_files')
    try:
        c = 0
        data = []
        for child_id in child_locations:
            data_filepath = os.path.join(
                parent_dir, 'draws/shocks_{location_id}.h5'.format(
                    location_id=str(child_id)))

            logger.info('Appending in {}'.format(data_filepath))
            print('Appending in {}'.format(data_filepath))
            data.append(read_hdf_draws(
                data_filepath, child_id).reset_index(drop=True))

            c += 1
            if c % 5 == 0:
                logger.info('Intermediate collapsing location')
                data = [aggregate_location(
                    pd.concat(data), location_id, index_columns)]
        logger.info('Intermediate collapsing location')
        data = aggregate_location(pd.concat(data), location_id, index_columns)

    except Exception as e:
        logger.exception('Failed to aggregate location: {}'.format(e))
    return data


def save_draws(data, index_columns):

    logger = logging.getLogger('shocks.save_draws')

    for c in index_columns:
        data[c] = data[c].astype('int64')

    data = data.sort_values(index_columns).reset_index(drop=True)
    data.to_hdf(
        os.path.join(parent_dir, 'draws/shocks_{location_id}.h5'.format(
            location_id=location_id)),
        'draws', mode='w', format='table', data_columns=index_columns)

    draw_filepath = os.path.join(
        parent_dir, 'draws/shocks_{location_id}.csv'.format(
            location_id=location_id))
    data.to_csv(draw_filepath, index=False)

    year_ids = data['year_id'].drop_duplicates().tolist()
    for year_id in year_ids:
        draw_filepath = os.path.join(
            parent_dir, 'draws/shocks_{lid}_{yid}.csv'.format(
                lid=location_id, yid=year_id))
        temp = data.loc[data['year_id'] == year_id].copy(deep=True)
        temp.to_csv(draw_filepath, index=False)


if __name__ == '__main__':

    output_version_id, location_id = parse_args()

    parent_dir = 'FILEPATH'.format(
        v=output_version_id)
    log_dir = os.path.join(parent_dir, 'logs')

    sa_log.setup_logging(log_dir, 'agg_location', output_version_id,
                         location_id)

    try:
        print("Reading in helper files")
        logging.info("Reading in helper files")
        config, child_locations = read_helper_files(parent_dir, location_id)

        index_columns = config['index_columns']
        data_columns = config['data_columns']

        print("Reading in child location draw files")
        logging.info("Reading in child location draw files")
        logging.info("{}".format(', '.join([str(x) for x in child_locations])))
        aggregated_data = read_child_location_draw_files(parent_dir,
                                                         location_id,
                                                         child_locations,
                                                         index_columns)

        logging.info("Save draws")
        save_draws(aggregated_data, index_columns)

        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in aggregate_locations.py')
