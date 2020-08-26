import argparse
import logging
import os
import pandas as pd
import sys
from os.path import join
 
from db_queries import (get_location_metadata as lm,
                        get_population as get_pops)

from shock_aggregator.core import Envelope, read_json
from shock_aggregator.io import import_cod_model_draws, read_envelope_draws
from shock_aggregator.error_check import (tag_zeros,
                                          check_data_format,
                                          missing_check,
                                          exclusivity_check)
from shock_aggregator.restrictions import expand_id_set
import shock_aggregator.log_utilities as sh_agg_log

SHOCKS_ALL_YEAR_OUTPUTS = ('FILEPATH')


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--output_version_id', type=str)
    parser.add_argument('--location_id', type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id

    return output_version_id, location_id


def read_helper_files(parent_dir, location_id):
    logger = logging.getLogger('shocks.read_helper_files')

    logger.info('Reading config file')
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))
    sex_dict = {1: 'male', 2: 'female'}

    cause_hierarchy = pd.read_csv(
        os.path.join(parent_dir, 'FILEPATH'))

    return config, cause_hierarchy


def read_all_model_draws(required_columns, 
                         location_id, filter_years=None):

    shocks_causes_count = [387]
    shocks_causes_rate = [i for i in os.listdir(SHOCKS_ALL_YEAR_OUTPUTS)
                            if not i.endswith('.sh')]
    shocks_causes_rate += [843]
    shocks_causes_rate = [int(i) for i in shocks_causes_rate]

    valid_causes = [729,945,387,699,707,693,695,727,854,842,711,703,
                    302,335,341,408,345,843,357,725,726]
    data = []
    def pull_draws(cause_id, location_id, required_columns):

        if cause_id == 843:
            SHOCKS_ALL_YEAR_OUTPUTS = "FILEPATH"
            read_file = join(SHOCKS_ALL_YEAR_OUTPUTS,'{}.csv'.format(location_id))
        else:
            SHOCKS_ALL_YEAR_OUTPUTS = ('FILEPATH')
            read_file = join(SHOCKS_ALL_YEAR_OUTPUTS,'{}/{}.csv'.format(cause_id,location_id))

        data_sub = pd.read_csv(read_file,encoding='utf8')

        assert max(data_sub.year_id.unique()) == 2019

        data_sub['cause_id'] = cause_id
        data_sub = data_sub.loc[:,required_columns]        
        return data_sub

   
    for cause in shocks_causes_rate:
        if int(location_id) in [44533] + list(range(44793,44801)):
            meta = lm(gbd_round_id=6, location_set_id=21)
            child_ids = meta.loc[meta['parent_id']==int(location_id),
                                 'location_id'].unique()
            child_draws = []
            for child_id in child_ids:
                data_sub = pull_draws(cause, child_id, required_columns)
                child_draws.append(data_sub)
            temp_data = pd.concat(child_draws)

            for i in range(0,1000):
                draw_col = 'draw_{}'.format(i)
            gb_cols = [i for i in required_columns if not i.startswith('draw_')
                                                      and i!='population']
            temp_data['location_id'] = int(location_id)
            temp_data = temp_data.groupby(by=gb_cols).sum().reset_index()
        else:
            temp_data = pull_draws(cause, location_id, required_columns)
            for i in range(0,1000):
                draw_col = 'draw_{}'.format(i)
        data.append(temp_data)

    del(temp_data)
    data = pd.concat(data)

    logger = logging.getLogger('correct.read_all_model_draws')
    return data


def aggregate_causes(data, index_columns, data_columns, cause_hierarchy):
    logger = logging.getLogger('aggregate_causes.aggregate_causes')
    try:
        cause_hierarchy['level'] = cause_hierarchy['level'].astype('int64')
        min_level = cause_hierarchy['level'].min()
        data = data[index_columns + data_columns]
        data = pd.merge(data,
                        cause_hierarchy[['cause_id', 'level', 'parent_id',
                                         'most_detailed']],
                        on='cause_id',
                        how='left')
        data = data.loc[data['most_detailed'] == 1]
        max_level = data['level'].max()
        data = data[index_columns + data_columns]
        for level in range(int(max_level), min_level, -1):
            print("Level: {}".format(level))
            data = pd.merge(
                data, cause_hierarchy[['cause_id', 'level', 'parent_id']],
                on='cause_id', how='left')
            temp = data.loc[data['level'] == level].copy(deep=True)
            temp['cause_id'] = temp['parent_id']
            temp = temp[index_columns + data_columns]
            temp = temp.groupby(index_columns).sum().reset_index()
            data = pd.concat(
                [data[index_columns + data_columns], temp]
            ).reset_index(drop=True)

    except Exception as e:
        logger.exception('Failed to aggregate causes: {}'.format(e))
        sys.exit()

    return data


def save_draws(data, index_columns):

    logger = logging.getLogger('shocks.save_draws')

    for c in index_columns:
        data[c] = data[c].astype('int64')

    data = data.sort_values(index_columns).reset_index(drop=True)
    data.to_hdf(
        os.path.join(
            parent_dir, 'draws/shocks_{loc}.h5'.format(loc=location_id)),
        'draws', mode='w', format='table', data_columns=index_columns)

    draw_filepath = os.path.join(
        parent_dir, 'draws/shocks_{loc}.csv'.format(loc=location_id))
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

    sh_agg_log.setup_logging(log_dir, 'shocks', output_version_id, location_id)

    try:
        print("Reading in helper files")
        logging.info("Reading in helper files")
        (config,
         cause_hierarchy) = read_helper_files(parent_dir, location_id)

        index_columns = config['index_columns']
        data_columns = config['data_columns']
        raw_data_columns = index_columns + data_columns
        eligible_year_ids = config['eligible_year_ids']

        print("Reading in best model draws")
        logging.info("Reading in best model draws")
        raw_data = read_all_model_draws(raw_data_columns,
                                        location_id=location_id,
                                        filter_years=eligible_year_ids)
        print("Aggregating causes")
        logging.info("Aggregating causes")
        aggregated_shocks = aggregate_causes(raw_data, index_columns,
                                             data_columns, cause_hierarchy)

        print("Saving data")
        logging.info("Saving data")
        save_draws(aggregated_shocks, index_columns)
        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in shocks.py')
