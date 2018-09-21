import argparse
import os
import time
import pandas as pd
from db_queries import get_population
from draw_io import get_best_envelope_version
from sorter import set_sort_index

def prep_envelope(years, upload_dir, vers):
    envelope_version_id = get_best_envelope_version()

    file_path = ('/FILEPATH/v{}/FILEPATH'.format(envelope_version_id))
    all_files = ['{fp}/combined_env_aggregated_{yr}.h5'.format(
            fp=file_path, yr=year) for year in years]
    envelope_list = []
    for f in all_files:
        envelope_list.append(pd.read_hdf(f, 'draws'))
    envelope = pd.concat(envelope_list)

    # Keep only what we need
    index_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
    draw_cols = ['env_{}'.format(x) for x in range(1000)]
    envelope = envelope[index_cols + draw_cols]

    envelope = set_sort_index(envelope,index_cols)

    output_dir = '%s/v%s/temps' % (upload_dir, vers)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        time.sleep(1)
    envelope.to_hdf('%s/envelope.hdf' % output_dir, 'draws', format='table')

def prep_pop(years, upload_dir, vers):
    population = get_population(year_id=years, location_id=-1,
                                age_group_id=-1, sex_id=-1)
    index_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']

    population = population[index_cols + ['population']]

    population = set_sort_index(population, index_cols)

    output_dir = '%s/v%s/temps' % (upload_dir, vers)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        time.sleep(1)
    population.to_csv('%s/population.csv' % output_dir)

def run_dem_prep(years, upload_dir, vers):
    prep_envelope(years, upload_dir, vers)
    prep_pop(years, upload_dir, vers)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--years",
            help="years",
            dUSERt=range(1980, 2017),
            type=list)
    parser.add_argument(
            "--upload_dir",
            help="input directory",
            dUSERt="",
            type=str)
    parser.add_argument(
            "--vers",
            help="version",
            dUSERt=0,
            type=int)
    args = parser.parse_args()
    years = args.years
    upload_dir = args.upload_dir
    vers = args.vers

    run_dem_prep(years, upload_dir, vers)
