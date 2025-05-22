import argparse
import numpy as np
import os
import pandas as pd
import sys
import time
from os.path import join
from datetime import datetime
from db_queries import (get_demographics,
                        get_demographics_template,
                        get_location_metadata,
                        get_population,
                        )
from multiprocessing import Pool

import getpass
USER = getpass.getuser()
RELEASE_ID = 16

def get_time_now():
    ts = time.time()
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def get_summary_data(infile, cause_id):
    print("**GETTING SUMMARY DATA*** {}".format(get_time_now()))
    full_df = pd.read_csv(infile)
    cause_df = full_df.query("cause_id == {}".format(cause_id))
    required_cols = ['year_id', 'location_id', 'sex_id',
                     'age_group_id', 'val', 'lower', 'upper']
    for col in required_cols:
        assert col in cause_df.columns, "Required column '{}' not in data".format(col)
    locs = get_location_metadata(location_set_id=21,
                                 release_id=RELEASE_ID
                                 ).query("most_detailed == 1")['location_id']
    years = list(range(1950, 2024))
    cause_df = cause_df.loc[cause_df['location_id'].isin(locs), :]
    cause_df = cause_df.loc[cause_df['year_id'].isin(years), :]
    print("  ...Summary data pulled. {}\n".format(get_time_now()))
    return cause_df
def transform_to_rate_space(cause_df, pop_filepath):
    print("**TRANSFORMING TO RATE SPACE**")
    full_pops = pd.read_csv(pop_filepath)
    full_pops = full_pops.loc[:, ['age_group_id', 'sex_id', 'year_id', 'location_id', 'population']]
    cause_df = pd.merge(left=cause_df,
                        right=full_pops,
                        on=['age_group_id', 'sex_id', 'year_id', 'location_id'],
                        how='inner'
                        )
    for col in ['val', 'lower', 'upper']:
        cause_df[col] = cause_df[col] / cause_df['population']
        cause_df.loc[cause_df[col] > 1, col] = 1
        cause_df.loc[cause_df[col] < 0, col] = 0
    print("  ...Successfully transformed summary values into rate space.\n")
    return cause_df

def gen_thousand_draws(mean, sd, pop):
    np.random.seed(seed=42)
    final_draws = np.random.lognormal(mean=mean, sigma=sd, size=1000)
    s = pd.Series(dict(zip(['draw_{}'.format(i) for i in range(0, 1000)],
                           final_draws)))
    mean_shift = np.exp(mean) / s.mean()
    s = s * mean_shift
    s = s * pop
    return s

def draw_churner(df):
    draws = pd.concat([df,
                       df.apply(lambda row:
                                gen_thousand_draws(row['val'],
                                                   row['se'],
                                                   row['population']),
                                axis=1)],
                      axis=1)
    return draws

def rates_to_draws(rate_df):
    assert rate_df.shape[0] > 0, "There are no rows left to include..."
    print("**GENERATING DRAWS FOR {} ROWS**".format(rate_df.shape[0]))
    print("  Time at start: {}".format(get_time_now()))
    rate_df['upper'] = np.log(rate_df['upper'])
    rate_df['lower'] = np.log(rate_df['lower'])
    rate_df['val'] = np.log(rate_df['val'])
    rate_df['se'] = (rate_df.upper - rate_df.lower) / (2 * 1.96)
    assert ~pd.isnull(rate_df['se']).any()
    draws_df = draw_churner(rate_df)
    draws_df = draws_df.drop(labels=['se', 'upper', 'lower', 'population'], axis=1)
    print("  ...Successfully generated draws.")
    print("  Time at end: {}\n".format(get_time_now()))
    return draws_df

def get_estimation_locs(release_id=RELEASE_ID, location_set_id=35):
    meta = get_location_metadata(release_id=RELEASE_ID,
                                 location_set_id=location_set_id)
    most_detailed = (meta.loc[(meta['is_estimate'] == 1) | (meta['most_detailed'] == 1),
                              'location_id'].tolist())
    return most_detailed

def location_saver(location, draws_df, template, output_dir):
    loc_output_file = join(output_dir, "{}.csv".format(location))
    print("Saving location {}...".format(location))
    loc_draws = template.copy()
    loc_draws['location_id'] = location
    loc_draws = pd.merge(
        left=loc_draws,
        right=draws_df,
        on=['location_id', 'year_id', 'age_group_id', 'sex_id'],
        how='left')
    loc_draws = loc_draws.fillna(0)
    loc_draws.to_csv(loc_output_file, encoding="utf8")
    print(loc_output_file)
    time.sleep(1)
    assert os.path.isfile(loc_output_file), "a file did not save correctly"

def save_by_location(draws_df, output_dir, encoding):
    print("**SAVING CSV FILES BY LOCATION**")
    print("  Time at start: {}".format(get_time_now()))
    # Keep only necessary columns:
    indexing_cols = ['age_group_id', 'sex_id', 'year_id', 'location_id']
    draw_cols = [i for i in draws_df.columns if i.startswith("draw_")]
    draws_df = draws_df.loc[:, indexing_cols + draw_cols]
    for col in indexing_cols:
        draws_df[col] = draws_df[col].astype(np.int32)
    template = get_demographics_template(release_id=16, gbd_team='cod')
    template = template.query("location_id == 10")
    template = template.drop("location_id", axis=1)
    template = template.query("year_id == 2024")
    full_template = pd.DataFrame()
    for year in range(1950, 2025):
        template['year_id'] = year
        full_template = pd.concat([full_template, template])
    template = full_template
    locs = list(get_location_metadata(release_id=RELEASE_ID, location_set_id=21).query("most_detailed == 1")['location_id'])
    locs2 = get_estimation_locs(release_id=RELEASE_ID, location_set_id=21)
    locs = list(set(locs + locs2))
    pool = Pool(25)
    for location in locs:
        location_df = draws_df.copy().query("location_id == {}".format(location))    
        pool.apply_async(location_saver, (location, location_df, template, output_dir))
    pool.close()
    pool.join()

    print("  ...All files saved successfully.")
    print("  Time at end: {}\n".format(get_time_now()))
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cause_id", type=int,
                        help="The cause ID of that these draws will save")
    parser.add_argument("-o", "--output_dir", type=str,
                        help="the folder the draws will be saved at")
    parser.add_argument("-p", "--population_filepath", type=str,
                        help="The population that should be used in the draws")
    cmd_args = parser.parse_args()
    cause_id = cmd_args.cause_id
    output_dir = cmd_args.output_dir
    pop_filepath = cmd_args.population_filepath
    infile=FILEPATH
    cause_df = get_summary_data(infile=infile,
                                cause_id=cause_id)
    rate_df = transform_to_rate_space(cause_df, pop_filepath)
    cause_draws = rates_to_draws(rate_df)
    save_by_location(draws_df=cause_draws,
                     output_dir=output_dir,
                     encoding="utf8")
