'''
GENERATE DRAWS


Purpose: Generate draws given an incomplete set of mean, upper, and lower values
  for a set of locations, years, ages, and sexes
'''
import argparse
import gc
import numpy as np
import os
import pandas as pd
import sys
import time
from os.path import join
from datetime import datetime
from db_queries import (get_demographics,
                        get_demographics_template as get_template,
                        get_location_metadata as lm,
                        get_population)
from time import sleep

################################################################################
# Helper functions
################################################################################
def get_estimation_locs(gbd_round_id=5, location_set_id=35):
    meta = lm(gbd_round_id=gbd_round_id,
              location_set_id=location_set_id)
    most_detailed = (meta.loc[(meta['is_estimate']==1)|(meta['most_detailed']==1),
                              'location_id'].tolist())
    return most_detailed


def get_time_now():
    ts = time.time()
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def get_summary_data(infile,encoding,cause):

    print("**GETTING SUMMARY DATA*** {}".format(get_time_now()))
    full_data = pd.read_csv(infile, encoding=encoding)
    summary_df = full_data.loc[full_data['cause_id']==cause,:]
    # Data assertions
    required_cols = ['year_id','location_id','sex_id','age_group_id',
                 'val','lower','upper']
    for col in required_cols:
        assert col in summary_df.columns, "Required column '{}' not in data".format(col)
    # Subset down to only location_ids and years that are required for saving
    demos = get_demographics(gbd_team='cod',gbd_round_id=5)
    years = list(range(1950,2018))
    summary_df = summary_df.loc[summary_df['location_id'].isin(demos['location_id']),:]
    summary_df = summary_df.loc[summary_df['year_id'].isin(years),:]
    print("  ...Summary data pulled. {}\n".format(get_time_now()))
    return summary_df


def assemble_gbd2016_best(path_to_best,
                          cause_id,
                          location_ids,
                          return_full=False):

    # Check to see if the cause exists in one of the files
    global_file = pd.read_hdf(join(path_to_best,'shocks_1.h5'))
    if cause_id not in global_file['cause_id'].unique().tolist():
        return None
    del(global_file)
    # Get the list of all possible files to read from
    folder_files = os.listdir(path_to_best)
    possible_files = ['shocks_{}.h5'.format(loc) for loc in location_ids]
    files_to_read = [join(path_to_best,f) for f in possible_files if f in folder_files]
    if len(files_to_read)==0:
        return None
    print("**PULLING GBD 2016 BEST ESTIMATES** {}".format(get_time_now()))
    print("  This could take a while...")
    # Assemble the list of summary data
    by_location = list()
    draw_cols = ['draw_{}'.format(i) for i in range(0,1000)]
    for f in files_to_read:
        # Iteratively process files and add to list
        sub_df = pd.read_hdf(f)
        sub_df = sub_df.loc[sub_df['cause_id']==cause_id,:]
        if not return_full:
            sub_df['val'] = sub_df.loc[:,draw_cols].mean(axis=1)
            sub_df['lower'] = (sub_df.loc[:,draw_cols]
                                     .apply(lambda x: np.percentile(x,2.5),axis=1))
            sub_df['upper'] = (sub_df.loc[:,draw_cols]
                                     .apply(lambda x: np.percentile(x,97.5),axis=1))
            sub_df = sub_df.drop(labels=draw_cols,axis=1,errors='ignore')
        by_location.append(sub_df)
    del(sub_df)
    # Concatenate all the data into a single frame
    full_data = pd.concat(by_location)
    del(by_location)
    gc.collect()
    # Return the full best data from GBD 2016
    print("  ...GBD 2016 best estimates pulled. {}".format(get_time_now()))
    return full_data


def vet_against_gbd2016_best(summary_db, cause_id, path_to_best):

    print("**VETTING AGAINST 2016 SHOCKS**")
    # Get all most-detailed location IDs for GBD 2017
    locs = get_estimation_locs()
    # Generate full summary data (val, lower, upper) for the GBD 2016 best model
    old_db = assemble_gbd2016_best(path_to_best=path_to_best,
                                   cause_id=cause_id,
                                   location_ids=locs)

    if old_db is None:
        print("  ...No GBD 2016 data was found.")
        return summary_db

    print("  Comparing to GBD 2016 Data...")
    index_cols = ['age_group_id','sex_id','year_id','location_id','cause_id']
    old_db = old_db.loc[:,index_cols + ['val','lower','upper']]
    diff = pd.merge(left=summary_db,
                    right=old_db,
                    on=index_cols,
                    how='left',
                    suffixes=('','_old'))
    # ************************ VETTING, ROUND 1 ************************
    diff['abs_diff'] = np.abs(diff['val'] - diff['val_old'])

    need_replace = (diff['abs_diff'] > 10000)
    for col in ['val','lower','upper']:
            diff.loc[need_replace,col] = diff.loc[need_replace,'{}_old'.format(col)]
    # ************************ VETTING, ROUND 1 ************************
    if cause_id in [696,699,707,689,693,695,724,854,842,990]:
        diff['dir_diff'] = diff['val'] - diff['val_old']

        need_replace = (diff['dir_diff'] > 1000)
        for col in ['val','lower','upper']:
            diff.loc[need_replace,col] = diff.loc[need_replace,'{}_old'.format(col)]
    vetted_db = diff.drop(labels=['val_old','lower_old','upper_old',
                                  'abs_diff','dir_diff'],
                          axis=1,errors='ignore')
    print("  ...Vetting against GBD 2016 data complete.")
    return vetted_db


def transform_to_rate_space(summary_df):
    '''
    Transform the summary data into rate space to allow for easier validation
    '''
    print("**TRANSFORMING TO RATE SPACE**")
    demos = get_demographics(gbd_round_id=5, gbd_team='cod')
    year_ids = list(range(1950,2018))
    full_pops = get_population(gbd_round_id=5,
                               age_group_id=demos['age_group_id'],
                               sex_id=demos['sex_id'],
                               year_id=year_ids,
                               run_id= 104,
                               location_id=demos['location_id'])
    full_pops = full_pops.loc[:,['age_group_id','sex_id','year_id','location_id',
                                'population']]
    summary_df = pd.merge(left=summary_df,
                          right=full_pops,
                          on=['age_group_id','sex_id','year_id','location_id'],
                          how='inner')
    for col in ['val','lower','upper']:
        summary_df[col] = summary_df[col] / summary_df['population']
        summary_df.loc[summary_df[col]>1,col] = 1
        summary_df.loc[summary_df[col]<0,col] = 0
    summary_df = summary_df.drop(labels=['population'], axis=1)
    print("  ...Successfully transformed summary values into rate space.\n")
    return summary_df


def gen_thousand_draws(mean, sd):
    # Set means to within reasonable limits
    if mean < 0:
        mean = 0
    if mean > 1:
        mean = 1
    # log-normal distribution:
    # generate 1000 draws from the log-normal distribution
    if sd < 0.00000001:
        return pd.Series(dict(zip(['draw_{}'.format(i) for i in range(0,1000)],
                                  np.full(1000,mean))))
    final_draws = np.random.lognormal(mean=np.log(mean), sigma=sd, size=1000)

    s = pd.Series(dict(zip(['draw_{}'.format(i) for i in range(0,1000)],
                           final_draws)))
    count = len(s[s>1])

    s = s.clip(0,1)
    return s

def summary_to_draws(summary_df):
    '''
    Convert the mean, upper, and lower values to 1000-draw distributions.
    '''
    assert summary_df.shape[0] > 0, "There are no rows left to include..."
    print("**GENERATING DRAWS FOR {} ROWS**".format(summary_df.shape[0]))
    print("  Time at start: {}".format(get_time_now()))
    # Create a new "se" (standard error) column
    summary_df['se'] = (summary_df['upper'] - summary_df['lower'])/(2 * 1.96)
    # Create 1000 new draws columns in the dataframe
    draws_df = pd.concat([summary_df,
                          summary_df.apply(lambda row: 
                            gen_thousand_draws(row['val'], row['se']),axis=1)],
                          axis=1)
    draws_df = draws_df.drop(labels=['se','upper','lower'],axis=1)
    print("  ...Successfully generated draws.")
    print("  Time at end: {}\n".format(get_time_now()))
    # Return the draws dataframe
    return draws_df


def save_by_location(draws_df, output_dir, full_output_dir, encoding):
    # Ensure that the folder exists
    print("**SAVING HDF FILES BY LOCATION**")
    print("  Time at start: {}".format(get_time_now()))
    if not (os.path.exists(output_dir) and os.path.isdir(output_dir)):
        os.mkdir(output_dir)
    if not (os.path.exists(full_output_dir) and os.path.isdir(full_output_dir)):
        os.mkdir(full_output_dir)
    # Keep only necessary columns:
    indexing_cols = ['age_group_id','sex_id','year_id','location_id']
    draw_cols = [i for i in draws_df.columns if i.startswith("draw_")]
    draws_df = draws_df.loc[:,indexing_cols + draw_cols]
    for col in indexing_cols:
        draws_df[col] = draws_df[col].astype(np.int32)
    # Function to create a full template dataframe for this round
    def get_full_template(gbd_team, gbd_round_id):
        years = pd.DataFrame({'year_id':list(range(1950,2018))})
        years['__merge'] = 1
        other = (get_template(gbd_team=gbd_team,gbd_round_id=gbd_round_id)
                   .drop(labels=['year_id','location_id'],axis=1,errors='ignore')
                   .drop_duplicates())
        other['__merge'] = 1
        temp_full = (pd.merge(left=years, right=other, on=['__merge'], how='inner')
                       .drop(labels=['__merge'],axis=1))
        return temp_full
    # GET THE FULL TEMPLATE
    template = get_full_template(gbd_team='cod',gbd_round_id=5)
    locs = get_demographics(gbd_team='cod',gbd_round_id=5)['location_id']
    locs2 = get_estimation_locs(gbd_round_id=5, location_set_id=21)
    locs = list(set(locs + locs2))
    for location in locs:
        loc_output_file = join(output_dir,"{}.csv".format(location))
        needs_save = True
        try:
            test = pd.read_hdf(loc_output_file)
            assert test.shape[0] > 0
            del(test)
            gc.collect()
            needs_save=False
        except:
            pass
        if needs_save:
            print("Saving location {}...".format(location))
            # Create a table containing only that location
            loc_draws = template.copy(deep=True)
            loc_draws['location_id'] = location
            # Join on results for that location only
            loc_draws = pd.merge(
                             left=loc_draws,
                             right=draws_df,
                             on=['location_id','year_id','age_group_id','sex_id'],
                             how='left')
            # Fill missing values with zeroes
            loc_draws = loc_draws.fillna(0)
            # Save ALL YEARS to the full-year folder using the location ID as the filename
            full_output_path = join(full_output_dir,"{}.csv".format(location))
            if os.path.exists(full_output_path):
                os.remove(full_output_path)
            save_worked=False
            save_tries = 0
            while save_tries <= 3 and not save_worked:
                try:
                    loc_draws.to_csv(full_output_path,encoding=encoding)
                    save_worked = True
                except:
                    save_tries = save_tries + 1
            loc_draws = loc_draws.loc[loc_draws['year_id'] >= 1980,:]
            if os.path.exists(loc_output_file):
                os.remove(loc_output_file)
            save_worked=False
            save_tries = 0
            while save_tries <= 3 and not save_worked:
                try:
                    loc_draws.to_csv(loc_output_file,encoding=encoding)
                    save_worked=True
                except:
                    save_tries = save_tries + 1
            del(loc_draws)
            gc.collect()
    print("  ...All files saved successfully.")
    print("  Time at end: {}\n".format(get_time_now()))
    return None


if __name__=="__main__":
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--cause_id",type=int,
                        help="The cause_id for which draws should be generated")
    parser.add_argument("-o","--infile",type=str,
                        help="The CSV file that needs to be split")
    parser.add_argument("-n","--encoding",type=str,
                        help="Encoding for input CSV file")
    parser.add_argument("-s","--savedir",type=str,
                        help="The output directory where all HDF files will be "
                             "saved.")
    cmd_args = parser.parse_args()
    cause_id = cmd_args.cause_id
    PATH_TO_OLD_BEST = 'FILEPATH'
    FULL_DATA_STORAGE = ('FILEPATH')


    if False:
        pass

    else:
        # Get the data for this cause_id
        cause_summary = get_summary_data(infile=cmd_args.infile,
                                         encoding=cmd_args.encoding,
                                         cause=cmd_args.cause_id)

        rates = transform_to_rate_space(cause_summary)
        # Generate draws for the cause id
        cause_draws = summary_to_draws(rates)
    # No matter which method was used to generate draws, save that
    # to a series of HDF files by location ID
    save_by_location(draws_df=cause_draws,
                     output_dir=join(cmd_args.savedir,str(cause_id)),
                     full_output_dir=join(FULL_DATA_STORAGE,str(cause_id)),
                     encoding=cmd_args.encoding)
    # Wait a few seconds so that the file system has time to recognize
    # new files have been created
    sleep(10)

