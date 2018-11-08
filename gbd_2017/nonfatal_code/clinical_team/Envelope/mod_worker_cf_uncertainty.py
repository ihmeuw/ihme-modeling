# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import numpy as np
import sys
import glob
import getpass
import platform
import os
import re
import time
import functools
import db_queries
import datetime
from db_queries import get_location_metadata


prep_path = 'FILEPATH'
sys.path.append(prep_path)
repo = 'FILEPATH'
import hosp_prep

if platform.system() == "Linux":
    root = r"/FILEPATH/j"
else:
    root = "J:"

age = sys.argv[1]
sex = sys.argv[2]
year = sys.argv[3]

age = int(age)
sex = int(sex)
year = int(year)

job_id = "{}_{}_{}".format(age, sex, year)

def map_to_country_id(df):
    """
    CF1 and 2 are modeled by country id so map to this level of the location hierarchy before merging on
    """
    pre = df.shape[0]
    cols = df.shape[1]
    # get countries from locs
    locs = get_location_metadata(QUERY)
    countries = locs.loc[locs.location_type == 'admin0', ['location_id', 'location_ascii_name']].copy()
    countries.columns = ["cf_location_id", "country_name"]
    # get parent ids from locs
    df = df.merge(locs[['location_id', 'path_to_top_parent']], how='left', on='location_id')
    df = pd.concat([df, df.path_to_top_parent.str.split(",", expand=True)], axis=1)
    df['cf_location_id'] = df[3].astype(int)
    df = df.merge(countries, how='left', on='cf_location_id')
    
    # drop all the merging cols
    colnames = df.columns
    drops = ['path_to_top_parent', 0, 1, 2, 3, 4, 5, 6, 'country_name']
    for col in drops:
        if col in colnames:
            df.drop(col, axis=1, inplace=True)
    # run a few tests
    assert df.shape[0] == pre
    assert df.shape[1] == cols + 1, "col size is {} expected {}".format(df.shape[1], cols+1)
    assert df.cf_location_id.isnull().sum() == 0,\
        "Something went wrong {}".format(df[df.cf_location_id.isnull()])

    return df


def env_reader(root, age, sex):
    # read in draw envelope
    env = pd.read_hdf(r"FILEPATH/{}_{}.H5".\
                      format(root, int(age), int(sex)))
    print("read in the env subset", env.shape)
    # subset the envelope using the inputs above
    cond = "(env.age_start == age) & (env.year_start == year) & (env.sex_id == sex)"
    env = env[eval(cond)].copy()
    # drop unneeded cols
    env_cols = env.columns
    drops = ['measure_id', 'modelable_entity_id',
              'mean', 'upper', 'lower', 'age_end']
    for col in drops:
        if col in env_cols:
            env.drop(col, axis=1, inplace=True)
    return env

 
def test_cols_to_mult(draw_cols, cf_cols):
    """
    There should be exactly 1,000 columns from each cf that we're
    multiplying across
    """
    assert draw_cols.size == 1000, "Wrong number of draw cols"
    
    assert cf_cols.size == 1000, "Wrong number of cf cols"
    return


def test_new_cols(df, draw_cols, cf_cols, new_draw_cols, ncols=13):
    """
    sample a few of the newly created draw cols to make sure that the
    multiplication is performing as expected

    Parameters:
        df: Pandas DataFrame
            dataframe with the correction factor draws applied
        draw_cols: list of str
            list of the hospital envelope draws
        cf_cols: list of str
            list of the correction factor draws
        new_draw_cols: list of str
            The result of multiplying the env draws by CF draws
        ncols: int
            number of columns to sample when testing
    """
    cols = df.filter(regex="^new_draw_").columns.size
    cols_to_test = pd.Series(np.arange(0, cols, 1)).sample(ncols).tolist()
    for i in cols_to_test:
        np.testing.assert_allclose(actual=df[draw_cols[i]] * df[cf_cols[i]],
                                   desired=df[new_draw_cols[i]])
    return

def apply_cf_uncertainty(df,
        cf_path="FILEPATH/"\
                       r"modified_cf_draws",
                       dev=False):
    """
    We need to multiple across draws to get the uncertainty
    from sampling the correction factors onto the envelope
    uncertainty and then onto the data

    Arguments:
        df: Pandas DataFrame
            consists of the interpolated hospital envelope
        with all 1,000 draws
        cf_path: str
            the parent directory to pull the smoothed correction factor
            files from
        dev: Bool
            Run on a small subset of the data to see if any syntax breaks
    """

    if "age_start" not in df.columns:
        df = hosp_prep.group_id_start_end_switcher(df)

    if dev:
        df = df.head(200)
    start = time.time()
    # make a list to append everything to
    df_list = []
    # copy the envelope
    raw_df = df.copy()
    # create raw mean upper lower env values
    draw_cols = raw_df.filter(regex="^draw_").columns

    raw_df['mean'] = raw_df[draw_cols].median(axis=1)
    # calculate quantiles for every row, then transpose and col bind
    quant = raw_df[draw_cols].quantile([0.025, 0.975], axis=1).transpose()
    # rename columns
    quant.columns = ['lower', 'upper']
    # drop draw cols
    raw_df.drop(draw_cols, axis=1, inplace=True)
    # col bind quant to env
    raw_df = pd.concat([raw_df, quant], axis=1)
    #df_list.append(raw_df)
    
    # add a test to make sure bundles aren't being dropped
    bundle_list = []

    # we want to do the same merge, multiply, condense operation 3 times
    # Changing this part to add a fourth with the new cf3
    print("processing the modified CFs by age and sex \n")
    for fpath in ['prevalence', 'incidence', 'indvcf']:
        print("\n\n beginning to process {}\n".format(fpath.upper()))

        cf_cols = fpath

        type_name = "mod" + fpath

        read_path = "FILEPATH/{}_{}.csv".format(fpath, age, sex)
        cf_df = pd.read_csv(read_path)

        bundle_list.append(cf_df.bundle_id.unique().tolist()) 

        if "sex" in cf_df.columns:
            cf_df.rename(columns={'sex': 'sex_id'}, inplace=True)
        if "Unnamed: 0" in cf_df.columns:
            cf_df.drop("Unnamed: 0", 1, inplace = True) 
        if dev:
            # this is just for dev
            cf_df = cf_df[(cf_df.age_start.isin(df.age_start.unique())) & (cf_df.sex_id.isin(df.sex_id.unique()))]

        # merge envelope onto CF by age and sex
        print("pre merge df shape is {}".format(df.shape))
        print("pre merge cf_df shape is {}".format(cf_df.shape))
        ## Merge on location too if looking at modeled CF
        if 'cf_location_id' in cf_df.columns:
            print("merging on mod CFs using cf location id, age and sex")

            if cf_df.cf_location_id.unique().size < 10:
                print("{} will be merged on by super region".format(fpath))
                ## Change location to super region and swap around
                
                locs = get_location_metadata(QUERY)
                locs = locs[['location_id', 'super_region_id']]
                locs.rename(columns={'super_region_id': 'cf_location_id'}, inplace=True)
                pre = df.shape[0]
                df = df.merge(locs, how='left', on='location_id')
                assert pre == df.shape[0], "merging on super region has duplicated some data"

            else:
                print("{} will be merged on by country".format(fpath))
                # merge the CFs on by country
                pre = df.shape[0]
                df = map_to_country_id(df)
                assert pre == df.shape[0], "merging on country id has duplicated some data"
            
            print(df.shape)
            print(cf_df.shape)
            df.to_csv('FILEPATH/df.csv')
            cf_df.to_csv('FILEPATH/cf_df.csv')
            test = df.merge(cf_df, how='left', on=['age_start', 'sex_id', 'cf_location_id'])
            print(test.shape)
            print('these shapes are the size of test, which will then get written as temp draw files')
            print('dont want them to break here')

        else:
            print("merging on CFs with our standard process by age and sex")
            test = df.merge(cf_df, how='left', on=['age_start', 'sex_id']) 
        print("post merge shape is {}".format(test.shape))

        # check to see if the correct number of rows are created
        assert test.shape[0] == df.shape[0] * cf_df.bundle_id.unique().size,\
            "There are {} rows after the merge and we expect {} rows."\
            " {} from the envelope * {} bundles".\
            format(test.shape[0],
                   df.shape[0] * cf_df.bundle_id.unique().size,
                   df.shape[0],
                   cf_df.bundle_id.unique().size)
        # prep to multiply CF * envelope draw by draw
        draw_cols = test.filter(regex="^draw_").columns
        if 'cf_location_id' in test.columns:
            cf_cols = test.filter(regex=cf_cols).columns
        else:
            cf_cols = test.filter(regex="_sm$").columns
        test_cols_to_mult(draw_cols, cf_cols)
        # create new names for the product
        new_draw_cols = ["new_" + x for x in draw_cols]
        
        test[new_draw_cols] = test[draw_cols].multiply(test[cf_cols].values)
        print("after mult the size is", test.shape)
        
        test_new_cols(test, draw_cols, cf_cols, new_draw_cols)
        # don't need the original columns anymore
        test.drop(cf_cols.tolist() + draw_cols.tolist(),
                  axis=1, inplace=True)
        
        test['mean_{}'.format(type_name)] = test[new_draw_cols].median(axis=1)
        # find the 2.5th percentile and 97.5 percentile
        # calculate quantiles for every row, then transpose and col bind
        quant = test[new_draw_cols].quantile([0.025, 0.975], axis=1).transpose()
        # rename columns
        quant.columns = ['lower_{}'.format(type_name), 'upper_{}'.format(type_name)]
        # col bind quant to env
        test = pd.concat([test, quant], axis=1)
        # drop the draws
        test.drop(new_draw_cols, axis=1, inplace=True)
        test.drop('cf_location_id', axis=1, inplace=True)
        df.drop('cf_location_id', axis=1, inplace=True)
        # append the data to df list
        df_list.append(test)
        print("one loop done in {} seconds".format(time.time()-start))
    final_df = functools.reduce(lambda x, y: pd.merge(x, y, on=['age_start', 'age_group_id',
                                              'sex_id', 'year_start', 'year_end',
                                              'location_id', 'bundle_id', ]),
        df_list)
    
    final_df = final_df.merge(raw_df, how='left', on=['age_start', 'age_group_id',
                                              'sex_id', 'year_start', 'year_end',
                                              'location_id'])
    final_df['age_start'] = final_df['age_start'].astype(int)
    print("The shape of the data we'll write is {}".format(final_df.shape))

    bundle_list = [item for sublist in bundle_list for item in sublist]
    # make sure bundles aren't lost
    bdiff = set(bundle_list).symmetric_difference(set(final_df.bundle_id.unique()))
    
    # this is our new env
    return final_df

def temp_writer(df):
    """
    write the data we've processed in parallel to be read up by the
    master script
    """
    num_cols = ['age_group_id', 'location_id', 'sex_id', 'age_start',
                'year_start', 'year_end', 'bundle_id']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')
    ## Write
    write_path = r"FILEPATH/{}_{}_{}.H5".format(int(age), int(sex), int(year))
    df.to_hdf(write_path, key="df", mode="w", format="table")
    return

def main():
    df = env_reader(root, age, sex)
    ulocs = df.location_id.unique().size
    
    df = apply_cf_uncertainty(df, dev=False)
    
    assert ulocs == df.location_id.unique().size, "Some location IDs changed (unexpectedly??)"
    temp_writer(df)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
        e = str(e)
        fail = pd.DataFrame({'fail_date': today, 'age_sex_year': job_id, 'error': e}, index=[0])
        fail.to_hdf("FILEPATH/failed_mod_worker_cf_unc.H5",
            key='df', mode='a', format='table', append=True)
