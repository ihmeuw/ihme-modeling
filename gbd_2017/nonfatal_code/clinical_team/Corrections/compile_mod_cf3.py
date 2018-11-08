"""
compile modified CFs


"""

import pandas as pd
import numpy as np
import glob
import platform
import getpass
import sys
import os
import re
import warnings
import time
import multiprocessing
import functools
from db_queries import get_location_metadata

user = getpass.getuser()
prep_path = r"filepath".format(user)
sys.path.append(prep_path)
import hosp_prep


def get_bundle_paths(cfpath):
    cf_bundles = glob.glob(cfpath + "*.csv")
    len(cf_bundles)
    cm = pd.read_csv(r"filepath".format(root))
    assert hosp_prep.verify_current_map(cm), "The map version is not correct"
    
    # create a list of bundle id ints to check against the map
    bundles = [int(re.sub("[^0-9]", "", os.path.basename(f))) for f in cf_bundles]
    map_bundles = cm[cm.bundle_id.notnull()].bundle_id.unique()
    bdiff = set(bundles).symmetric_difference(set(map_bundles))
    if bdiff:
        msg = """
        {} bundles are different between the current map and the CF folder. Bundles {}
        are present in the CFs but not the map and bundles {} are present in the map but
        not the CF folder.
        """.format(len(bdiff), set(bundles) - set(map_bundles), set(map_bundles) - set(bundles))
        warnings.warn(msg)
    return cf_bundles


def require_cols(df, bpath):
    failed = []
    df.columns = df.columns.str.lower()
    dfcols = df.columns
    required = ['location_id', 'age_start', 'sex_id', 'bundle_id']
    for col in required:
        try:
            assert col in dfcols, "{} is missing from the df from file {}".format(col, bpath)
        except Exception as e:
            print(e)
            failed.append(e)
    # check for the draws
    draw_cols = df.filter(regex="[0-9]$").columns

    # fix this before running prod
    if draw_cols.size != 1000:
        warnings.warn("wrong number of draws!")

    # convert inf to null
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df, failed


def process_bundle(bpath):
    df = pd.read_csv(bpath)

    if df.shape[0] == 0:
        print("bundle path {} has no records".format(bpath))
        return "skip this bundle", "nope"

    if 'bundle_id' not in df.columns:
        df['bundle_id'] = int(re.sub("[^\d]", "", os.path.basename(bpath)))

    if "Unnamed: 0" in df.columns:
        df.drop("Unnamed: 0", axis=1, inplace=True)
    
    df, failed = require_cols(df, bpath)

    
    exp_rows = df.location_id.unique().size * df.age_start.unique().size * df.sex_id.unique().size
    assert exp_rows == df.shape[0], "bundle path {} has {} rows, expected {}".format(bpath, df.shape[0], exp_rows)
        
    return df, failed


def test_bundle_cf(df, draw_name):
    draw_cols = df.filter(regex="[0-9]$").columns
    
    all_missing = (df[draw_cols].isnull().sum() / df.shape[0] == 1).all()
    
    missingness = (df[draw_cols].isnull().sum() / float(df.shape[0])).mean()
    if missingness > 0.949999:
        print("95% or more of the draws are null")
    # no negative CFs
    if not (df[draw_cols].min() >= 0).all():
        if missingness < 1:
            msg = """
            The draw cols for bundle {} contain values below 0
            """.format(df.bundle_id.unique())
            warnings.warn(msg)
    
    # no infinite values
    assert (df[draw_cols] == np.inf).sum().sum() == 0, "there are infinite values present"
    
    # no values over 1 for cf1
    if draw_name == "indvcf":
        if all_missing:
            warnings.warn("bundle {} has 100% missing draws for {}".format(df.bundle_id.unique(), draw_name))
            return
        else:
            assert (df[draw_cols].max() <= 1).all(), "CF1 has values over 1 for bundle {}".format(df.bundle_id.unique())
    
    if all_missing:
        warnings.warn("bundle {} has 100% missing draws for {}".format(df.bundle_id.unique(), draw_name))
    
    return


def name_draws(df, draw_name):
    draw_cols = df.filter(regex="[0-9]$").columns
    if '1000' in draw_cols:
        print("the offending bundle is {} fixing now...".format(df.bundle_id.unique()))
        draw_cols2 = (pd.to_numeric(draw_cols) - 1).astype(str)
        assert '1000' not in draw_cols2, "it won't die"
    else:
        draw_cols2 = draw_cols

    # assert draw_cols.size == 1000, "wrong number of draw cols"
    
    # remove any existing non digit strings from the cols
    num_cols = [re.sub("[^0-9]", "", f) for f in draw_cols2]
    # rename cols
    new_draw_cols = ["mod_{}_{}".format(draw_name, f) for f in num_cols]
    df.rename(columns=dict(zip(draw_cols, new_draw_cols)), inplace=True)
    assert '1000' not in df.columns
    return df
    

def pooled_bundle_proc(bpath, draw_name):
    df, failed = process_bundle(bpath)
    if type(df) == str:
        if df == "skip this bundle":
            return

    df = name_draws(df, draw_name)
    test_bundle_cf(df, draw_name)
    if failed:
        print(failed)
    return(df)

def pooled_writer(df, draw_name):
    assert df.age_start.unique().size == 1
    assert df.sex_id.unique().size == 1
    age = df.age_start.iloc[0]
    sex = df.sex_id.iloc[0]
    
    write_path = "filepath"
    df.to_csv(write_path, index=False)


def remove_compiled_files(draw_name):
    files = glob.glob("filepath")
    if len(files) != 42:
        warnings.warn("why are there not 42 csv files here?")
    for f in files:
        os.remove(f)
    return


def test_write(draw_name):
    files = glob.glob("filepath")
    exp_files = ["{}_{}".format(a, s) for a in [0, 1] + list(np.arange(5, 96, 5)) for s in [1,2]]
    obs_files = [os.path.basename(f)[:-4] for f in files]
    diff = set(exp_files).symmetric_difference(set(obs_files))

    assert len(files) == 42, "The wrong number of files were written, an age or sex group is missing {}".format(diff)
    assert not diff, "Something is wrong with the expected files {}".format(diff)
    return



if __name__ == "__main__":
    true_start = time.time()
    draw_dir = "filepath"
    cf_dirs = [draw_dir + "/cf{}_by_bundle/".format(i) for i in np.arange(1, 4, 1)]
    for cfpath in cf_dirs:

        loop_start = time.time()
        if 'cf1' in cfpath:
            draw_name = "indvcf"
        elif 'cf2' in cfpath:
            draw_name = 'incidence'
        elif 'cf3' in cfpath:
            draw_name = 'prevalence'

        remove_compiled_files(draw_name)
        df_list = []
        draw_types = []
        failed_list = []
        cf_paths = get_bundle_paths(cfpath)

        partial_bproc = functools.partial(pooled_bundle_proc, draw_name=draw_name)
        p = multiprocessing.Pool(10)
        df_list = list(p.map(partial_bproc, cf_paths))
        # append all the bundle dfs for one cf together
        res = pd.concat(df_list, ignore_index=True)
        print("concatted data is {}".format(res.shape))
        
        draw_cols = res.filter(regex="[0-9]$").columns
        assert draw_cols.size == 1000, "wrong number of draw cols"
        
        # rename loc id to super region id
        res.rename(columns={'location_id': 'cf_location_id'}, inplace=True)
        # print(res.head())

        for col in ['age_end', 'age_group_id', 'model_prediction']:
            if col in res.columns:
                res.drop(col, axis=1, inplace=True)

        # prep data to merge on by location id in hosp data
        res_mean = res.copy()
        res_mean['mean_' + draw_name] = res_mean[draw_cols].mean(axis=1)
        res_mean.drop(draw_cols, axis=1, inplace=True)

        # go from super region id to country id
        if draw_name == 'prevalence':
            mod_countries = pd.read_csv(r"filepath").cf_location_id.unique()
            res_mean.rename(columns={'cf_location_id': 'super_region_id'}, inplace=True)
            locs = get_location_metadata(location_set_id=35)
            locs = locs[['path_to_top_parent', 'super_region_id']].copy()
            locs = pd.concat([locs, locs.path_to_top_parent.str.split(",", expand=True)], axis=1)
            locs = locs[locs[3].notnull()]
            locs['cf_location_id'] = locs[3].astype(int)
            
            res_mean = res_mean.merge(locs[['cf_location_id', 'super_region_id']], how='right', on='super_region_id')
            res_mean = res_mean[res_mean.cf_location_id.isin(mod_countries)]
            res_mean.drop('super_region_id', axis=1, inplace=True)
            uniqs = res_mean.mean_prevalence.unique().size
            res_mean.drop_duplicates(inplace=True)
            assert res_mean.mean_prevalence.unique().size == uniqs
            print("ok")

        print("shape of {} is {}".format(draw_name, res_mean.shape))
        res_mean.to_csv("filepath".format(draw_name), index=False)
        
        print("creating the list to write data")
        write_list = []
        res = res.groupby(['age_start', 'sex_id'])
        for x, y in res:
            write_list.append(y)
        
        del res
        
        print("beginning to write data")
        partial_writer = functools.partial(pooled_writer, draw_name=draw_name)
        p = multiprocessing.Pool(10)
        p.map(partial_writer, write_list)

        test_write(draw_name)
        loop_time = ((time.time() - loop_start)/60)
        print("{} finished in {}".format(draw_name, loop_time))
        
        del write_list

    print("Everything is finished in {} minutes".format((time.time() - true_start)/60))
