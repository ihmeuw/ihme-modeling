"""
Set of functions to apply the envelope to inpatient hospital data
"""

import datetime
import platform
import re
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
import subprocess
import os
import glob
import time
import multiprocessing

# load our functions
user = getpass.getuser()
repo = 'FILEPATH'

sys.path.append(repo + r"Functions")
sys.path.append(repo + r"Envelope")
    
import apply_env_only as aeo
import hosp_prep
import gbd_hosp_prep

if platform.system() == "Linux":
    root = r"/FILEPATH/j"
else:
    root = "J:"

def delete_uncertainty_tmp():
    """
    Deletes the temp files used to make the uncertainty product
    """
    print("deleting existing mod_tmp_draw_product files...")
    # get list of anything in the dir
    to_delete = glob.glob("FILEPATH")

    # delete it
    for fpath in to_delete:
        os.remove(fpath)
    print("files deleted")
    return


def make_uncertainty_jobs(df, fix_failed = False):
    """
    send out jobs to run in parallel for the product of the envelope and
    the sampled correction factors
    """
    if fix_failed:
        print("qsubbing to fix the jobs that failed")
        for index, row in df.iterrows():
            age = df['age_start'][index]
            sex = df['sex_id'][index]
            year = df['year_start'][index]
            # qsub by each row of df rather than all unique combos
            qsub = "qsub -P proj_hospital -N unc_{}_{}_{} "\
                        r"-pe multi_slot 5 -l mem_free=10g "\
                        r"-o FILEPATH "\
                        r"-e FILEPATH "\
                        r"{}FILEPATH/python_shell_gbd.sh "\
                        r"{}FILEPATH/mod_worker_cf_uncertainty.py {} {} {}".\
                        format(int(age), int(sex), int(year),
                                repo, repo,
                                int(age), int(sex), int(year))
            subprocess.call(qsub, shell=True)
        print("qsub finished")
    else:
        # get age start
        if "age_start" not in df.columns:
            # switch back to age group id
            df = hosp_prep.group_id_start_end_switcher(df, remove_cols = False)

        ages = df.age_start.unique()
        sexes = df.sex_id.unique()
        years = df.year_start.unique()
        print("qsubbing modified CF * env jobs...")
        for age in ages:
            for sex in sexes:
                for year in years:
                    qsub = "qsub -P proj_hospital -N unc_{}_{}_{} "\
                        r"-pe multi_slot 5 -l mem_free=10g "\
                        r"-o FILEPATH "\
                        r"-e FILEPATH "\
                        r"{}FILEPATH/python_shell_gbd.sh "\
                        r"{}FILEPATH/mod_worker_cf_uncertainty.py {} {} {}".\
                        format(int(age), int(sex), int(year),
                            repo, repo,
                            int(age), int(sex), int(year))
                    subprocess.call(qsub, shell=True)
        print("qsub finished")
    return

def job_holder():
    """
    don't do anything while the uncertainty jobs are running, wait until
    they're finished to proceed with the script
    """
    status = "wait"
    while status == "wait":
        print(status)
        # this is experimental!!!
        p = subprocess.Popen("qstat", stdout=subprocess.PIPE)
        qstat_txt = p.communicate()[0]
        print("waiting...")
        pattern = 'unc_[0-9]+_[0-9]+_[0-9]+'
        #pattern = 'QLOGIN'
        found = re.search(pattern, qstat_txt)
        try:
            found.group(0)  # if the unc jobs are out this should work
            status = "wait"
            time.sleep(40)  # wait 40 seconds before checking again
        except:
            status = "go"
    print(status)
    return


def mc_file_reader(fpath):
    try:
        dat = pd.read_hdf(fpath, key="df", format="fixed")
    except:
        dat = pd.read_hdf(fpath, key="df", format="table")

    return dat


def read_tmp_jobs(cores):
    """
    reads all the hdf files sent out above back in and appends them
    """
    print("reading modified cf * env files back in...")
    start = time.time()

    parent_dir = r"FILEPATH/"

    print("begin reading in the temp files from {}".format(parent_dir))

    files = glob.glob(parent_dir + "*.H5")

    p = multiprocessing.Pool(cores)
    dat_list = list(p.map(mc_file_reader, files))
    env_cf = pd.concat(dat_list)

    # rename cols
    env_cf.rename(columns={'mean_modprevalence': 'mean_prevalence',
                           'lower_modprevalence': 'lower_prevalence',
                           'upper_modprevalence': 'upper_prevalence',
                           'mean_modindvcf': 'mean_indvcf',
                           'lower_modindvcf': 'lower_indvcf',
                           'upper_modindvcf': 'upper_indvcf',
                           'mean_modincidence': 'mean_incidence',
                           'lower_modincidence': 'lower_incidence',
                           'upper_modincidence': 'upper_incidence'},
                  inplace=True)

    # drop the age start col
    env_cf.drop('age_start', axis=1, inplace=True)

    print("read temp jobs finished in {} seconds".format((time.time()-start)))
    return env_cf


def env_merger(df, read_cores=10):
    """
    Merge the env*CF draws onto the hospital data

    Parameters:
        df: a Pandas dataframe of hospital data with bundle IDs
        attached
    """
    print("merging the envelope * mod CFs onto inpatient primary hospital data...")
    # read in the env data
    env_df = read_tmp_jobs(cores=read_cores)

    demography = ['location_id', 'year_start', 'year_end',
                  'age_group_id', 'sex_id', 'bundle_id']

    # MERGE ENVELOPE onto data
    pre_shape = df.shape[0]
    df = df.merge(env_df, how='left', on=demography)
    assert pre_shape == df.shape[0],\
        "The merge duplicated rows unexpectedly"
    print("finished merging")
    return df


def apply_env(df):
    """
    Multiply the CF*env values by the cause fractions in hosp data
    This needs to be at the bundle ID level

    Parameters:
        df: A Pandas dataframe with the env_cf already attached
    """

    # get the columns to mult
    cols_to_mult = df.filter(regex="^mean|^upper|^lower").columns

    for col in cols_to_mult:
        df[col] = df[col] * df['cause_fraction']
    return df


def reattach_covered_data(df, full_coverage_df):
    """
    Our sources have been split in two depending on whether or not they have covered,
    time to concat them back together
    """
    
    df = pd.concat([df, full_coverage_df]).reset_index(drop=True)
    return df


def merge_scalars(df, scalar_path, scalar, merge_on=[]):
    """
    A general(ish) function to apply scalars using a filepath and a scalar
    column of interest.
    
    Parameters:
        df: Pandas DataFrame
        scalar_path: str
            Direct me to the scalar file you'd like to merge on
        scalar: str
            Name of the column that contains the actual scalars
        merge_on: list
            list of columns to use to merge the scalars on

    Note: This will drop anything from the file that isn't a scalar or a merge
    column.
    """
    if merge_on == []:
        assert False, "Please provide a list of columns to merge on"
    # prep the scalars
    if scalar_path.split(".")[1] == "csv":
        scalars = pd.read_csv(scalar_path)
    if scalar_path.split(".")[1] == "H5":
        scalars = pd.read_hdf(scalar_path)
    # drop the columns which aren't used to merge or the scalars
    keep = merge_on + [scalar]
    scalars = scalars[keep]

    # merge on the scalars
    pre = df.shape
    df = df.merge(scalars, how='left', on=merge_on)
    assert pre[0] == df.shape[0], "Rows were added"
    assert pre[1] + 1 == df.shape[1], "Columns were added"

    return df


def drop_cols(df):
    """
    Drop columns we don't need anymore:
      cause fraction: finished using it, was used to make product
    """
    to_drop = ['cause_fraction']
    df.drop(to_drop, axis=1, inplace=True)
    return df

def fix_failed_jobs():
    # get a list of files the para jobs wrote
    output_files = glob.glob(r"FILEPATH/*.H5")
    # get a list of files that have no bytes
    zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    # keep trying new jobs until there are no filepaths with zero byte files
    while zeroes:
        # re-send those jobs
        rows = [os.path.basename(n).split(".")[0].split("_") for n in zeroes]
        dat = pd.DataFrame(rows)
        dat.columns = ['age_start', 'sex_id', 'year_start']
        make_uncertainty_jobs(dat, fix_failed = True)
        job_holder()
        zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    return

def apply_env_main(df, full_coverage_df,
                    env_path,
                    run_tmp_unc=True,
                    write=False,
                    read_cores=15):
    """
    do everything
    """
    back = df.copy()
    starting_bundles = df.bundle_id.unique()
    # delete existing env*CF files and re-run them again
    if run_tmp_unc:
        delete_uncertainty_tmp()
        make_uncertainty_jobs(df)
        job_holder()
        fix_failed_jobs()

    # hold the script up until all the uncertainty jobs are done
    job_holder()

    if run_tmp_unc:
        check_len = df.age_group_id.unique().size *\
                df.sex_id.unique().size *\
                df.year_start.unique().size
        actual_len = len(glob.glob(r"FILEPATH/*.H5"))
        assert actual_len == check_len,\
            "Check the error logs, it looks like something went wrong while re-writing files. We expected {} files and there were {} files".format(check_len, actual_len)

    df = env_merger(df, read_cores=read_cores)

    # apply the hospital envelope
    print("Applying the uncertainty to cause fractions")
    df = apply_env(df)

    df = reattach_covered_data(df, full_coverage_df)

    df = drop_cols(df)

    # reapply age/sex restrictions
    df = hosp_prep.apply_bundle_restrictions(df, 'mean')
    
    df['bundle_id'] = df['bundle_id'].astype(float)
    int_cols = ['sex_id', 'location_id', 'year_start', 'year_end']
    for col in int_cols:
        df[col] = df[col].astype(int)

    end_bundles = df.bundle_id.unique()

    diff = set(starting_bundles).symmetric_difference(set(end_bundles))

    if len(diff) > 0:
        print("The difference in bundles is {}. Reprocessing these without correction"\
            r" factor uncertainty.".format(diff))
        redo_df = back[back.bundle_id.isin(diff)]
        del back

        redo_df = aeo.apply_envelope_only(df=redo_df,
                     env_path=env_path,
                     return_only_inj=False,
                     apply_age_sex_restrictions=False, want_to_drop_data=False,
                     create_hosp_denom=False, apply_env_subroutine=True,
                     fix_norway_subnat=False)
        pre_cols = df.columns
        df = pd.concat([df, redo_df], ignore_index=True)
        # make sure columns keep the right dtype
        for col in ['sex_id', 'age_group_id', 'bundle_id', 'location_id',
                    'year_start', 'year_end', 'nid', 'representative_id']:
            df[col] = pd.to_numeric(df[col], errors='raise')
        # df.info()

        post_cols = df.columns
        print("{} change in columns".format(set(pre_cols).\
            symmetric_difference(post_cols)))

    if write:
        print("Writing the data to..")
        file_path = "FILEPATH/"\
                r"mod_apply_env.H5"
        hosp_prep.write_hosp_file(df, file_path, backup=True,
            include_version_info=True)

    return df
