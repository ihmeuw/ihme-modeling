"""
Set of functions to apply the envelope to inpatient hospital data
"""

import platform
import re
import sys
import getpass
import warnings
import pandas as pd
import subprocess
import os
import glob
import time
import multiprocessing

# load our functions
user = getpass.getuser()
repo = r"FILEPATH".format(user)

for d in ["PATHS"]:
    sys.path.append(repo + d)

import apply_env_only as aeo
import hosp_prep
import clinical_mapping
import clinical_funcs
import data_structure_utils as dsu
import source_selection_utils as ssu
from icg_prep_maternal_data import prep_maternal_main
from correction_factor_utils import apply_corrections

if platform.system() == "Linux":
    root = r"FILEPATH"
else:
    root = "FILEPATH


def clear_logs():
    root = "FILEPATH"
    errors = "FILEPATH"
    outputs = "FILEPATH"

    log_type = [errors, outputs]
    logs_all = []
    for e in log_type:
        var = 'e'
        if e.endswith('output/'):
            var = 'o'
        file_a = glob.glob(e + '*.{}*'.format(var))
        file_b = glob.glob(e + '*.p{}*'.format(var))

        logs_all = file_a + file_b + logs_all

    for f in logs_all:
        os.remove(f)
    return

def delete_uncertainty_tmp(run_id):
    """
    Deletes the temp files used to make the uncertainty product
    """
    print("deleting existing mod_tmp_draw_product files...")

    to_delete = glob.glob(r"FILEPATH")

    # delete it
    if to_delete:
        for fpath in to_delete:
            os.remove(fpath)
        print("files deleted")
    return

def create_mem_est(df):
    """
    Jobs are being sent out with uniform mem reqs but data is highly variable
    First pass is super rough, split the jobs into 3 groups
    """
    memdf = df.groupby(['age_start', 'sex_id', 'year_id']).size().reset_index()
    memdf['mem'] = 8
    memdf.loc[memdf[0] > 5000, 'mem'] = 16
    memdf.loc[memdf[0] > 17000, 'mem'] = 24

    return memdf

def make_uncertainty_jobs(df, run_id, bundle_level_cfs, fix_failed=False):
    """
    send out jobs to run in parallel for the product of the envelope and
    the modeled correction factors
    """
    # get age start
    if "age_start" not in df.columns:
        # switch back to age group id
        df = hosp_prep.group_id_start_end_switcher(df, remove_cols = False)

    memdf = create_mem_est(df.copy())
    print("memory values:")
    print(memdf['mem'].value_counts().to_string())

    if fix_failed:
        print("qsubbing to fix the jobs that failed")
        for index, row in df.iterrows():
            age = df['age_start'][index]
            sex = df['sex_id'][index]
            year = df['year_id'][index]
            mem = memdf.query("age_start == @age & sex_id == @sex & year_id == @year").mem.unique()[0]
            # qsub by each row of df rather than all unique combos
            qsub = "qsub "
            subprocess.call(qsub, shell=True)
        print("qsub finished")
    else:
        ages = df.age_start.unique()
        sexes = df.sex_id.unique()
        years = df.year_id.unique()
        print("qsubbing modeled CF * env jobs...")
        for age in ages:
            for sex in sexes:
                for year in years:
                    mem = memdf.query("age_start == @age & sex_id == @sex & year_id == @year").mem.unique()[0]
                    qsub = "qsub"
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
        qstat_txt = qstat_txt.decode('utf-8')
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
    """Helper function to be used in multiprocessing"""
    dat = pd.read_hdf(fpath, key="df")
    dat = dsu.year_id_switcher(dat)
    return dat

def read_tmp_jobs(cores, run_id):
    """
    reads all the hdf files sent out above back in and appends them
    """
    print("reading modeled cf * env files back in...")
    start = time.time()

    parent_dir = r"FILEPATH")

    print("begin reading in the temp files from {}".format(parent_dir))

    files = glob.glob(parent_dir + "*.H5")

    p = multiprocessing.Pool(cores)
    dat_list = list(p.map(mc_file_reader, files))
    env_cf = pd.concat(dat_list)

    # rename cols
    cols = env_cf.columns.tolist()
    rename_dict = {'mean_modprevalence': 'mean_prevalence',
                           'lower_modprevalence': 'lower_prevalence',
                           'upper_modprevalence': 'upper_prevalence',
                           'mean_modindvcf': 'mean_indvcf',
                           'lower_modindvcf': 'lower_indvcf',
                           'upper_modindvcf': 'upper_indvcf',
                           'mean_modincidence': 'mean_incidence',
                           'lower_modincidence': 'lower_incidence',
                           'upper_modincidence': 'upper_incidence'}

    for key, value in rename_dict.items():
        if key in cols:
            env_cf.rename(columns={key: value}, inplace=True)

    # drop the age start col
    env_cf.drop('age_start', axis=1, inplace=True)

    print("read temp jobs finished in {} seconds".format((time.time()-start)))
    return env_cf

def env_merger(df, run_id, read_cores):
    """
    Merge the env*CF draws onto the hospital data

    Parameters:
        df: a Pandas dataframe of hospital data with icg IDs
        attached
    """
    print("merging the envelope * mod CFs onto inpatient primary hospital data...")
    # read in the env data
    env_df = read_tmp_jobs(cores=read_cores, run_id=run_id)

    demography = ['location_id', 'year_id',
                  'age_group_id', 'sex_id', 'icg_id', 'icg_name']

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
    This needs to be at the icg ID level

    Parameters:
        df: A Pandas dataframe with the env_cf already attached
    """

    # get the columns to mult
    cols_to_mult = df.filter(regex="^mean|^upper|^lower").columns

    for col in cols_to_mult:
        # overwrite the existing value to compute icg hospitalization rate
        # aka "apply the envelope"
        df[col] = df[col] * df['cause_fraction']
    return df

def reattach_covered_data(df, full_coverage_df):
    """
    Our sources have been split in two depending on whether or not they
    have full coverage. time to concat them back together
    """
    # NOTE now there's going be a column "sample_size" and "cases" that
    # is null for every source except the fully covered ones
    df = pd.concat([df, full_coverage_df], sort=False).reset_index(drop=True)
    return df

def merge_scalars(df, scalar_path, scalar, merge_on=[]):
    """
    A general(ish) function to apply scalars using a filepath and a scalar
    column of interest.
    Not in use as of 11/7/2017 and may not be for the near future. We're applying
    our scalars at the end in 5 year bands

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
      mean: finished using it, was used to make product
      upper: finished using, it was used ot make product_upper
      lower: finished using, it was used ot make product_lower
      val, numerator, denomantor: no longer in count space
    """
    to_drop = ['cause_fraction', 'val', 'numerator', 'denominator']
    df.drop(to_drop, axis=1, inplace=True)
    return df

def fix_failed_jobs(df, run_id, bundle_level_cfs):
    """
    Re-send jobs here based on two failure types. First if the job died for some
    random reason? re-send once if exp_files aren't there

    Second is a little tricker, resend if a job writes an empty dataframe to PATH
    This is related to hdf files having some disk locking issue
    """
    # get age start
    if "age_start" not in df.columns:
        # switch back to age group id
        df = hosp_prep.group_id_start_end_switcher(df, remove_cols = False)

    exp_files = []
    for age in df.age_start.unique():
        for sex in df.sex_id.unique():
            for year in df.year_id.unique():
                exp_files.append("{}_{}_{}.H5".format(int(age), int(sex), int(year)))
    # get a list of files the para jobs wrote
    output_files = glob.glob(r"FILEPATH")
    diff_files = set(exp_files).symmetric_difference(set([os.path.basename(f) for f in output_files]))
    for f in diff_files:
        print(f)
        f = f.split(".")[0]
        f = f.split("_")
        rdf = pd.DataFrame({'age_start': f[0],
                            'sex_id': f[1],
                            'year_id': f[2]},
                            index=[0])
        make_uncertainty_jobs(rdf, run_id, bundle_level_cfs, fix_failed=True)
        del rdf

    # get a list of files that have no bytes
    zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    # keep trying new jobs until there are no filepaths with zero byte files
    while zeroes:
        # re-send those jobs
        rows = [os.path.basename(n).split(".")[0].split("_") for n in zeroes]
        dat = pd.DataFrame(rows)
        dat.columns = ['age_start', 'sex_id', 'year_id']
        make_uncertainty_jobs(dat, run_id, bundle_level_cfs, fix_failed=True)
        job_holder()
        zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    return

def apply_env_main(df, full_coverage_df,
                   run_id,
                   env_path,
                   gbd_round_id,
                   decomp_step,
                   bundle_level_cfs,
                   run_tmp_unc=True,
                   write=False,
                   read_cores=2):
    """
    do everything
    """
    warnings.warn("This script uses multiprocessing with {} pools. If ran on fair cluster then it needs to be ran with at least {} threads.".format(read_cores, read_cores))
    df = dsu.year_id_switcher(df)
    full_coverage_df = dsu.year_id_switcher(full_coverage_df)
    back = df.copy()
    starting_icgs = df.icg_id.unique()
    # delete existing env*CF files and re-run them again
    if run_tmp_unc:
        clear_logs()
        delete_uncertainty_tmp(run_id=run_id)
        make_uncertainty_jobs(df, run_id=run_id, bundle_level_cfs=bundle_level_cfs)
        job_holder()
        fix_failed_jobs(df, run_id=run_id, bundle_level_cfs=bundle_level_cfs)

    # hold the script up until all the uncertainty jobs are done
    job_holder()

    # really rough check to see if the length of new files is what we'd
    # expect
    if run_tmp_unc:
        check_len = df.age_group_id.unique().size *\
                df.sex_id.unique().size *\
                df.year_id.unique().size

        actual_len = len(glob.glob(r"FILEPATH")
        assert actual_len == check_len,\
            "Check the error logs, it looks like something went wrong while re-writing files. We expected {} files and there were {} files".format(check_len, actual_len)

    df = env_merger(df, run_id=run_id, read_cores=read_cores)

    # apply the hospital envelope
    print("Applying the uncertainty to cause fractions")
    df = apply_env(df)

    df = reattach_covered_data(df, full_coverage_df)

    df = drop_cols(df)

    # reapply age/sex restrictions
    # df = hosp_prep.apply_bundle_restrictions(df, 'mean')
    if 'age_group_id' in df.columns:
        age_set = 'age_group_id'
    elif 'age_start' in df.columns:
        age_set = 'binned'
    df = clinical_mapping.apply_restrictions(df, age_set=age_set, cause_type='icg', prod=False)

    # kind of disturbed that our functions constantly alter the dtype of icg_id
    # it's caused by some merges, quick fix for 11/17 just cast to what we expect
    df['icg_id'] = df['icg_id'].astype(float)
    int_cols = ['sex_id', 'location_id', 'year_id']
    for col in int_cols:
        df[col] = df[col].astype(int)

    end_icgs = df.icg_id.unique()

    diff = set(starting_icgs).symmetric_difference(set(end_icgs))

    if len(diff) > 0:
        print("The difference in icgs is {}. Reprocessing these without correction"\
            r" factor uncertainty.".format(diff))
        redo_df = back[back.icg_id.isin(diff)]
        del back

        redo_df = aeo.apply_envelope_only(df=redo_df,
                                          env_path=env_path,
                                          run_id=run_id,
                                          gbd_round_id=gbd_round_id,
                                          decomp_step=decomp_step,
                                          return_only_inj=False,
                                          apply_age_sex_restrictions=False, want_to_drop_data=False,
                                          create_hosp_denom=False, apply_env_subroutine=True,
                                          fix_norway_subnat=False)
        pre_cols = df.columns
        df = pd.concat([df, redo_df], ignore_index=True)
        # make sure columns keep the right dtype
        for col in ['sex_id', 'age_group_id', 'icg_id', 'location_id',
                    'year_id', 'nid', 'representative_id']:
            df[col] = pd.to_numeric(df[col], errors='raise')
        # df.info()

        post_cols = df.columns
        print("{} change in columns".format(set(pre_cols).\
            symmetric_difference(post_cols)))
    else:
        print("There is no difference in unique ICGs after applying the envelope and CFs: {}".format(diff))

    if write:
        file_path = "FILEPATH")
        print("Writing the applied envelope data to {}".format(file_path))
        hosp_prep.write_hosp_file(df, file_path, backup=False)

    return df

def dev_clean_cols(df):
    """
    This might be removing columns a little too aggressively and we may want to
    stop using it for a true production run
    """
    cols = df.columns

    to_drop = ['val', 'numerator', 'denominator', 'ifd_mean', 'asfr_mean', 'year_end']
    for col in ['age_group_unit', 'diagnosis_id', 'outcome_id', 'lower_raw', 'upper_raw',
                'lower_indvcf', 'upper_indvcf', 'lower_incidence', 'upper_incidence',
                'lower_prevalence', 'upper_prevalence']:
        if col in cols:
            if df[col].unique().size == 1:
                to_drop += [col]

    for col in to_drop:
        if col in cols:
            df.drop(col, axis=1, inplace=True)
    return df

def reshape_sample_size_unc(df, maternal=False):
    """
    reshape the utla data and the maternal denom data, both only have
    mean estimates and use sample size to calculate uncertainty
    """
    df = dev_clean_cols(df)
    cols = df.columns.tolist()

    idx = df.filter(regex="^(?!mean).*").columns.tolist()

    if maternal:
        rename_dict = {'mean_raw': 'inp-primary-live_births_unadj',
                           'mean_indvcf': 'inp-primary-live_births_cf1_modeled',
                           'mean_incidence': 'inp-primary-live_births_cf2_modeled',
                           'mean_prevalence': 'inp-primary-live_births_cf3_modeled'}
    else:
        rename_dict = {'mean_raw': 'inp-primary-unadj',
                       'mean_indvcf': 'inp-primary-cf1_modeled',
                       'mean_incidence': 'inp-primary-cf2_modeled',
                       'mean_prevalence': 'inp-primary-cf3_modeled'}
    for key, value in rename_dict.items():
        if key in cols:
            df.rename(columns={key: value}, inplace=True)

    df = df.set_index(idx).stack().reset_index()

    df.rename(columns={'level_{}'.format(len(idx)): 'estimate_type', 0: 'mean'}, inplace=True)

    # use the estimate table to replace estimate type(name) with estimate_id
    q = QUERY
    est = pd.read_sql(q, clinical_funcs.get_engine())
    name_dict = dict(list(zip(est['estimate_name'], est['estimate_id'])))
    df['estimate_id'] = -1
    for etype in df['estimate_type'].unique():
        df.loc[df['estimate_type'] == etype, 'estimate_id'] = name_dict[etype]
    assert (df['estimate_id'] > 0).all(), "There are still dummy estimate IDs"

    return df

def reshape_env_unc(df, drop_nulls=False):
    """
    reshape the data that gets uncertainty from envelope.
    This is honestly kind of tricky I don't know the best way to do it...
    make n copies where n is the number of estimate types present
    rename the cols and assign estimate type
    concat back together
    """
    df = dev_clean_cols(df)

    est_cols = df.filter(regex="^mean|^upper|^lower").columns.tolist()
    non_est_cols = [n for n in df.columns if n not in est_cols]
    assert (set(df.columns) - set(non_est_cols)) == set(est_cols),\
        "Something went wrong when getting the estimate and non-estimate col names"
    est_types = {'incidence': 'inp-primary-cf2_modeled',
                 'indvcf': 'inp-primary-cf1_modeled',
                 'prevalence': 'inp-primary-cf3_modeled',
                 'raw': 'inp-primary-unadj'}
    long_list = []
    for t in list(est_types.keys()):
        sub_cols = [n for n in est_cols if t in n]
        # when the CFs don't exist this will be an empty list, so skip it
        if not sub_cols:
            continue

        tmp = df[non_est_cols + sub_cols].copy()
        # rename cols to just mean/upper/lower
        tmp.rename(columns={'mean_{}'.format(t): 'mean',
                            'upper_{}'.format(t): 'upper',
                            'lower_{}'.format(t): 'lower'}, inplace=True)
        if drop_nulls:
            # like 40% of rows have no estimates, we'll keep them for now but might want
            # to drop them in the future
            tmp = tmp[tmp['mean'].notnull()| tmp['lower'].notnull() | tmp['upper'].notnull()]

        # add on the estimate type
        tmp['estimate_type'] = est_types[t]
        long_list.append(tmp)

    df = pd.concat(long_list, ignore_index=True, sort=False)

    # use the estimate table to replace estimate type(name) with estimate_id
    q = QUERY
    est = pd.read_sql(q, clinical_funcs.get_engine())
    name_dict = dict(list(zip(est['estimate_name'], est['estimate_id'])))
    df['estimate_id'] = -1
    for etype in df['estimate_type'].unique():
        df.loc[df['estimate_type'] == etype, 'estimate_id'] = name_dict[etype]
    assert (df['estimate_id'] > 0).all(), 'There are still dummy estimate IDs'
    assert df['estimate_id'].isnull().sum() == 0, 'There are missing estimate ids'

    return df

def reshape_all_types(df_env, full_coverage_df, mat_df):
    """
    Reshape 3 separate processes from wide estimates to long estimates and concat them
    together. Note: The estimate_id column is used to identify each process.
    """

    # reshape maternal and UTLA data
    full_coverage_df = reshape_sample_size_unc(full_coverage_df, maternal=False)
    mat_df = reshape_sample_size_unc(mat_df, maternal=True)

    # more complicated reshape of the other hospital sources
    df_env = reshape_env_unc(df_env, drop_nulls=True)

    # merge everything back together and return it
    if len(full_coverage_df) == 0:
        warnings.warn("There is no full coverage data, is this expected??")
        df = pd.concat([df_env, mat_df], ignore_index=True, sort=False)
    else:
        df = pd.concat([df_env, full_coverage_df, mat_df], ignore_index=True, sort=False)
    return df

def pooled_writer(df, run_id):
    assert df.age_group_id.unique().size == 1
    assert df.sex_id.unique().size == 1
    age = df.age_group_id.iloc[0]
    sex = df.sex_id.iloc[0]

    write_path = "FILEPATH".\
        format(rid=run_id, age=age, sex=sex)
    df.to_csv(write_path, index=False, na_rep='NULL')
    return "age group id {} and sex id {} for run id {} has finished".format(age, sex, run_id)

def split_sources(df_env):
    # split out sources
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]
    # make dataframe with sources
    full_coverage_df = df_env[df_env.source.isin(full_coverage_sources)].copy()
    # drop these sources from dataframe
    df_env = df_env[~df_env.source.isin(full_coverage_sources)].copy()

    # slight rename
    for col in ['mean', 'upper', 'lower']:
        df_env.rename(columns={col: col + "_raw"}, inplace=True)
        full_coverage_df.rename(columns={col: col + "_raw"}, inplace=True)

    return df_env, full_coverage_df

def confirm_icg_cfs_exist(run_id, cf_model_type, bundle_level_cfs):
    if bundle_level_cfs:
        pass
    else:
        if cf_model_type == 'rmodels':
            cf_files = glob.glob("FILEPATH")
        elif cf_model_type == 'mr-brt':
            cf_files = glob.glob("FILEPATH")
        assert cf_files,\
            "The correction factor mean files from the draws are not present in run_id {}".format(run_id)

    return

def create_icg_single_year_est(df, full_coverage_df, no_draws_env_path, run_id, gbd_round_id, decomp_step,
                               run_tmp_unc, write, cf_model_type, bundle_level_cfs):
    """
    This runs the pipeline from prep_env to the produce single year ICG level
    estimates of inpatient clinical data
    """
    confirm_icg_cfs_exist(run_id, cf_model_type, bundle_level_cfs)

    # apply the envelope
    df_env = apply_env_main(df, full_coverage_df, env_path=no_draws_env_path,
                            run_id=run_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                            run_tmp_unc=run_tmp_unc, write=write,
                            bundle_level_cfs=bundle_level_cfs)

    # drop sources we don't want
    # this uses a flat file and should be updated to the db
    # df_env = ssu.retain_active_inp_sources(df_env, verbose=True)

    # create adjusted maternal data and apply corrections
    mat_df = prep_maternal_main(df=df_env.copy(), run_id=run_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                                cf_model_type=cf_model_type, write_denom=True, write=True, denom_type='ifd_asfr',
                                bundle_level_cfs=bundle_level_cfs)

    df_env, full_coverage_df = split_sources(df_env)

    if bundle_level_cfs:
        pass
    else:
        # apply full coverage CFs (the CFs don't have uncertainty)
        full_coverage_df = apply_corrections(full_coverage_df, run_id, cf_model_type=cf_model_type)

    # bring data together and reshape data long by estimate type
    final_df = reshape_all_types(df_env=df_env, full_coverage_df=full_coverage_df, mat_df=mat_df)

    return final_df
