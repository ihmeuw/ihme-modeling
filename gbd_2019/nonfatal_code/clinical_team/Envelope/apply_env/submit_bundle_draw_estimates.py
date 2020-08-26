"""
This is the code that will send out each worker job to create draws from ICG estimates,
aggregate to the bundle level, aggregate to 5 year groups, apply the CFs and then
compile all the results back together to write the final file to drive in the inpatient Class

"""
import os
import glob
import warnings
import time
import subprocess
import getpass
import numpy as np
import pandas as pd

from clinical_info.Functions import hosp_prep

USER = getpass.getuser()
REPO = FILEPATH.format(USER)

def get_demos(all_years):
    """
    Returns 3 lists which we'll use to send jobs in parallel and check our outputs
    """
    age_groups = hosp_prep.get_hospital_age_groups().age_group_id.unique().tolist()
    sexes = [1, 2]
    if all_years:
        years = np.arange(1988, 2017, 5)
    else:
        years = np.arange(2003, 2017, 5)
        warnings.warn("This is running on a subset of years, specifically {}".format(years))
    warnings.warn("update this function if 2018 or later data is added")

    return age_groups, sexes, years

def exp_file_getter(age_groups, sexes, years):
    """
    age_groups, sexes, years: (list) use these to generate a list with all possible combinations of
                                     the three. This is the set of jobs we'll send out and the set
                                     we (exp)ect to get back
    """
    # list comprehension or something to put it all together
    exp_files = ["{}_{}_{}".format(age, sex, year) for age in age_groups for sex in sexes for year in years]

    return exp_files

def ob_file_getter(run_id, for_reading=False):
    """
    glob to get a list of filenames that are written to drive after job_holder says it's ok to stop waiting
    """
    base = FILEPATH.format(run_id)
    ob_prep_files = glob.glob(base + "FILEPATH")
    ob_prep_files = [os.path.basename(f) for f in ob_prep_files]

    ob_final_files = glob.glob(base + "FILEPATH")
    if for_reading:
        return ob_final_files

    ob_final_files = [os.path.basename(f) for f in ob_final_files]

    return ob_prep_files, ob_final_files

def check_files(run_id, age_groups, sexes, years):
    """
    Compare the set of expected files against the set of observed files. If there are any differences
    the script will break
    """
    exp_prep_files = ["{}.H5".format(f) for f in exp_file_getter(age_groups, sexes, years)]
    exp_final_files = ["{}.csv".format(f) for f in exp_file_getter(age_groups, sexes, years)]

    ob_prep_files, ob_final_files = ob_file_getter(run_id)

    prep_diffs = set(exp_prep_files).symmetric_difference(set(ob_prep_files))
    final_diffs = set(exp_final_files).symmetric_difference(set(ob_final_files))

    msg = ""
    if prep_diffs:
        msg += "prep files {} don't match".format(prep_diffs)
    elif final_diffs:
        msg += " final files {} don't match".format(final_diffs)
    if msg:
        assert False, msg
    print("All files are present")
    return

def qsubber(age_group, sex, year, repo, run_id, draws, gbd_round_id, decomp_step):
    """
    Won't be able to carry this over to the new cluster but qsub out each job with
    trailing arguments
    """
    qsub = QSUB.format(
                r=repo, a=age_group, s=sex, y=year, stp=decomp_step, gbd=gbd_round_id, run=run_id, d=draws)

    os.popen(qsub)

def main_bundle_draw_submit(run_id, draws, gbd_round_id, decomp_step):
    """
    submit all the bundle draw estimate files by age/sex/5 year start value
    """
    # get the age groups sexes and years we'll use to send out jobs
    age_groups, sexes, years = get_demos(all_years=True)
    # send out all the jobs
    [qsubber(age_group=age_group, sex=sex, year=year, repo=REPO,
             run_id=run_id, draws=draws, gbd_round_id=gbd_round_id,
             decomp_step=decomp_step) for age_group in age_groups for sex in sexes for year in years]
    # wait until they've finished
    hosp_prep.job_holder(job_name='envunc', sleep_time=60)
    # check the files that were written against what we expected to write
    check_files(run_id, age_groups, sexes, years)

    # get the final files from the para jobs
    print("concatting all the final files back together")
    final_files = ob_file_getter(run_id, for_reading=True)
    df = pd.concat([pd.read_csv(f) for f in final_files], sort=False, ignore_index=True)
    return df
