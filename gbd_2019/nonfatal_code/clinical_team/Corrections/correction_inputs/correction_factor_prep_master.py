"""
create the non-marketscan correction factor input data by running
the main function of this script:
create_non_ms_cf_inputs(run_id, user)

"""
import subprocess
import glob
import shutil
import os
import time
import getpass
import warnings

from clinical_info.Functions import hosp_prep

def file_mover(run_id, cause_type):
    """
    If the file checker fails we'll move every output over to an archive folder
    This could definitely be improved to re-run failed jobs but the output
    format makes it more difficult to identify which jobs to re-run. Basically a lot of
    HCUP jobs go out and fail b/c they don't have patient IDs
    """
    src = FILEPATH
    dst = '{}/_archive'.format(src)

    files = glob.glob(src + "/*.csv")
    dst = [dst + "/{}".format(os.path.basename(f)) for f in files]
    move_dict = dict(list(zip(files, dst)))

    for key in list(move_dict.keys()):
        shutil.move(key, move_dict[key])

    return

def exp_getter(run_id, cause_type):
    """
    for nzl and phl files are created by year, for usa they're created by year
    and subnational location id. Unfortunately some type of hard coding will
    be used here
    """
    base = FILEPATH
    # one file is written for each year
    phl_files = [base + "{c}_PHL_HICC_{yr}.csv".format(yr=y, c=cause_type) for y in range(2013, 2015, 1)]
    nzl_files = [base + "{c}_NZL_NMDS_{yr}.csv".format(yr=y, c=cause_type) for y in range(2000, 2016, 1)]

    # recursively glob through the input files
    usa_files = glob.glob(FILEPATH_USA)
    usa_files = [base + os.path.basename(f) for f in usa_files]
    usa_files = [f.replace('formatted', cause_type) for f in usa_files]
    usa_files = [f.replace('dta', 'csv') for f in usa_files]

    exp_files = phl_files + nzl_files + usa_files
    return exp_files

def file_checker(exp_files, run_id, cause_type):
    """
    Our outputs are source dependent rather than highly structured
    this function takes a list of expected files and checks what was actually
    written after job_holder is done, if there's a difference it moves all
    the finished files to an archive folder and fails
    """
    obs_files = glob.glob(FLAIMS_FILEPATH)

    diff = set(exp_files).symmetric_difference(set(obs_files))
    msg = """There should be no difference between observed and \
          expected files, but we're seeing {l} different files {d}, Moving all the outputs to _archive
          """.format(l=len(diff), d=diff)
    if diff:
        file_mover(run_id, cause_type)
        assert False, msg
    return

def create_non_ms_cf_inputs(run_id, user):
    """
    qsub the 3 non-MS correction factor scripts to create tabulated inputs for the
    Cf models
    """

    qsub = qsup_PHL

    qsub = qsub_NZL

    qsub = qsub_HCUP

    hosp_prep.job_holder(job_name="cfjob_", sleep_time=200, init_sleep=1200)
    for cause_type in ['icg', 'bundle']:
        exp_files = exp_getter(run_id, cause_type=cause_type)
        file_checker(exp_files=exp_files, run_id=run_id, cause_type=cause_type)

    return

if __name__ == '__main__':
    run_id = sys.argv[1]
    user = getpass.getuser()
    create_non_ms_cf_inputs(run_id, user)
