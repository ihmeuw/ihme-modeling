"""
Submit script to send out jobs which create population level
estimates from inpatient hospital data

Overall structure is now-
1) As before, prep_for_env writes 2 files to drive, one
   file to use with env and one to use with GBD Pop
2) This script sends out N jobs, one for each age/sex
   in our data
3) Each worker_pop_ests job converts from hospital-level
   admissions to population level rates and writes two
   files to drive
3) This script confirms that each job wrote two files
4) If anything is missing or has zero bytes it will
   resend those jobs
5) Aggregates the data back together but retains only
   'estimate_id', 'icg_id', 'icg_name', 'location_id',
   'source'] because these are all that's needed to
   create helper files for squaring the data
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
USER = getpass.getuser()
REPO = r"FILEPATH".format(USER)

from clinical_info.Envelope import apply_env_only as aeo
from clinical_info.Functions import (
    hosp_prep,
    data_structure_utils as dsu,
    source_selection_utils as ssu,
)
from clinical_info.Functions.Clinical_functions import clinical_funcs
from clinical_info.Functions import hosp_prep


def clear_logs():
    root = "FILEPATH"
    errors = "FILEPATH".format(root)
    outputs = "FILEPATH".format(root)

    log_type = [errors, outputs]
    logs_all = []
    for e in log_type:
        var = "e"
        if e.endswith("output/"):
            var = "o"
        file_a = glob.glob(e + "*.{}*".format(var))
        file_b = glob.glob(e + "*.p{}*".format(var))

        logs_all = file_a + file_b + logs_all

    for f in logs_all:
        os.remove(f)
    return


def delete_pop_estimate_files(run_id):
    """
    Deletes the output files used to make the uncertainty product
    """

    # get list of anything in the dir
    to_delete = glob.glob(f"FILEPATH")

    # delete it
    if to_delete:
        print(f"Deleting {len(to_delete)} existing pop estimate split files...")
        for fpath in to_delete:
            os.remove(fpath)
        print("Files deleted")
    return


def create_mem_est(df):
    """
    Jobs are being sent out with uniform mem reqs but data is highly variable
    First pass is super rough, split the jobs into 3 groups
    """
    memdf = df.groupby(["age_group_id", "sex_id"]).size().reset_index()
    memdf["mem"] = 55
    memdf.loc[memdf[0] > 750000, "mem"] = 65
    memdf.loc[memdf[0] > 1000000, "mem"] = 70

    return memdf


def get_run_que(run_id):
    p = pd.read_pickle(f"FILEPATH")
    return p.que


def make_uncertainty_jobs(
    df, run_id, gbd_round_id, decomp_step, env_path, fix_failed=False
):
    """
    send out jobs to run in parallel for the product of the envelope and
    the modeled correction factors
    """

    memdf = create_mem_est(df[["age_group_id", "sex_id"]].copy())
    print("memory values:")
    print(memdf["mem"].value_counts().to_string())

    # get the que from apply env pickle
    apply_env_que = get_run_que(run_id)

    if fix_failed:
        print("qsubbing to fix the jobs that failed")
        for index, row in df.iterrows():
            age = df["age_group_id"][index]
            sex = df["sex_id"][index]
            # year = df['year_id'][index]
            mem = memdf.query("age_group_id == @age & sex_id == @sex").mem.unique()[0]
            # qsub by each row of df rather than all unique combos
            qsub = "QSUB".format(
                a=int(age),
                s=int(sex),
                threads=4,
                memory=int(mem),
                REPO=REPO,
                r=run_id,
                gbd=gbd_round_id,
                decomp=decomp_step,
                ep=env_path,
                que=apply_env_que,
            )

            subprocess.call(qsub, shell=True)
        print("qsub finished")
    else:
        ages = df.age_group_id.unique()
        sexes = df.sex_id.unique()
        print("qsubbing PopEstimates jobs...")
        for age in ages:
            for sex in sexes:
                mem = memdf.query("age_group_id == @age & sex_id == @sex").mem.unique()[
                    0
                ]
                qsub = "QSUB".format(
                    a=int(age),
                    s=int(sex),
                    threads=4,
                    memory=int(mem),
                    REPO=REPO,
                    r=run_id,
                    gbd=gbd_round_id,
                    decomp=decomp_step,
                    ep=env_path,
                    que=apply_env_que,
                )
                subprocess.call(qsub, shell=True)

        print("qsub finished")
    return


def mc_file_reader(fpath):
    """Helper function to be used in multiprocessing"""

    cols = [
        "estimate_id",
        "icg_id",
        "icg_name",
        "location_id",
        "source",
        "nid",
        "year_id",
    ]
    dat = pd.read_hdf(fpath, key="df", columns=cols).drop_duplicates()
    # dat = dsu.year_id_switcher(dat)
    return dat


def read_split_icg_jobs(cores, run_id):
    """
    reads all the hdf files sent out above back in and appends them
    """

    print("reading select cols from pop level estimate files in...")
    start = time.time()

    parent_dir = "FILEPATH"

    files = glob.glob(parent_dir + "*.H5")
    len_files = len(files)
    print(f"begin reading in the {len_files} temp files from {parent_dir}")

    p = multiprocessing.Pool(cores)
    dat_list = list(p.map(mc_file_reader, files))
    df = pd.concat(dat_list, sort=False, ignore_index=True)
    df.drop_duplicates(inplace=True)

    print("read popEst jobs finished in {} seconds".format((time.time() - start)))
    return df


def fix_failed_jobs(df, run_id, gbd_round_id, decomp_step, env_path):
    """
    Re-send jobs here based on two failure types. First if the job died for some
    random reason? re-send once if exp_files aren't there
    """

    exp_files = []
    for age in df.age_group_id.unique():
        for sex in df.sex_id.unique():
            for t in ["df_env", "df_fc"]:
                exp_files.append("{}_{}_{}.H5".format(int(age), int(sex), t))
    # get a list of files the para jobs wrote
    output_files = glob.glob("FILEPATH".format(run_id))
    diff_files = set(exp_files) - set([os.path.basename(f) for f in output_files])
    for f in diff_files:
        print(f)
        f = f.split(".")[0]
        f = f.split("_")
        rdf = pd.DataFrame({"age_group_id": f[0], "sex_id": f[1]}, index=[0])
        make_uncertainty_jobs(
            rdf,
            run_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            env_path=env_path,
            fix_failed=True,
        )
        del rdf

    # get a list of files that have no bytes
    zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    # keep trying new jobs until there are no filepaths with zero byte files
    while zeroes:
        # re-send those jobs
        rows = [os.path.basename(n).split(".")[0].split("_") for n in zeroes]
        dat = pd.DataFrame(rows)
        dat.columns = ["age_group_id", "sex_id", "t1", "t2"]
        make_uncertainty_jobs(
            dat,
            run_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            env_path=env_path,
            fix_failed=True,
        )
        hosp_prep.job_holder(job_name="unc_", sleep_time=40)
        zeroes = [z for z in output_files if os.path.getsize(z) < 1]
    return


def create_pop_est_jobs(
    df,
    run_id,
    gbd_round_id,
    decomp_step,
    env_path,
    run_tmp_unc=True,
    write=False,
    read_cores=2,
):
    """
    Send out the 42 jobs by age and sex to generate population level estimates at
    the ICG level from inpatient data
    """

    warnings.warn(
        "This script uses multiprocessing with {} pools. If ran on fair cluster then it needs to be ran with at least {} threads.".format(
            read_cores, read_cores
        )
    )
    df = dsu.year_id_switcher(df)

    starting_icgs = df.icg_id.unique()

    # delete existing env * Admit fraction and full cover jobs and run again
    if run_tmp_unc:
        clear_logs()
        delete_pop_estimate_files(run_id=run_id)
        make_uncertainty_jobs(
            df,
            run_id=run_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            env_path=env_path,
        )
        hosp_prep.job_holder(job_name="unc_", sleep_time=40, init_sleep=120)
        fix_failed_jobs(
            df,
            run_id=run_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            env_path=env_path,
        )

    # hold the script up until all the uncertainty jobs are done
    hosp_prep.job_holder(job_name="unc_", sleep_time=40)

    # rough check to see if the length of new files is what we'd expect
    if run_tmp_unc:
        check_len = (
            df.age_group_id.unique().size * df.sex_id.unique().size * 2
        )  # 2 files for each job
        actual_len = len(glob.glob("FILEPATH".format(run_id)))
        assert actual_len == check_len, (
            "Check the error logs, it looks like something went wrong while "
            "re-writing files. We expected {} files and there were {} files".format(
                check_len, actual_len
            )
        )

    df = read_split_icg_jobs(cores=read_cores, run_id=run_id)

    end_icgs = df.icg_id.unique()

    diff = set(starting_icgs).symmetric_difference(set(end_icgs))

    if len(diff) > 0:
        print(
            "The difference in icgs is {}. Reprocessing these without correction"
            r" factor uncertainty.".format(diff)
        )
        assert False, "We're not gonna re-do it but this shouldn't trip!"

    if write:
        file_path = "FILEPATH".format(run_id)
        print("Writing the applied envelope data to {}".format(file_path))
        exp_cols = [
            "estimate_id",
            "icg_id",
            "icg_name",
            "location_id",
            "source",
            "year_id",
        ]
        hosp_prep.write_hosp_file(
            df.drop_duplicates(subset=exp_cols), file_path, backup=False
        )

    return df


def split_sources():
    """This should be stored somewhere central and updated to use the database

    Probably source selection utilities

    Takes in nothing and return a dictionary
    {'use_env': [source1, source2],
     'full_coverage': ['source10', 'source11']}
    """

    # split out sources
    split_dict = {"full_coverage_sources": hosp_prep.full_coverage_sources()}

    return split_dict


def create_icg_single_year_est(
    df, run_id, gbd_round_id, decomp_step, run_tmp_unc, write, cf_model_type, env_path
):
    """
    This runs the pipeline from prep_env to the produce single year ICG level
    estimates of inpatient clinical data
    """

    # apply the envelope
    df = create_pop_est_jobs(
        df=df,
        run_id=run_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        run_tmp_unc=run_tmp_unc,
        write=write,
        env_path=env_path,
    )

    return df
