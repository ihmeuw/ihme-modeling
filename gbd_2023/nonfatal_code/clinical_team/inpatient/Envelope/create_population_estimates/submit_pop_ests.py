"""
Submit script to send out jobs which create population level
estimates from inpatient hospital data

Updated Sept 2019 to remove ICG level CFs and do a pretty
heavy re-factor. The draw level outputs of this can be
used in the future for any ICG CF work

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

import getpass
import glob
import multiprocessing
import os
import subprocess
import time
import warnings
from pathlib import Path

import pandas as pd
from crosscutting_functions import demographic, general_purpose, legacy_pipeline

# Get shared code path for Python bash shell and code to run in sbatch
USER = getpass.getuser()
REPO = FILEPATH


def clear_logs() -> None:
    root = FILEPATH
    errors = "{}/errors/".format(root)
    outputs = "{}/output/".format(root)

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


def delete_pop_estimate_files(run_id: int) -> None:
    """
    Deletes the output files used to make the uncertainty product
    """

    # get list of anything in the dir
    to_delete = glob.glob(FILEPATH
    )

    # delete it
    if to_delete:
        print(f"Deleting {len(to_delete)} existing pop estimate split files...")
        for fpath in to_delete:
            os.remove(fpath)
        print("Files deleted")


def create_mem_est(df: pd.DataFrame) -> pd.DataFrame:
    """
    Jobs are being sent out with uniform mem reqs but data is highly variable
    First pass is super rough, split the jobs into 3 groups
    """
    memdf = df.groupby(["age_group_id", "sex_id"]).size().reset_index()
    memdf["mem"] = 100
    memdf.loc[memdf[0] > 750000, "mem"] = 120
    memdf.loc[memdf[0] > 1000000, "mem"] = 140

    return memdf


def get_run_que(run_id: int):
    p = pd.read_pickle(FILEPATH)
    return p.que


def make_uncertainty_jobs(
    df: pd.DataFrame, run_id: int, env_path: str, draws: int, fix_failed=False
) -> None:
    """
    send out jobs to run in parallel for the product of the envelope and
    the modeled correction factors
    """
    memdf = create_mem_est(df[["age_group_id", "sex_id"]].copy())
    print("memory values:")
    print(memdf["mem"].value_counts().to_string())

    # get the que from apply env pickle
    apply_env_que = get_run_que(run_id)

    # log directory
    log_path = FILEPATH

    if fix_failed:
        print("Submitting batch jobs to fix the jobs that failed")
        for index, _ in df.iterrows():
            age = df["age_group_id"][index]
            sex = df["sex_id"][index]
            mem = memdf.query("age_group_id == @age & sex_id == @sex").mem.unique()[0]
            sbatch = (ADDRESS.format(
                    a=int(age),
                    s=int(sex),
                    draws=draws,
                    threads=1,
                    memory=int(mem),
                    REPO=REPO,
                    r=run_id,
                    ep=env_path,
                    que=apply_env_que,
                    log_path=log_path,
                )
            )

            subprocess.call(sbatch, shell=True)
        print("Batch jobs finished")
    else:
        ages = df.age_group_id.unique()
        sexes = df.sex_id.unique()
        print("Sbatching PopEstimates jobs...")
        for age in ages:
            for sex in sexes:
                mem = memdf.query("age_group_id == @age & sex_id == @sex").mem.unique()[0]
                sbatch = (ADDRESS.format(
                        a=int(age),
                        s=int(sex),
                        draws=draws,
                        threads=1,
                        memory=int(mem),
                        REPO=REPO,
                        r=run_id,
                        ep=env_path,
                        que=apply_env_que,
                        log_path=log_path,
                    )
                )
                subprocess.call(sbatch, shell=True)

        print("Batch jobs finished")


def mc_file_reader(fpath: str) -> pd.DataFrame:
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

    return dat


def read_split_icg_jobs(cores: int, run_id: int) -> pd.DataFrame:
    """
    reads all the hdf files sent out above back in and appends them
    """

    print("reading select cols from pop level estimate files in...")
    start = time.time()

    parent_dir = (FILEPATH
    )

    files = glob.glob(parent_dir + "*.H5")
    len_files = len(files)
    print(f"begin reading in the {len_files} temp files from {parent_dir}")

    p = multiprocessing.Pool(cores)
    dat_list = list(p.map(mc_file_reader, files))
    df = pd.concat(dat_list, sort=False, ignore_index=True)
    df.drop_duplicates(inplace=True)

    print("read popEst jobs finished in {} seconds".format((time.time() - start)))
    return df


def fix_failed_jobs(df: pd.DataFrame, run_id: int, env_path: str, draws: int) -> None:
    """
    Re-send jobs here based on two failure types. First if the job died

    Second is a little tricker, resend if a job writes an empty dataframe 
    This is related to hdf files having some disk locking issue
    """
    present_sources = df.source.unique().tolist()
    full_cover_sources = legacy_pipeline.full_coverage_sources()

    present_full = [s for s in present_sources if s in full_cover_sources]
    present_env = [s for s in present_sources if s not in full_cover_sources]
    expected_files = []
    if present_full:
        expected_files.append("df_fc")
    if present_env:
        expected_files.append("df_env")

    exp_files = []
    for age in df.age_group_id.unique():
        for sex in df.sex_id.unique():
            for t in expected_files:
                exp_files.append("{}_{}_{}.H5".format(int(age), int(sex), t))
    # get a list of files the para jobs wrote
    output_files = glob.glob(FILEPATH.format(run_id)
    )
    diff_files = set(exp_files) - set([os.path.basename(f) for f in output_files])
    for f in diff_files:
        print(f)
        f = f.split(".")[0]
        f = f.split("_")
        rdf = pd.DataFrame({"age_group_id": f[0], "sex_id": f[1]}, index=[0])
        make_uncertainty_jobs(
            rdf,
            run_id,
            env_path=env_path,
            fix_failed=True,
            draws=draws,
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
            env_path=env_path,
            fix_failed=True,
            draws=draws,
        )
        general_purpose.job_holder(job_name="unc_", sleep_time=40)
        zeroes = [z for z in output_files if os.path.getsize(z) < 1]


def create_pop_est_jobs(
    df: pd.DataFrame,
    run_id: int,
    env_path: str,
    draws: int,
    run_tmp_unc: bool = True,
    write: bool = False,
    read_cores: int = 2,
) -> pd.DataFrame:
    """
    Send out the 42 jobs by age and sex to generate population level estimates at
    the ICG level from inpatient data
    """

    warnings.warn(
        "This script uses multiprocessing with {} pools. If ran on fair cluster "
        "then it needs to be ran with at least {} threads.".format(read_cores, read_cores)
    )
    df = demographic.year_id_switcher(df)

    starting_icgs = df.icg_id.unique()

    # delete existing env * Admit fraction and full cover jobs and run again
    if run_tmp_unc:
        clear_logs()
        delete_pop_estimate_files(run_id=run_id)
        make_uncertainty_jobs(
            df,
            run_id=run_id,
            env_path=env_path,
            draws=draws,
        )
        general_purpose.job_holder(job_name="unc_", sleep_time=40, init_sleep=120)
        fix_failed_jobs(
            df,
            run_id=run_id,
            env_path=env_path,
            draws=draws,
        )

    # hold the script up until all the uncertainty jobs are done
    general_purpose.job_holder(job_name="unc_", sleep_time=40)

    # rough check to see if the length of new files is what we'd expect
    if run_tmp_unc:
        check_len = df.age_group_id.unique().size * df.sex_id.unique().size
        path = Path(FILEPATH.format(run_id)
        )
        # we can produce two files for each sex and age: one for fc the other for env
        # so the number of files might be double than check_len
        actual_len = len({e.stem.split("_df")[0] for e in path.glob("*.H5")})
        if actual_len != check_len:
            raise ValueError(
                "Check the error logs, it looks like something went wrong while "
                "re-writing files. We expected {} files and there were {} files".format(
                    check_len, actual_len
                )
            )

    df = read_split_icg_jobs(cores=read_cores, run_id=run_id)

    end_icgs = df.icg_id.unique()

    diff = set(starting_icgs).symmetric_difference(set(end_icgs))

    if len(diff) > 0:
        raise ValueError(
            "The difference in icgs is {}. Reprocessing these without correction"
            " factor uncertainty.".format(diff)
        )

    if write:
        file_path = (FILEPATH.format(run_id)
        )
        print("Writing the applied envelope data to {}".format(file_path))
        exp_cols = [
            "estimate_id",
            "icg_id",
            "icg_name",
            "location_id",
            "source",
            "year_id",
        ]
        general_purpose.write_hosp_file(
            df.drop_duplicates(subset=exp_cols), file_path, backup=False
        )

    return df


def create_icg_single_year_est(
    df: pd.DataFrame, run_id: int, run_tmp_unc: bool, write: bool, env_path: str, draws: int
) -> pd.DataFrame:
    """
    This runs the pipeline from prep_env to the produce single year ICG level
    estimates of inpatient clinical data
    """

    # apply the envelope
    df = create_pop_est_jobs(
        df=df,
        run_id=run_id,
        run_tmp_unc=run_tmp_unc,
        write=write,
        env_path=env_path,
        draws=draws,
    )

    return df
