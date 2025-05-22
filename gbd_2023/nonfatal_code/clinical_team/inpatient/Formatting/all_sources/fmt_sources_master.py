import getpass
import glob
import os
import subprocess

import pandas as pd
from crosscutting_functions import general_purpose
from crosscutting_functions.get-cached-population import cached_pop_tools


def profile_jobs(source, source_type):
    if source_type == "old":
        ext = ".dta"
    elif source_type == "new":
        ext = ".H5"

    data_path = (
        "FILEPATH/{s}/data/"
        "FILEPATH/formatted_{s}{ext}".format(s=source, ext=ext)
    )

    
    size = 1
    if source != "USA_NAMCS":
        size = os.path.getsize(data_path)

    if ext == ".dta":
        size = size / 3.0

    # set slots and mem based on filesize
    # cap at 300G
    if size > 10000000:
        mem = round(size / 1000000)
    else:
        mem = 6

    return round(min(mem, 300))


def submit_jobs(
    sources,
    run_id,
    source_type,
    clinical_age_group_set_id,
    remove_live_births,
):

    user = getpass.getuser()
    ppath = f"FILEPATH/run_{run_id}/init.p"
    p = pd.read_pickle(ppath)

    hosp_repo = "FILEPATH/{}/clinical_info".format(user)

    for source in sources:

        mem = profile_jobs(source, source_type)

        # log directory
        log_path = f"FILEPATH/run_{run_id}/fmt_sources"

        sbatch = (
            "ADDRESS".format(
                source=source,
                log_path=log_path,
                threads=1,
                memory=mem,
                hosp_repo=hosp_repo,
                type=source_type,
                a=clinical_age_group_set_id,
                lb=remove_live_births,
                run=run_id,
                que=p.que,
            )
        )

        subprocess.call(sbatch, shell=True)
        # time.sleep(2)


def clear_logs():
    root = "FILEPATH"
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


def verify_sources(new_sources, old_sources, run_id):
    all_sources = old_sources + new_sources
    dir_sources = []
    files = glob.glob(
        "FILEPATH/run_{}/estimates/"
        "FILEPATH/*.H5".format(run_id)
    )

    for f in files:
        file_name = f.split("/")[-1].split(".H5")[0]
        dir_sources.append(file_name)

    source_diff = set(all_sources).symmetric_difference(dir_sources)

    # we renamed some sources so we expect there to be a difference
    expected_diff = set()
    # expected_diff = set(['USA_HCUP_SID_09', 'USA_HCUP_SID_08',
    #                      'USA_HCUP_SID_03', 'USA_HCUP_SID_07',
    #                      'USA_HCUP_SID_06', 'USA_HCUP_SID_05',
    #                      'USA_HCUP_SID_04', 'USA_HCUP_SID'])

    assert_msg = """This is the difference between what sources are present,
        and what we expected the diff to be (a diff of diffs): {}. Check your
        qstat, if you still have fmt_* jobs running then manually verify sources
        after they are finished. If there are no fmt_* jobs then something went wrong.""".format(
        source_diff.symmetric_difference(expected_diff)
    ).replace(
        "\n", ""
    )
    assert_msg = " ".join(assert_msg.split())
    assert source_diff == expected_diff, assert_msg

    # now check population cache
    cached_pop_tools.check_pop_files(run_id=run_id, exp_sources=all_sources)
    print("Sources have been verified")
    return


def main(
    run_id,
    clinical_age_group_set_id,
    gbd2019_decomp4,
    new_sources,
    old_sources,
    remove_live_births,
):
    """
    Send out the parallel jobs to process the formatted files into
    our master_data structure
    """
    clear_logs()  # remove the existing master_data log files

    # for decomp 4 we're only appending on the new hospital data. We don't
    # need to do a full inpatient run
    if gbd2019_decomp4:
        new_sources = [
            "BWA_HMDS",
            "MEX_SINAIS",
            "GEO_Discharge_16_17",
            "SWE_Patient_Register_15_16",
            "IND_SJMCH",
            "ECU_INEC_15_17",
        ]

        old_sources = ["NZL_NMDS_16_17"]

    submit_jobs(
        sources=new_sources,
        run_id=run_id,
        source_type="new",
        clinical_age_group_set_id=clinical_age_group_set_id,
        remove_live_births=remove_live_births,
    )

    submit_jobs(
        sources=old_sources,
        run_id=run_id,
        source_type="old",
        clinical_age_group_set_id=clinical_age_group_set_id,
        remove_live_births=remove_live_births,
    )

    general_purpose.job_holder(job_name="fmt_", sleep_time=40, init_sleep=300)
    verify_sources(new_sources, old_sources, run_id)

    print("All of the master data files have finished processing")
