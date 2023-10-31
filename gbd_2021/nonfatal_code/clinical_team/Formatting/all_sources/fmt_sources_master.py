import subprocess
import getpass
import glob
import os
import time
import sys
import re
import pandas as pd
# import ipdb
from clinical_info.Functions import cached_pop_tools
from clinical_info.Functions import hosp_prep


def profile_jobs(source, source_type):
    if source_type == 'old':
        ext = '.dta'
    elif source_type == 'new':
        ext = '.H5'

    data_path = FILEPATH.format(s=source, ext=ext)

    # ugh, USA NAMCS data is annoying
    size = 1
    if source != 'USA_NAMCS':
        size = os.path.getsize(data_path)

    if ext == '.dta':
        size = size / 3.

    # set slots and mem based on filesize
    mem = 6

    if size > 35000000:
        mem = 12
    if size > 120000000:
        mem = 40

    return mem


def submit_jobs(sources, run_id, source_type,
                clinical_age_group_set_id,
                remove_live_births,
                gbd_round_id, decomp_step):

    user = getpass.getuser()
    ppath = FILEPATH
    p = pd.read_pickle(ppath)


    sge_dir = FILEPATH
    hosp_repo = FILEPATH.format(user)

    for source in sources:

        mem = profile_jobs(source, source_type)

        qsub = 'QSUB {source} {type} {a} {r} {s} {lb} {run}'.\
                format(source=source, sge_dir=sge_dir,
                       threads=1, memory=mem,
                       hosp_repo=hosp_repo, type=source_type,
                       a=clinical_age_group_set_id,
                       r=gbd_round_id, s=decomp_step,
                       lb=remove_live_births,
                       run=run_id,
                       que=p.que)

        subprocess.call(qsub, shell=True)
        # time.sleep(2)


def clear_logs():
    root = 'FILEPATH'
    errors = '{}/errors/'.format(root)
    outputs = '{}/output/'.format(root)

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


def verify_sources(new_sources, old_sources, run_id):
    all_sources = old_sources + new_sources
    dir_sources = []
    files = glob.glob(FILEPATH.format(run_id))

    for f in files:
        file_name = f.split('/')[-1].split('.H5')[0]
        dir_sources.append(file_name)

    source_diff = set(all_sources).\
        symmetric_difference(dir_sources)

    # we renamed some sources so we expect there to be a difference
    expected_diff = set()
    # expected_diff = set(['USA_HCUP_SID_09', 'USA_HCUP_SID_08',
    #                      'USA_HCUP_SID_03', 'USA_HCUP_SID_07',
    #                      'USA_HCUP_SID_06', 'USA_HCUP_SID_05',
    #                      'USA_HCUP_SID_04', 'USA_HCUP_SID'])

    assert_msg = """This is the difference between what sources are present,
        and what we expected the diff to be (a diff of diffs): {}. Check your
        qstat, if you still have fmt_* jobs running then manually verify sources
        after they are finished. If there are no fmt_* jobs then something went wrong.""".\
        format(source_diff.symmetric_difference(expected_diff)).\
        replace("\n", "")
    assert_msg = " ".join(assert_msg.split())
    assert source_diff == expected_diff, assert_msg

    # now check population cache
    cached_pop_tools.check_pop_files(run_id=run_id, exp_sources=all_sources)
    print("Sources have been verified")
    return


def main(run_id, clinical_age_group_set_id, gbd_round_id, decomp_step,
         gbd2019_decomp4, new_sources, old_sources, remove_live_births):
    """
    Send out the parallel jobs to process the formatted files into
    our master_data structure
    """
    clear_logs()  # remove the existing master_data log files

    # for decomp 4 we're only appending on the new hospital data. We don't
    # need to do a full inpatient run
    if gbd2019_decomp4:
        new_sources = ['BWA_HMDS', 'MEX_SINAIS', 'GEO_Discharge_16_17',
                       'SWE_Patient_Register_15_16', 'IND_SJMCH',
                       'ECU_INEC_15_17']

        old_sources = ['NZL_NMDS_16_17']

    submit_jobs(sources=new_sources, run_id=run_id,
                source_type='new',
                clinical_age_group_set_id=clinical_age_group_set_id,
                gbd_round_id=gbd_round_id,
                decomp_step=decomp_step,
                remove_live_births=remove_live_births)

    submit_jobs(sources=old_sources, run_id=run_id,
                source_type='old',
                clinical_age_group_set_id=clinical_age_group_set_id,
                gbd_round_id=gbd_round_id,
                decomp_step=decomp_step,
                remove_live_births=remove_live_births)

    hosp_prep.job_holder(job_name='fmt_', sleep_time=40, init_sleep=300)

    verify_sources(new_sources, old_sources, run_id)
    print("All of the master data files have finished processing")
