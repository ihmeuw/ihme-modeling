import numpy as np
import pandas as pd
import platform
import sys
import getpass
import glob
import subprocess
import time
import warnings
import os
import re
import datetime

from clinical_info.Functions import hosp_prep

USER = getpass.getuser()
REPO = FILEPATH

def qsubber(repo, group_num, run_id):

    qsub = "QSUB"/
           .format(group_num, repo, repo, group_num, run_id)
    print(qsub)
    subprocess.call(qsub, shell=True)


def send_jobs(run_id, groups=350, jobs_at_once=25, resend_list=[], repo=REPO):

    clean_finished_querying_dir(run_id)

    finished_count = 0
    counter = 0
    wait = 11
    if resend_list:
        jobs = resend_list
    else:
        jobs = np.arange(0, groups, 1)

    # run everything like usual
    for group_num in jobs:
        group_num = "group_{}".format(group_num)
        status = 'wait'
        counter += 1
        if counter > jobs_at_once:
            while status == 'wait':
                print(
                    "max jobs running. Waiting until a job finishes querying the dB to send a new one")
                fqp = FILEPATH.format(
                    run_id)

                # Count of jobs that have finished querying the dB
                updated_count = len(glob.glob(fqp + FILEPATH))

                # if no jobs have finished querying the dB then sleep
                if finished_count >= updated_count:
                    time.sleep(40)

                else:
                    finished_count += 1
                    print("Starting next job {}".format(group_num))
                    qsubber(repo, group_num, run_id)
                    time.sleep(wait)
                    status = 'go'
        else:  # this sends out the first set of jobs == jobs_at_once)
            print(group_num)
            qsubber(repo, group_num, run_id)
            time.sleep(wait)
    return


def check_jobs(groups, run_id):

    ob_icg_files = glob.glob(
        FILEPATH.format(run_id, 'icg'))
    ob_bundle_files = glob.glob(
        FILEPATH.format(run_id, 'bundle'))

    ob_files = ob_icg_files + ob_bundle_files
    ob_files = [int(re.sub("[^0-9]", "", os.path.basename(f)[:-3]))
                for f in ob_files]
    ob_files = list(set(ob_files))

    exp_files = np.arange(0, groups, 1)
    diff = set(ob_files).symmetric_difference(set(exp_files))

    return list(diff)


def outpatient_aggregations(df, good_facility_ids=[11, 22, 95, 19]):
    numer = df[df.estimate_type == 'otp_any_indv_cases']
    denom = df[df.estimate_type == 'otp_any_claims_cases']

    denom = denom[denom.facility_id.isin(good_facility_ids)]
    denom = denom.groupby(denom.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    numer = numer.groupby(numer.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    result = pd.concat([denom, numer], ignore_index=True, sort=False)

    return result


def aggregate_output(run_id, clinical_age_group_set_id, groups=350,
                     agg_to_national=True,
                     cause_type='icg', cf_prep=True,
                     break_if_not_contig=False):

    # check if jobs are running
    hosp_prep.job_holder(job_name="ms_group_", sleep_time=200, init_sleep=0)


    output_dir = (FILEPATH)
    files = glob.glob(f"{output_dir}/{cause_type}")

    expected_files = []
    for i in range(groups):
        expected_files.append(f"{output_dir}/{cause_type}_group_{i}.H5")
    if len(files) != groups:
        warnings.warn("The number of files we read back in doesn't match "
                      "the number of jobs sent out")
        missing_files = set(expected_files) - set(files)
        raise ValueError("Look to the logs for failed jobs. These files are"
                         f"missing: {missing_files}")
    print("All jobs have finished, aggregating data to the national level "
          "or subnational level depending on args and writing the "
          "condensed CSV and H5 files.")
    # read in 1 file to append all groups to
    df = pd.read_hdf(files[0])
    df = hosp_prep.age_binning(df, terminal_age_in_data=False, drop_age=True,
                               break_if_not_contig=break_if_not_contig,
                               clinical_age_group_set_id=clinical_age_group_set_id)

    if agg_to_national:
        df['location_id'] = 102

    pre = df[(df['estimate_type'] == 'inp_any_indv_cases')].val.sum()

    # make first otp_cf_df outside of loop
    if cf_prep:
        otp_cf_df = outpatient_aggregations(df)

    counter = 1
    for f in files[1:]:
        print("starting {}".format(f))
        tmp = pd.read_hdf(f)
        if len(tmp) == 0:
            continue

        tmp = hosp_prep.age_binning(
            tmp, terminal_age_in_data=False, drop_age=True, break_if_not_contig=break_if_not_contig,
            clinical_age_group_set_id=clinical_age_group_set_id)
        if agg_to_national:
            tmp['location_id'] = 102

        if cf_prep:
            otp_tmp = outpatient_aggregations(tmp)
            otp_cf_df = pd.concat([otp_cf_df, otp_tmp],
                                  ignore_index=True, sort=False)

        pre += tmp[(tmp['estimate_type'] == 'inp_any_indv_cases')].val.sum()

        df = pd.concat([df, tmp], ignore_index=True, sort=False)

        df = df[df['estimate_type'] !=
                'inp_otp_any_adjusted_otp_only_claims_cases']

        df = df.fillna(0)
        if counter % 25 == 0 or counter == groups:
            df = df.groupby(df.columns.drop('val').tolist()).agg(
                {'val': 'sum'}).reset_index()
            if cf_prep:
                otp_cf_df = otp_cf_df.groupby(otp_cf_df.drop(
                    ['val'], axis=1).columns.tolist()).agg({'val': 'sum'}).reset_index()

        print("There are {} rows in df object".format(df.shape[0]))

        print("{}% done".format(round(counter / float(groups), 3) * 100))
        counter += 1

    print("Done reading files.")

    if cf_prep:
        otp_cf_df = otp_cf_df.groupby(otp_cf_df.drop(
            ['val'], axis=1).columns.tolist()).agg({'val': 'sum'}).reset_index()

        # save
        otp_cf_base_dir = FILEPATH.format(
            run_id)
        otp_cf_filepath = FILEPATH.format(
            otp_cf_base_dir)
        otp_cf_df.to_csv(otp_cf_filepath, index=False)

    df = df.groupby(df.columns.drop('val').tolist()).agg(
        {'val': 'sum'}).reset_index()

    df_pri = df[(df['estimate_type'] == 'inp_any_indv_cases')].val.sum()
    print("pre equals {} and df.val sum equals {} It's {} they're equal!".format(
        pre, df_pri, pre == df_pri))
    assert df_pri == pre, "we expect primary inpatient indv cases to be equal and they're not"

    # write to a csv for use with R scripts
    filepath = FILEPATH.format(
        run_id, cause_type)
    filepath = filepath.replace("\r", "")
    df.to_csv(filepath, index=False)

    write_path = FILEPATH.format(
        run_id, cause_type)
    write_path = write_path.replace("\r", "")
    print(f"Saving to {write_path}...")
    hosp_prep.write_hosp_file(df, write_path, backup=False)
    print("Saved.")

    return


def confirm_run_id(run_id, break_if_not_current=True):

    if 'test' in str(run_id):
        msg = "\n\n{} is acceptable for development- "\
              "Note: this is not a production run".format(run_id)
        warnings.warn(msg)
        return
    else:
        try:
            run_id = int(run_id)
        except Exception as e:
            msg = "{} is not an acceptable value for a run_id. A run_id should be an "\
                  "integer or castable to int.".format(run_id)
            assert False, msg

    if int(run_id) > 0:
        # get list of folders in run directory
        run_reg = FILEPATH
        run_paths = glob.glob(run_reg)
        # turn these folders into a list of ints, an int for each run
        runs = [int(re.sub("[^0-9]", "", f)) for f in run_paths]
        # the highest integer should be the current run_id
        current_run = max(runs)

        if run_id < current_run:
            msg = """
            The current run_id is {} but this script is being run with id {}
            """.format(current_run, run_id)
            if break_if_not_current:
                assert False, msg
            else:
                warnings.warn(msg)
        elif run_id > current_run:
            msg = """
            Run id {} doesn't have any directories yet. Please instantiate the run
            """.format(run_id)
            assert False, msg
    return


def clean_finished_querying_dir(run_id):
    out_dir =FILEPATH.format(
        run_id)

    files = glob.glob(out_dir + FILEPATH)
    if files:
        warnings.warn("removing existing helper files")
        for f in files:
            os.remove(f)
    return


def attach_nid(df):
    """
    add merged (private + medicare supplemental insurance) NIDs
    onto the marketscan data
    """
    nid_dict = {2000: 244369,
                2010: 244370,
                2011: 336850,
                2012: 244371,
                2013: 336849,
                2014: 336848,
                2015: 336847,
                2016: 408680,
                2017: 433114}

    df = hosp_prep.fill_nid(df, nid_dict)

    assert (df['nid'] == 0).sum() == 0, "There are missing NIDs"

    return df


def convert_int(df, cols):
    for col in cols:
        assert df[col].isnull().sum(
        ) == 0, "There are null values present in {}".format(col)
        df[col] = df[col].astype(int)

    return df


def prep_for_upload(df, run_id):
    try:
        df['run_id'] = int(run_id)
    except Exception as e:
        print("It looks like this is a dev run, the run_id {} can't be cast to int. {}".format(
            run_id, e))
    df = hosp_prep.group_id_start_end_switcher(df, remove_cols=True)

    df = attach_nid(df)

    # use year_id
    assert (df['year_start'] == df['year_end']).all(),\
        "Some year start and ends don't match {}".format(
        df[df['year_start'] != df['year_end']])
    df.rename(columns={'year_start': 'year_id'}, inplace=True)
    df.drop('year_end', axis=1, inplace=True)

    # collapse data
    collapse_cols = df.columns.drop('val').tolist()
    df = df.groupby(collapse_cols).agg({'val': 'sum'}).reset_index()

    df['representative_id'] = 1

    df['source_type_id'] = 17

    df['diagnosis_id'] = 3
    df.loc[df['estimate_type'].str.contains('inp_pri'), 'diagnosis_id'] = 1

    # make dictionary, keys are estimate_types, vals are estimate_ids
    est_dict = {'inp_any_claims_cases': 16, 'inp_any_indv_cases': 17,
                'inp_otp_any_adjusted_otp_only_indv_cases': 21, 'inp_pri_claims_cases': 14,
                'inp_pri_indv_cases': 15, 'otp_any_claims_cases': 18, 'otp_any_indv_cases': 19}

    df['estimate_id'] = 0
    for etype in df.estimate_type.unique():
        df.loc[df['estimate_type'] == etype, 'estimate_id'] = est_dict[etype]

    assert (df['estimate_id'] == 0).sum(
    ) == 0, "There are missing estimate IDs"

    cols = ['age_group_id', 'sex_id', 'location_id', 'year_id', 'representative_id',
            'estimate_id', 'source_type_id', 'icg_id', 'nid', 'run_id', 'diagnosis_id']
    df = convert_int(df, cols)

    drops = [c for c in ['age_group_unit', 'outcome_id', 'icg_name',
                         'facility_id', 'estimate_type'] if c in df.columns]
    df.drop(drops, axis=1, inplace=True)

    print("Writing the large claims CSV to FILEPATH
          "intermediate` folder. This will take a few minutes ... ")
    filepath = FILEPATH.format(
        run_id)
    filepath = filepath.replace("\r", "")
    df.to_csv(filepath, index=False)

    return


def run_marketscan(run_id, groups=350):

    confirm_run_id(run_id, break_if_not_current=True)

    print("Launching the claims process with {} groups "
          "for run {}.".
          format(groups, run_id))

    send_jobs(groups=groups, run_id=run_id)
    hosp_prep.job_holder(job_name="ms_group_", sleep_time=200)

    counter = 0
    while counter < 3:
        resend_list = check_jobs(groups=groups, run_id=run_id)
        if resend_list:
            send_jobs(groups=groups, resend_list=resend_list, run_id=run_id)
        else:
            break
        counter += 1
        hosp_prep.job_holder(job_name="ms_group_", sleep_time=200)
    print("Continuing with aggregate_output...")

    aggregate_output(groups=groups, agg_to_national=False,
                    cause_type='bundle', run_id=run_id,
                    clinical_age_group_set_id=3)

    print("The Marketscan claims process has finished")
    return
