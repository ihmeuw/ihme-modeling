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


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)
import hosp_prep

if platform.system() == "Linux":
    repo = r"FILEPATH".format(user)
else:
    assert False, "This must be run on the cluster"


def qsubber(repo, group_num, run_id):
    # qsub the actual job to go out by file name
    # group_num = get_group_number(file)
    qsub = r"qsub -P proj_hospital -N ms_{}"\
           r" -l fthread=4 -l m_mem_free=90G -l h_rt=12:00:00 -q all.q" \
           r" -o FILEPATH
           r" {}EnvelopeFILEPATH
           .format(group_num, repo, repo, group_num, run_id)
    print(qsub)
    subprocess.call(qsub, shell=True)


def send_jobs(run_id, groups=350, jobs_at_once=25, resend_list=[], repo=repo):
    """
    qsub a list of jobs to process MS claims data from the dB

    Params:
        groups: (int)
            the number of groups that were used to process the MS claims data. This
            is dependent on the ms db helpers module, so do not change it unless you're certain
        jobs_at_once: (int)
            the number of jobs to run simulataneously.
        resend_list: (list)
            Jobs fail on the cluster stochastically so I built this parameter
            to re-send failed jobs.
        repo: (str)
            cluster location of the user's clinical data repo to pull the worker scripts from
        run_id: (str or int)
            identifies the run number for storing and accessing all of the data associated with a given
            run of the clinical process

    """

    # remove any existing helper files
    clean_finished_querying_dir(run_id)

    finished_count = 0
    counter = 0
    wait = 11
    # files = get_eid_groups()
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
                print("max jobs running. Waiting until a job finishes querying the dB to send a new one")
                fqp = "FILEPATH".format(run_id)

                updated_count = len(glob.glob(fqp + "/*.csv"))  # Count of jobs that have finished querying the dB

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

    # create a list of ints to match with a list of group numbers that went out
    # get all the icg and bundle files that were written
    ob_icg_files = glob.glob("FILEPATH".format(run_id, 'icg'))
    ob_bundle_files = glob.glob("FILEPATH".format(run_id, 'bundle'))

    # combine the lists, pull out just the group num integers and then remove duplicates
    ob_files = ob_icg_files + ob_bundle_files
    ob_files = [int(re.sub("[^0-9]", "", os.path.basename(f)[:-3])) for f in ob_files]
    ob_files = list(set(ob_files))

    exp_files = np.arange(0, groups, 1)
    diff = set(ob_files).symmetric_difference(set(exp_files))

    return list(diff)


def job_holder(sleep_time=200):
    """
    checks users qstat every n seconds to see if any ms jobs are still running

    Params:
        sleep_time: (int) time in seconds to sleep between qstat calls
    """

    status = "wait"
    while status == "wait":
        p = subprocess.Popen("qstat", stdout=subprocess.PIPE)
        # get qstat text
        qstat_txt = p.communicate()[0]
        qstat_txt = qstat_txt.decode('utf-8')
        # search qstat text for our job name
        job_count = qstat_txt.count("ms_group_")
        if job_count == 0:
            status = "go"
        else:
            time.sleep(sleep_time)
    return


def outpatient_aggregations(df, good_facility_ids=[11, 22, 95]):
    numer = df[df.estimate_type == 'otp_any_indv_cases']
    denom = df[df.estimate_type == 'otp_any_claims_cases']

    good_facility_ids = [11, 22, 95]

    denom = denom[denom.facility_id.isin(good_facility_ids)]
    denom = denom.groupby(denom.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    numer = numer.groupby(numer.drop(['facility_id', 'val'], axis=1).
                          columns.tolist()).agg({'val': 'sum'}).reset_index()

    result = pd.concat([denom, numer], ignore_index=True, sort=False)

    return result


def aggregate_output(run_id, groups=350, agg_to_national=True, cause_type='icg', cf_prep=True):
    """
    MS is processed by enrolid now, which results in very large output files
    This function waits until all jobs are finished then aggregates them all together.

    Params:
        groups: (int) the number of groups that were used to process the MS claims data. This
                      is dependent on the ms db helpers module, so do not change it unless you're certain
        agg_to_national (bool) if True, re-assign all location IDs to the national level in order to save space
    """

    job_holder()
    print("All jobs have finished, aggregating data to the national level and writing the condensed CSV and H5 files.")

    files = glob.glob("FILEPATH".format(run_id, cause_type))
    if len(files) != groups:
        warnings.warn("The number of files we read back in doesn't match the number of jobs sent out")
        assert False, "Look to the logs for failed jobs"

    # read in 1 file to append all groups to
    df = pd.read_hdf(files[0])
    df = hosp_prep.age_binning(df, terminal_age_in_data=False, drop_age=True)

    if agg_to_national:
        df['location_id'] = 102

    # case sum to check aggregation
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

        tmp = hosp_prep.age_binning(tmp, terminal_age_in_data=False, drop_age=True)
        if agg_to_national:
            tmp['location_id'] = 102

        if cf_prep:
            otp_tmp = outpatient_aggregations(tmp)
            otp_cf_df = pd.concat([otp_cf_df, otp_tmp],
                                  ignore_index=True, sort=False)

        pre += tmp[(tmp['estimate_type'] == 'inp_any_indv_cases')].val.sum()

        df = pd.concat([df, tmp], ignore_index=True, sort=False)

        # we can drop inp_otp claims cases cause they're unused
        df = df[df['estimate_type'] != 'inp_otp_any_adjusted_otp_only_claims_cases']

        df = df.fillna(0)
        if counter % 25 == 0 or counter == groups:
            df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()
            if cf_prep:
                otp_cf_df = otp_cf_df.groupby(otp_cf_df.drop(['val'], axis=1).columns.tolist()).agg({'val': 'sum'}).reset_index()

        print("There are {} rows in df object".format(df.shape[0]))
        print("{}% done".format(round(counter/float(groups), 3) * 100))
        counter += 1

    if cf_prep:
        # one last groupby
        otp_cf_df = otp_cf_df.groupby(otp_cf_df.drop(['val'], axis=1).columns.tolist()).agg({'val': 'sum'}).reset_index()

        # save
        otp_cf_base_dir = "FILEPATH".format(run_id)
        otp_cf_filepath = "{}/bundle_condensed_otp_repreped_claims_process.csv".format(otp_cf_base_dir)
        otp_cf_df.to_csv(otp_cf_filepath, index=False)

    # a final groupby for good measure, I think the counter was perhaps not hitting the groups value above so the last
    # groupby didn't occur creating a bit of a larger data set than necessary
    df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()

    df_pri = df[(df['estimate_type'] == 'inp_any_indv_cases')].val.sum()
    print("pre equals {} and df.val sum equals {} It's {} they're equal!".format(pre, df_pri, pre == df_pri))
    assert df_pri == pre, "we expect primary inpatient indv cases to be equal and they're not"

    # write to a csv for use with R scripts
    filepath = "FILEPATH".format(run_id, cause_type)
    filepath = filepath.replace("\r", "")
    df.to_csv(filepath, index=False)

    # Saving the file to H5 as well and back it up
    write_path = "FILEPATH".format(run_id, cause_type)
    write_path = write_path.replace("\r", "")
    hosp_prep.write_hosp_file(df, write_path, backup=False)

    if cause_type == 'icg':
        prep_for_upload(df=df, run_id=run_id)

    return


def confirm_run_id(run_id, break_if_not_current=True):
    """
    We'll need some tools in place to make sure our runs are working correctly,
    This might make more sense to move to a different script like hosp_prep eventually
    """

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
        run_reg = "FILEPATH"
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
    out_dir = "FILEPATH".format(run_id)

    files = glob.glob(out_dir + "/*.csv")
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
                2016: 408680}

    df = hosp_prep.fill_nid(df, nid_dict)

    assert (df['nid'] == 0).sum() == 0, "There are missing NIDs"

    return df


def convert_int(df, cols):
    """
    Cast tricky ints that get converted to floats b/c Pandas sucks at NAs back to int
    """
    for col in cols:
        assert df[col].isnull().sum() == 0, "There are null values present in {}".format(col)
        df[col] = df[col].astype(int)

    return df

def prep_for_upload(df, run_id):
    """
    The condensed file needs to be cleaned a little bit to fit with the schema of
    the other processes (inpatient, outpatient) so far we:

        * switch age start/end to age group id
        * switch year start/end to year id
        * add bunches of columns

    """
    try:
        df['run_id'] = int(run_id)
    except Exception as e:
        print("It looks like this is a dev run, the run_id {} can't be cast to int. {}".format(run_id, e))
    df = hosp_prep.group_id_start_end_switcher(df, remove_cols=True)

    df = attach_nid(df)

    # use year_id
    assert (df['year_start'] == df['year_end']).all(),\
        "Some year start and ends don't match {}".format(\
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

    assert (df['estimate_id'] == 0).sum() == 0, "There are missing estimate IDs"

    # we need real ints for the db
    cols = ['age_group_id', 'sex_id', 'location_id', 'year_id', 'representative_id',
            'estimate_id', 'source_type_id', 'icg_id', 'nid', 'run_id', 'diagnosis_id']
    df = convert_int(df, cols)

    drops = [c for c in ['age_group_unit', 'outcome_id', 'icg_name', 'facility_id', 'estimate_type'] if c in df.columns]
    df.drop(drops, axis=1, inplace=True)

    print("Writing the large claims CSV to `FILEPATH
          "intermediate` folder. This will take a few minutes ... ")
    filepath = "FILEPATH".format(run_id)
    filepath = filepath.replace("\r", "")
    df.to_csv(filepath, index=False)

    return


def run_marketscan(run_id, groups=350):
    """
    This will launch the python process to make individual records from market scan data

    Note: Do not change the groups arg unless you're 100% sure you know what you're doing
    """

    confirm_run_id(run_id, break_if_not_current=True)

    print("Launching the claims process with {} groups "\
          "for run {}.".\
          format(groups, run_id))

    send_jobs(groups=groups, run_id=run_id)
    job_holder() 

    # check for failed jobs and re-send 3 times if necessary
    counter = 0
    while counter < 3:
        resend_list = check_jobs(groups=groups, run_id=run_id)
        if resend_list:
            send_jobs(groups=groups, resend_list=resend_list, run_id=run_id)
        else:
            break
        counter += 1
        job_holder()
    print("Continuing with aggregate_output...")

    aggregate_output(groups=groups, agg_to_national=False, cause_type='icg', run_id=run_id)
    aggregate_output(groups=groups, agg_to_national=False, cause_type='bundle', run_id=run_id)
    print("The Marketscan claims process has finished")
    return
