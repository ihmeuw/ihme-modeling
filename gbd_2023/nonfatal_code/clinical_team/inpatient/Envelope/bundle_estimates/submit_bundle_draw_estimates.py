"""
This is the code that will send out each worker job to create draws from ICG estimates,
aggregate to the bundle level, aggregate to 5 year groups, apply the CFs and then
compile all the results back together to write the final file to drive in the inpatient Class

"""
import getpass
import glob
import os

import pandas as pd
from crosscutting_functions.clinical_constants.values import Estimates
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    InpatientWrappers,
)
from clinical_db_tools.db_connector.database import Database
from clinical_functions import demographic, general_purpose

from inpatient.Clinical_Runs.utils.constants import InpRunSettings, RunDBSettings

USER = getpass.getuser()
REPO = FILEPATH


def get_db_reader(odbc_profile=RunDBSettings.db_profile):
    db = Database()
    db.load_odbc(odbc_profile)
    return db


def verfiy_active_bundle_cf_version_diff(run_metadata):
    """
    Pre verification that there is no diff between inp estimates'
    bundle ids in cf_version_set_id and active_bundle_metadata
    """
    db = get_db_reader()
    # 2,3,4,7,8,9
    cf_estimates = (
        Estimates.inp_primary_cf1_modeled,
        Estimates.inp_primary_cf2_modeled,
        Estimates.inp_primary_cf3_modeled,
        Estimates.inp_primary_live_births_cf1_modeled,
        Estimates.inp_primary_live_births_cf2_modeled,
        Estimates.inp_primary_live_births_cf3_modeled,
    )
    dql = (QUERY
    )
    active_bids = db.query(dql)

    dql = (QUERY
    )
    cf_bids = db.query(dql)

    # the difference here should none
    missing = set(active_bids.bundle_id.tolist()) - set(cf_bids.bundle_id.tolist())

    if missing:
        raise RuntimeError("There are missing bundle CF models.")


def exp_file_getter(age_groups, sexes, years):
    """
    age_groups, sexes, years: (list) use these to generate a list with all possible
                                     combinations of the three. This is the set of jobs
                                     we'll send out and the set we (exp)ect to get back
    """
    exp_files = [
        "{}_{}_{}".format(age, sex, year)
        for age in age_groups
        for sex in sexes
        for year in years
    ]

    return exp_files


def ob_file_getter(run_id, bin_years, for_reading=False):
    """
    glob to get a list of filenames that are written to drive after job_holder
    says it's ok to stop waiting
    """
    base = FILEPATH
    bin_dir = "agg_5_years"
    if not bin_years:
        bin_dir = "single_years"

    ob_prep_files = glob.glob(FILEPATH)
    ob_prep_files = [os.path.basename(f) for f in ob_prep_files]

    ob_final_files = glob.glob(FILEPATH)
    if for_reading:
        return ob_final_files

    ob_final_files = [os.path.basename(f) for f in ob_final_files]

    return ob_prep_files, ob_final_files


def check_files(run_id, age_groups, sexes, years, bin_years):
    """
    Compare the set of expected files against the set of observed files.
    If there are any differences the script will break
    """
    exp_prep_files = ["{}.H5".format(f) for f in exp_file_getter(age_groups, sexes, years)]
    exp_final_files = ["{}.csv".format(f) for f in exp_file_getter(age_groups, sexes, years)]

    ob_prep_files, ob_final_files = ob_file_getter(run_id, bin_years)

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


def sbatcher(age_group, sex, year, repo, run_id, draws, bin_years, queue):
    """
    Submit batch job with trailing arguments
    """
    # log directory
    log_path = FILEPATH

    sbatch = (ADDRESS
    ).format(
        r=repo,
        a=age_group,
        s=sex,
        y=year,
        run=run_id,
        d=draws,
        b=bin_years,
        log_path=log_path,
        queue=queue,
    )

    os.popen(sbatch)


def pull_run_source_years(run_id):
    db = get_db_reader()
    dql = QUERY
    years = db.query(dql)
    return years["year_id"].tolist()


def get_age_sex(clinical_age_group_set_id):
    """
    Returns 3 lists which we'll use to send jobs in parallel and check our outputs
    """
    age_groups = (
        demographic.get_hospital_age_groups(clinical_age_group_set_id)
        .age_group_id.unique()
        .tolist()
    )
    sexes = [1, 2]

    return age_groups, sexes


def get_years(bin_years, run_id):
    run_years = pull_run_source_years(run_id)

    # pre 1990 data should have already been dropped in previous steps
    # run_source is not necessarily the best source of truth when
    # we include pre-1990 data in a run
    if min(run_years) < InpRunSettings.GBD_START_YEAR:
        run_years = [yr for yr in run_years if yr >= InpRunSettings.GBD_START_YEAR]

    if not bin_years:
        return run_years

    years_binned = pd.DataFrame(run_years, columns=["year_start"])
    years_binned["year_end"] = years_binned["year_start"]
    years_binned = demographic.year_binner(years_binned)
    years = years_binned["year_start"].unique().tolist()
    return years


def get_demos(bin_years, clinical_age_group_set_id, run_id):
    age_groups, sexes = get_age_sex(clinical_age_group_set_id=clinical_age_group_set_id)
    years = get_years(bin_years, run_id)
    return age_groups, sexes, years


def main_bundle_draw_submit(run_id, draws, clinical_age_group_set_id, bin_years):
    """
    submit all the bundle draw estimate files by age/sex/year start value
    """
    iw = InpatientWrappers(run_id, "clinical")
    run_metadata = iw.pull_run_metadata()

    pickle_path = FILEPATH
    run_pickle = pd.read_pickle(pickle_path)
    verfiy_active_bundle_cf_version_diff(run_metadata)

    # get the age groups sexes and years we'll use to send out jobs.
    age_groups, sexes, years = get_demos(
        bin_years=bin_years,
        clinical_age_group_set_id=clinical_age_group_set_id,
        run_id=run_id,
    )

    # send out all the jobs
    [
        sbatcher(
            age_group=age_group,
            sex=sex,
            year=year,
            repo=REPO,
            run_id=run_id,
            draws=draws,
            bin_years=bin_years,
            queue=run_pickle.que,
        )
        for age_group in age_groups
        for sex in sexes
        for year in years
    ]
    # wait until they've finished
    general_purpose.job_holder(job_name="envunc", sleep_time=60)

    # check the files that were written against what we expected to write
    check_files(run_id, age_groups, sexes, years, bin_years)

    # # get the final files from the para jobs
    print("concatting all the final files back together")
    final_files = ob_file_getter(run_id, bin_years, for_reading=True)
    df = pd.concat([pd.read_csv(f) for f in final_files], sort=False, ignore_index=True)

    return df
