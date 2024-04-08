"""
Script to run qsubs to collect "claude" data from CoD for use in Mortality data
prep / Empirical Deaths.
"""

import sys
import getpass
import pandas as pd
import time
import subprocess
import os
import re
from db_tools import ezfuncs

# cod-data repo dir
sys.path.append("".format(getpass.getuser()))
from cod_prep.downloaders import add_nid_metadata

# globals
RUN_ID = sys.argv[1]  # run id for location to save
GBD_YEAR = int(sys.argv[2])
YEARS = range(1950, GBD_YEAR+1)
TRY_AGAIN = True


def run_workers(year, run_id):
    """
    Fuction that submits 1 qsub for a year
    """

    # multiline string for human readability
    # uses Unix $HOME and $USER  to find stuff
    qsub_str = """
    sbatch
    -A proj_mortenvelope
    -J cod_vr_{y}
    -c 2
    --mem=8G
    -t 1:00:00
    -p long.q
    -e 
    -o 
    $
    $
    {y} {rid}
    """.format(y=year, rid=run_id, user = getpass.getuser())

    # get rid of extra whitespace / newlines for computer readability
    qsub_str = " ".join(qsub_str.split())

    # submit
    subprocess.call(qsub_str, shell=True)


def watch_jobs(years, data_dir):
    """
    Function that checks to see if all the outputs of the qsubs are present
    """

    # initialize lists
    expected = ["{}.csv".format(y) for y in years]
    found = [f for f in os.listdir(data_dir)]
    not_found = list(set(expected) - set(found))

    # initialize times
    time_limit = 60 * 20
    time_elapsed = 0
    start_time = time.time()

    # loop and check
    while len(not_found) != 0 and time_elapsed < time_limit:
        found = [f for f in os.listdir(data_dir)]
        not_found = list(set(expected) - set(found))

        # update time elapsed
        time_elapsed = int(time.time() - start_time)

        # print updates on what is missing every 30 seconds
        if time_elapsed % 30 == 0:
            print("Not all files present; Waiting on {} files. It's been {} "
                  "minutes.".format(len(not_found), time_elapsed / 60.0))
        time.sleep(15)  # wait 15 seconds and then proceed

    if len(not_found) > 0:
        print("Not all files were found after waiting for {} minutes. "
              "Here are the missing files:\n{}".format(time_limit / 60,
                                                       not_found))
    else:
        print("All files found.")
    return not_found


def qdel_obsolete_jobs():
    """
    The main() function will resubmit jobs if they take too long. That's useful
    when the cluster is really really busy, because sometimes a job will just
    hang until a node clears up. So resubmitting helps. But resubmitting a job
    without deleting the old job is dangerous since there's a change that both
    jobs end up writing to the same file.

    This function deletes jobs whose year_id has been resubmitted as a new job.

    Alternatively, one could just do a wild card qdel based on the job names,
    but that's probably bad / not nice to the cluster.
    """

    # submit qstat command to system
    p = subprocess.Popen("squeue", stdout=subprocess.PIPE)
    # get qstat text as a string
    qstat_str = p.communicate()[0]

    # split the string by lines
    qstat = qstat_str.splitlines()

    # get just the data rows
    qstat = qstat[2:]  # drop header and line that is all dashes

    # split each row on whitespace, so that each row is a list of
    # separate entries for one job
    qstat = [row.split() for row in qstat]

    # get a list of job_ids, filtering using a regex that finds job names
    # for this scripts' jobs. Job names should be "cod_vr_" followed by 3
    # numbers. it's three numbers because qstat truncates job names
    # row[0] is job_id, row[2] is job name
    job_ids = [row[0] for row in qstat if re.search(pattern="cod_vr_\d{3}", string=row[2].decode("utf-8") )]

    # submit qdel command for each job id. There's away to give qdel a list
    # of job ids though.
    for job in job_ids:
        subprocess.call("qdel {}".format(job), shell=True)


def collect_qsub_results(data_dir):
    """
    Function that collects the outputs of the qsubs.
    """

    print("Collecting data for all years...")

    # initialize empty list
    df_list = []

    # get all the files in the directory
    files = os.listdir(data_dir)
    # keep only those that are CSVs
    files = [f for f in files if os.path.splitext(f)[-1].lower() == ".csv"]
    # remove extension so we can insert active_name
    files = [os.path.splitext(f)[0] for f in files]
    # sort
    files = sorted(files)

    # read and append each file
    for f in files:
        df = pd.read_csv("".format(data_dir, f))
        assert df.shape[0] > 0, "DataFrame {} has no data!".format(f)
        df_list.append(df)

    # Make one data frame
    df = pd.concat(df_list, ignore_index=True)

    # replace age_group_id -1 with 283
    df.loc[df.age_group_id == -1, 'age_group_id'] = 283

    return df


def aggregate_under_one(df):
    """
    Certain sources /  country-years have both age_group_id 28 and 2, 3, 4
    containing deaths. That is, 28 has deaths that 2, 3, 4 do not have and
    vice versa. These should be added together

    For now, there are three countries we want to do this too; Scotland, 
    Venezuela, and Hungary, in certain years.
    """

    # separate into dataframes to keep things explicit:
    # Scotland 1970-1973
    df.loc[
        (df.location_id == 434) & (df.year_id.isin([1970, 1971, 1972, 1973])) &  # select scotland
        (df.age_group_id.isin([2, 3, 4])),  # select neonatals
        "age_group_id"  # target column
    ] = 28

    # Venezuela 1968
    df.loc[
        (df.location_id == 133) & (df.year_id == 1968) &  # select VEN
        (df.age_group_id.isin([2, 3, 4])),  # select neonatals
        "age_group_id"  # target column
    ] = 28

    # hungary 1970-1974
    df.loc[
        (df.location_id == 48) & (df.year_id.isin([1969, 1970, 1971, 1972, 1973, 1974])) &  # select HUN
        (df.age_group_id.isin([2, 3, 4])),  # select neonatals
        "age_group_id"  # target column
    ] = 28

    # Need to fill NaNs before collapse so they don't get dropped
    df.loc[df.underlying_nid.isnull(), "underlying_nid"] = "--filled-na--"

    # collapse
    group_cols = [col for col in df.columns if col not in ["deaths"]]
    assert_msg = "There are Nulls present in the grouping key columns; These rows with Nulls will be dropped.\n{}".format(df[group_cols].isnull().sum().to_string())
    assert df[group_cols].notnull().all().all(), assert_msg
    df = df.groupby(by=group_cols, as_index=False).deaths.sum()

    # Want to put the NaNs back
    df.loc[df.underlying_nid == "--filled-na--", "underlying_nid"] = pd.np.nan

    return df


def prepare_and_save_cod_data(df, save_filepath):
    """
    Function that does any last formatting to the data and saves it
    """
    for col in ['sex_id', 'age_group_id', 'year_id', 'location_id']:
        df[col] = df[col].astype(int)
    df.loc[df.age_group_id == -1, 'age_group_id'] = 283
    df.to_csv(save_filepath, index=False)
    print("Saved to {}".format(save_filepath))
    return df


def drop_overlapping_cod_years(df):
    """
    This function drops overlapping location-years of data that couldn't be
    filtered out with is_mort_active.

    There can be many location-years associated with a nid-extract_type_id pair.
    Some times we want to use some location-years from one nid-extract_type_id
    pair and other location-years from another nid-extract_type_id pair. 
    In these cases we can't depend on is_mort_active alone. 
    This function addresses that limitation. We have to pull overlapping 
    location-years (all of which have is_mort_active set to 1)
    and in this function drop the overlap.
    """

    # Ukraine 2013
    # Have overlapping sources for the national level Ukraine, location_id 63
    # build mask that identifies data to be dropped:
    drop_rows = (df.location_id == 63) & (df.year_id == 2013) & (df.nid == 333806)
    # drop the unwanted UKR 2013 data:
    df = df[~drop_rows].copy()

    # Ukraine (without Crimea & Sevastopol) UKR_50559 2015 & 2016
    # Have overlapping sources for this subnational location in Ukraine
    # build mask that identifies data to be dropped:
    drop_rows = (df.location_id == 50559) & (df.year_id.isin([2015, 2016])) & (df.nid == 333806)
    # drop the unwanted UKR_50559 2015 & 2016 data:
    df = df[~drop_rows].copy()


    return df


def get_code_ids(run_id):
    
    code_ids = ezfuncs.query(
    """QUERY""", conn_def="engine")
    # Output so we can read in parallel
    code_ids.to_csv("".format(run_id), index=False)
    return None


def bugfix_replace_bad_age_groups(df):
    """
    This function fixes an age group: it replaces age_group_id 236 with
    age_group_id 1.

    Age_group_id 236 doesn't split correctly.
    Demo:

        In [1]: from hierarchies.dbtrees import agetree

        In [2]: agetree(236).leaves()
        Out[2]: [2, 3, 4]

        In [3]: agetree(1).leaves()
        Out[3]: [2, 3, 4, 5]

    age_group_id 236 maps to just the neonatal ages, and not to anything
    representing ages 1-4 years old.

    Since age_group_id 1 does map the way desired this function replaces 236
    with 1.

    Returns:
        df (Pandas DataFrame): Data with age_group_id 1 instead of 236
    Raises: AssertionError if the death total changes at all
    """

    print("Running bugfix function bugfix_replace_bad_age_groups()...")

    starting_deaths = df.deaths.sum()  # save for later testing.

    source_msg = """
    These sources have age_group_id 236, which will be replaced with age_group_id 1: {}
    """.format(df[df.age_group_id == 236].source.unique().tolist())
    print(source_msg)


    # replace
    df.loc[df.age_group_id == 236, "age_group_id"] = 1

    # check
    assert 236 not in df.age_group_id.unique(), "236 did not get replaced"
    assert df.deaths.sum() == starting_deaths, "Somehow deaths changed by running bugfix_replace_bad_age_groups()"

    return df



def main(years, qsub_out_dir, run_id, try_again=False):
    """
    This function runs all the other functions.
    """

    # First, run the engine room query that was running in parallel previously.
    get_code_ids(run_id)

    # Submit the worker scripts - no need to throttle
    print("Submitting jobs...")
    for year in years:
        run_workers(year=year, run_id=run_id)
    print("Done submitting.")

    # wait while checking for files
    print("Checking for files...")
    not_found = watch_jobs(years=years,
                           data_dir=qsub_out_dir)

    # wait for all files to appear and relaunch if they do not
    # if not all the file were found, and we want to try again,
    # delete the remaining jobs, and resubmit them.
    # deleting and THEN resubmitting is important, because
    # it prevents having two jobs altering the same file.
    if len(not_found) > 0 and try_again:
        print("Didn't find all files on the first try; Trying again...")

        # delete the remaining jobs
        print("Deleting remaining obsolete jobs...")
        qdel_obsolete_jobs()
        time.sleep(30)

        print("Re-submitting jobs that haven't yet completed...")
        for year in [x[:4] for x in not_found]:  # grab years from list of unfinished jobs
            run_workers(year=year, run_id=run_id)
        print("Done re-submitting.")

        print("Checking for files...")
        not_found = watch_jobs(years=years,
                               data_dir=qsub_out_dir)
    # This will end the code from running. Within the context of the
    # empirical deaths run_all system, which will be checking for the output
    # of this launcher script, this means that the run all scipt won't find
    # the output file and will raise its own assertion error.
    assert len(not_found) == 0, "Not all files present, still missing {}".format(not_found)

    # still want to sleep some more incase some files are still writing
    time.sleep(30)

    # delete the remaining jobs
    print("Deleting remaining obsolete jobs...")
    qdel_obsolete_jobs()

    print("Collecting job outputs...")
    data = collect_qsub_results(data_dir=qsub_out_dir)

    data = add_nid_metadata(df=data, add_cols=['source', 'parent_nid'], force_rerun=True, cache_dir='standard')

    # drop unneeded columns. Can't drop this column until after add_nid_metadata
    # runs because it's needed for that function
    data = data.drop('extract_type_id', axis=1)

    # In situations where there is a parent_nid, we want to make it an underlying nid.
    # When there is an underlying nid, the main NID represents something like
    
    # The underlying_nid for this NID would represent a specific country-year that is a part
    # of this WHO Database version. When there is not an underlying_nid, the NID
    # is a specific country-year source. That is, an NID can represent two types of things,
    # it can be a specific country year source, or it can be a source that covers many
    # country years.  An underlying_nid, when there is one, will always be a specific country year.
    # so we have to make an underlying_nid column, and do some swapping of nids.
    # This is a different paradigm than how CoD stores NID data, where NID is always the most
    # detailed country-year and parent_nid would represent the larger source.
    # when there is a parent_nid, it should get placed into the nid column,
    # and what was in the nid column, should be placed into the underlying_nid column

    # this is how CoD Does it:
    data.loc[data.parent_nid.notnull(), ['nid', 'parent_nid']] = data.loc[data.parent_nid.notnull(), ['parent_nid', 'nid']].values
    # now that parent_nid matches our definition of underlying_nid it can be renamed
    data = data.rename(columns={"parent_nid": "underlying_nid"})

    # aggregate under one for certain loc-years
    data = aggregate_under_one(data)

    # drop overlapping location-years
    data = drop_overlapping_cod_years(data)

    # bugfix for age_group_id 236 to replace it with age_group_id 1
    data = bugfix_replace_bad_age_groups(data)

    # age group id 161 (age group 0) should be coded as 28 (<1 year)
    data.loc[data.age_group_id == 161, "age_group_id"] = 28

    # check for duplicates
    duplicated = data.duplicated(subset=['sex_id', 'age_group_id',
                                          'location_id', 'year_id'],
                                keep=False)
    assert not duplicated.any(), "There are duplicates."

    for col in ['sex_id', 'age_group_id', 'year_id', 'location_id']:
        data[col] = data[col].astype(int)

    # add needed columns
    # need to add a source_type_id column
    # see 
    # for a list of source types
    # for most of the cod branch of the empirical deaths process this column is
    # a place holder it gets replaced with a merge inside the script
    # combine_cod_noncod.do.
    data["source_type_id"] = 1

    # age group id 49 should be coded as 238
    data.loc[data.age_group_id == 49, 'age_group_id'] = 238

    return data


if __name__ == "__main__":

    # set up final outpur/save location
    out_dir = ("".
               format(RUN_ID))
    out_filename = ""
    outfile = "".format(out_dir, out_filename)

    # intermediate file output/save location
    qsub_out_dir = (""
                    "".format(RUN_ID))
    if not os.path.isdir(qsub_out_dir):
        os.mkdir(qsub_out_dir)

    # run stuff
    data = main(years=YEARS, qsub_out_dir=qsub_out_dir,
                run_id=RUN_ID, try_again=TRY_AGAIN)

    # save
    data = prepare_and_save_cod_data(df=data, save_filepath=outfile)
