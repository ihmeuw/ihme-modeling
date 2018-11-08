"""
Script to run qsubs to collect "claude" data from CoD for use in Mortality data
prep / Empirical Deaths.
"""

import sys
import getpass
import pandas as pd
from db_queries import get_location_metadata
import time
import subprocess
import os
import re
from db_tools import ezfuncs
from itertools import product

sys.path.append("FILEPATH")

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders import (add_nid_metadata, get_datasets,
    get_current_location_hierarchy, get_country_level_location_id)
from cod_prep.utils import report_if_merge_fail


# globals
RUN_ID = sys.argv[1]  # run id for location to save
YEARS = range(1950, 2017+1)
TRY_AGAIN = True

def run_workers(year, run_id):
    """
    Fuction that submits 1 qsub for a year
    """
    
    # multiline string for human readability
    # uses the Unix $USER thing to find stuff
    qsub_str = """
    qsub
    -P proj_mort_prep
    -N cod_vr_{y}
    -pe multi_slot 2 
    -l mem_free=4g
    -e FILEPATH
    -o FILEPATH
    FILEPATH
    FILEPATH
    {y} {rid}
    """.format(y=year, rid=run_id)
    
    # get rid of extra whitespace / newlines for computer readability
    qsub_str = " ".join(qsub_str.split())
    
    # submit
    subprocess.call(qsub_str, shell=True)
    
    # that's all


def watch_jobs(years, data_dir):
    """
    Function that checks to see if all the outputs of the qsubs are present
    """
    
    # initialize lists
    expected = ["{}.csv".format(y) for y in years]
    found = [f for f in os.listdir(data_dir)]  # TODO filter on years incase there are other files?
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
    If this script had to resubmit jobs, and as already found all files
    to exist, then the original jobs that had to be resubmitted are
    probably still running. This function finds and deletes those jobs.
    
    Alternatively, one could just do a wild card qdel based on the job names,
    but that's probably bad / not nice to the cluster.
    """
    
    # submit qstat command to system
    p = subprocess.Popen("qstat", stdout=subprocess.PIPE)
    # get qstat text as a string
    qstat_str = p.communicate()[0]
    
    # split the string by lines
    qstat = qstat_str.splitlines()
    
    # get just the data rows
    qstat_header = qstat[0]  # keep the header row just incase we want it
    qstat = qstat[2:]  # drop header and line that is all dashes
    
    # split each row on whitespace, so that each row is a list of separate entries for one job
    qstat = [row.split() for row in qstat]
    
    # get a list of job_ids, filtering using a regex that finds job names
    # for this scripts jobs. Job names should be "cod_vr_" followed by 3 numbers
    # it's three numbers because qstat truncates job names
    # row[0] is job_id, row[2] is job name
    job_ids = [row[0] for row in qstat if re.search(pattern="cod_vr_\d{3}", string=row[2])]

    # submit qdel command for each job id. There's away to give qdel a list of job ids though.
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
    # sort em
    files = sorted(files)  
    
    # read and append each file
    for f in files:
        df = pd.read_csv("{}/{}.csv".format(data_dir, f))
        assert df.shape[0] > 0, "DataFrame {} has no data!".format(f)
        df_list.append(df)
        
    # Make one data frame
    df = pd.concat(df_list, ignore_index=True)

    # replace age_group_id -1 with 283
    df.loc[df.age_group_id == -1, 'age_group_id'] = 283
    
    return df


def get_additional_location_years():
    """
    Not using this function yet because as it turns out it might not be useful yet.
    """
    location_years = pd.read_excel("FILEPATH")
    
    # filter to just "cod_only"
    location_years = location_years[location_years._merge == "cod_only"]
    
    drop_rows = (location_years.ihme_loc_id == "MYS") & (location_years.year_id.isin([1980, 1981, 1982]))
    location_years = location_years[~drop_rows]
    
    # Mongolia source "Other_maternal"
    location_years = location_years[location_years.cod_source != "Other_Maternal"]
    
    # West Bank Palestine in the late 90s / early 2000s
    drop_rows = (location_years.location_id == 149) & (location_years.year_id.isin([1997, 1998, 1999, 2001, 2005])) & (location_years.cod_source.str.contains("West Bank"))
    location_years = location_years[~drop_rows]
    
    location_years = location_years[~location_years.cod_source.str.contains("MCCD")]
    
    drop_rows = (location_years.location_id == 6) & (location_years.year_id.isin([1980, 1982, 1983, 1984, 1985, 1990]))
    location_years = location_years[~drop_rows]
    
    # just need the two columns:
    location_years = location_years[['location_id', 'year_id']]
    
    return location_years


def get_location_years():
    # make dataframe of location years that were in cod_vr handoffs
    original_location_years = pd.read_stata("FILEPATH",
                                       columns=['location_id', 'year']).drop_duplicates().\
                                       rename(columns={"year": "year_id"})
    
    # addtional loc years that have been added recently and would not be in the handoffs
    new = pd.read_excel("FILEPATH")
    new = new[new.nid.notnull()]
    new_nids = list(new.nid.unique())
    df_list = []
    for nid in new.nid.unique():
        df = get_datasets(nid=nid,
                          is_active=None,
                          need_data_drop_status=False, need_nr_model_group=False, need_survey_type=False)
        df = df[['location_id', 'year_id']]
        df_list.append(df)
    new_location_years = pd.concat(df_list, ignore_index=True)

    ## keep 2015/2016 data for Scotland (since these location/years aren't in the other files)
    new_scotland = pd.DataFrame({"location_id": 434, "year_id": [2015, 2016]})

    ## keep Saudi Arabia data 
    saudi_arabia = pd.DataFrame({"location_id": 152, "year_id": list(range(1999, 2012+1))})

    ## get 2012-2016 Russian subnationals
    locs_metadata = get_location_metadata(location_set_id=82)
    rus_locs = list(locs_metadata[locs_metadata.path_to_top_parent.str.contains(",62,")].location_id.unique())
    rus_years = [2012, 2013, 2014, 2015, 2016]
    rus_location_years = pd.DataFrame(list(product(rus_locs, rus_years)), columns=["location_id", "year_id"])

    ## get 2015 South Africa
    zaf_locs = list(locs_metadata[locs_metadata.path_to_top_parent.str.contains(",196,")].location_id.unique())
    zaf_location_years = pd.DataFrame({"location_id": zaf_locs, "year_id": 2015})

    ## get sweden subnationals: Stockholm and Sweden w/o Stockholm
    swe_locs = list(locs_metadata[locs_metadata.path_to_top_parent.str.contains(",93,")].location_id.unique())
    swe_location_years = pd.DataFrame({"location_id": swe_locs, "year_id": 2016})

    # get norway subnationals 1969-1985
    nor_locs = list(locs_metadata.loc[locs_metadata.path_to_top_parent.str.contains(",90,")].location_id.unique())
    nor_years = range(1969, 1985 + 1)
    nor_location_years = pd.DataFrame(list(product(nor_locs, nor_years)), columns=["location_id", "year_id"])

    # get mex subnationals 2016
    print("Getting mex_locs_years...")
    mex_locs = list(locs_metadata[locs_metadata.path_to_top_parent.str.contains(",130,")].location_id.unique())
    mex_locs_years = pd.DataFrame({"location_id": mex_locs, "year_id": 2016})


    # get Netherlands 2016
    nld_location_years = pd.DataFrame({"location_id": [89], "year_id": [2016]})

    location_years = pd.concat(
        [new_location_years,
         original_location_years,
         new_scotland,
         saudi_arabia,
         rus_location_years,
         zaf_location_years,
         swe_location_years,
         nor_location_years,
         nld_location_years,
         mex_locs_years,
         ],
         ignore_index=True)
    location_years = location_years.drop_duplicates()

    assert location_years.shape[0] > 0, "DataFrame contains no data!"
    return location_years


def filter_by_location_and_year(df, location_years):
    """
    Filters data that was pulled to just be the location-years that used to be in cod handoffs.
    """

    pre = df.shape[0]
    
    # filter to just the location_years in cod handoffs
    df = pd.merge(left=df, right=location_years, how='left', on=['location_id', 'year_id'], indicator=True)
    df['_merge'] = df._merge.astype(str)
    # since df is on the left, "left_only" means location years that are in COD VR that we don't want
    # "both" would be country years that are in cod VR and in the location years that we do want
    # "right_only" would be impossible in this kind of merge.
    df = df[df._merge != "left_only"]
    df = df.drop("_merge", axis=1)

    post = df.shape[0]

    msg = """
    Right now the data has {} rows. After filtering it has {} rows.
    """.format(pre, post)

    print(msg)

    return df


def filter_duplicates(df):
    
    # before the other filters we want to take care of singapore
    # because if not, we'll end up with mixed source age patterns!
    sgp = df[df.location_id == 69].copy()
    df = df[df.location_id != 69]
    sgp = sgp[sgp.source.isin(["Singapore_MoH_ICD9_detail", "Singapore_MoH_ICD10"])]
    df = pd.concat([df, sgp], ignore_index=True)
    
    # before the other filters we want to take care of Norway,
    # because prior testing showed that the later drops remove all Norway (90) 1951-1979
    nor = df[df.location_id == 90].copy()  # national level
    df = df[df.location_id != 90]
    nor = nor[nor.source.isin(['NOR_collab_counties_ICD7',  # keep the sources we want
                               'NOR_collab_counties_ICD8_detail'])]
    df = pd.concat([df, nor], ignore_index=True)  # put it back
    
    # another special case, Ukraine. UKR_databank_ICD10_tab is prefered over ICD10_tabulated,
    # even though UKR_databank_ICD10_tab is NOT active (at this time). So,
    # we want to drop ICD10_tabulated in the years where both sources are present
    drop_rows = (df.location_id == 63) & (df.year_id.isin(range(2005, 2013+1))) & (df.source == "ICD10_tabulated")
    df = df[~drop_rows]
    
    # take note of the unique location year age sex we started with
    id_cols = ['location_id', 'sex_id', 'age_group_id', 'year_id']
    starting_ids = df[id_cols].drop_duplicates()
    assert starting_ids.notnull().all().all(), "For a test later there can't be nulls"
    
    # Main duplication drop here!!!
    starting_shape = df.shape[0]
    # Identify rows that are dupilcates on location, year, age, and sex
    # I think it is fair to keep the ones that are set to active
    dups = df.duplicated(subset=['location_id', 'year_id', 'sex_id', 'age_group_id'], keep=False)
    # now, identify rows that are NOT active AND are duplicates
    drop_rows = (dups) & (df.is_active==0)
    df = df[~drop_rows]
    del dups, drop_rows

    # site_id de-duplication here
    # Identify and remove rows that are duplicates among the columns
    # age_group_id, year_id, sex_id, location_id, deaths
    site_id_dups = df.duplicated(subset=["age_group_id", "year_id", "sex_id",
                                         "location_id", "deaths"], keep=False)
    # I expect that there's just one source in these duplicates:
    assert len(df[site_id_dups].source.unique()) == 1
    # I expect that site_id is what distinguishes these rows, so there
    # should be zero duplicates onces site_id is included
    assert_msg = """Expectations not met! Thought that there was one
                    source with duplicate values distinguished by site_id"""
    assert df.duplicated(subset=["age_group_id", "year_id", "sex_id",
                                 "location_id", "deaths", "site_id"],
                         keep=False).sum() == 0, " ".join(assert_msg.split())
    # select everything from this source
    source = df[site_id_dups].source.unique()[0]
    duplicated_df = df[df.source == source]
    df = df[df.source != source]
    duplicated_df['site_id'] = 2
    group_cols = [col for col in duplicated_df.columns if col not in ["deaths"]]
    duplicated_df = duplicated_df.groupby(by=group_cols, as_index=False).deaths.sum()
    df = pd.concat([df, duplicated_df], ignore_index=True)
    del duplicated_df, site_id_dups
    
    # merge the deduplicated data back onto the beginning id values
    # to check if any are missing
    test = pd.merge(left=starting_ids, right=df[['location_id', 'sex_id', 'age_group_id', 'year_id', 'nid']].drop_duplicates(),
                    how="left", on=id_cols)
    if test.isnull().any().any():
        locs = list(test[test.isnull().any(axis=1)].location_id.unique())
        yrs = list(test[test.isnull().any(axis=1)].year_id.unique())
        assert test.notnull().all().all(), "The locations {} and the years {} went missing".format(", ".join(map(str, locs)), ", ".join(map(str, yrs)))

    print("Started with {} rows. Finished with {} rows. Removed {} rows.".
          format(starting_shape, df.shape[0], starting_shape-df.shape[0]))
    return df


def aggregate_under_one(df):
    """
    Certain sources /  country-years have both age_group_id 28 and 2, 3, 4 contains deaths.
    That is, 28 having deaths that 2, 3, 4 do not have and vice versa.  These should be added
    together

    For now, there are three countries we want to do this:
    Scotland, Venezuela, and Hungary, in certain years.
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

    # collapse
    group_cols = [col for col in df.columns if col not in ["deaths"]]
    df = df.groupby(by=group_cols, as_index=False).deaths.sum()

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


def main(years, qsub_out_dir, run_id, try_again=False):
    """
    This function runs all the other functions.
    """
    print("Submitting jobs...")
    for year in years:
        run_workers(year=year, run_id=run_id)
    print("Done submitting.")

    # wait for 20 minutes while checking for files
    print("Checking for files...")
    not_found = watch_jobs(years=years,
                           data_dir=qsub_out_dir)

    # wait for all files to appear and maybe relaunch if they do not
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
    # of this master script, this means that the run all scipt won't find
    # the output file and will raise its own assertion error.
    assert len(not_found) == 0, "Not all files present, still missing {}".format(not_found)
    
    # still want to sleep some more incase some files are still writing
    time.sleep(30)
    
    # delete the remaining jobs
    print("Deleting remaining obsolete jobs...")
    qdel_obsolete_jobs()
   
    print("Collecting job outputs...")
    data = collect_qsub_results(data_dir=qsub_out_dir)

    # filter down to just the location_years we want
    location_years = get_location_years()
    data = filter_by_location_and_year(data, location_years)
    
    # add nid metadata
    data = add_nid_metadata(df=data, add_cols=['source', 'is_active'], force_rerun=False, cache_dir='standard')
    
    # filter out duplicates
    data =  filter_duplicates(data.copy())

    # aggregate under one for certain loc-years
    data = aggregate_under_one(data)

    # check that there isn't any All Cause VR in the data
    assert_msg = ("There is all cause VR in the data; "
                  "This will lead to duplicates later in the process")
    assert (data.extract_type_id != 167).all(), assert_msg
    print("Done!")

    for col in ['sex_id', 'age_group_id', 'year_id', 'location_id']:
        data[col] = data[col].astype(int)

    data = data.drop(['extract_type_id', 'site_id', 'is_active'], axis=1)

    return data


if __name__ == "__main__":

    # set up final outpur/save location
    out_dir = ("FILEPATH".
               format(RUN_ID))
    out_filename = "claude_data"
    outfile = "{}/{}.csv".format(out_dir, out_filename)

    # intermediate file output/save location
    qsub_out_dir = ("FILEPATH")
    if not os.path.isdir(qsub_out_dir):
        # NOTE that this only makes the "right most" directory,
        # i.e., "get_cod_data_files"
        os.mkdir(qsub_out_dir)

    # run stuff
    data = main(years=YEARS, qsub_out_dir=qsub_out_dir,
                run_id=RUN_ID, try_again=TRY_AGAIN)

    # save
    data = prepare_and_save_cod_data(df=data, save_filepath=outfile)
