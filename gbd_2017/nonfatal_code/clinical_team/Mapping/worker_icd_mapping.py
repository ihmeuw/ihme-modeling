import pandas as pd
import numpy as np
import platform
import datetime
import re
import warnings
import getpass
import sys
import subprocess

if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

# load our functions
prep_path = r"FILEPATH"
sys.path.append(prep_path)
repo = r"FILEPATH"


import hosp_prep

# first system arg, the data source
source = sys.argv[1]
# second system arg, whether it wants the deaths master data
deaths = sys.argv[2]
# third arg, whether to write a log
write_log = sys.argv[3]

if source == "":
    source = "IDN_SIRS"

def use_special_map(data, map_path, sheetname, asource):
        """
        input data that needs to be mapped with a special map.
        Things like aggregated ICD codes, or non ICD codes.  This function
        requires that the map be in an excel spreadsheet.

        Call this function once per source.

        Parameters:
            data: Pandas DataFrame
                data that needs special mapping
            map_path: string
                path to special map
            sheetname: string
                sheet of map to use.
            asource: name of the source that needs special mapping.
        """
        print("Formatting the special map for {}...".format(asource))
        # read in the special map
        spec_map = pd.read_excel(map_path, sheetname=sheetname)

        if "indonesia" in map_path:
            spec_map.rename(columns={'subnational_icd_code': 'cause_code'},
                            inplace=True)
            spec_map['code_system_id'] = 2

        if asource == "VNM_MOH":
            spec_map['code_system_id'] = 2
            spec_map['cause_code'] = spec_map['cause_code'].astype(str)

        # check for baby sequelae column with the correct name
        assert 'nonfatal_cause_name' in spec_map.columns, ("spec_map is"
            " missing the column 'nonfatal_cause_name'")

        # keep cols of interest
        spec_map = spec_map[['cause_code', 'code_system_id',
                             'nonfatal_cause_name']].copy()

        # remove non alphanumeric chars from icd codes
        if asource != "GEO_COL_00_13":
            spec_map['cause_code'] =\
                spec_map['cause_code'].str.replace("\W", "")

        # convert to string        
        spec_map['cause_code'] = spec_map['cause_code'].astype(str)

        # make baby sequelae lower case
        spec_map['nonfatal_cause_name'] =\
            spec_map['nonfatal_cause_name'].str.lower()

        # check map icd codes against data icd codes
        set(data.cause_code).symmetric_difference(set(spec_map.cause_code))

        # merge the special map onto the subset of data
        pre = data.shape[0]
        merged_subset = data.merge(spec_map, how='left',
                                   on=["cause_code", "code_system_id"])
        assert merged_subset.shape[0] == pre, ("Shape changed during "
            "merge")

        assert merged_subset.nonfatal_cause_name.isnull().sum() == 0, (
            "the specially mapped data has nulls in the column "
            "'nonfatal_cause_name' after merge")

        return(merged_subset)


def extract_primary_dx(df):
    """
    creates a new column with just the primary dx when multiple dx have been
    combined together (this happens often in the phil health data
    """
    # split the icd codes on letter
    df['split_code'] = df.cause_code.map(lambda x: re.split("([A-Z])", x))
    # keep primary icd code
    df['split_code'] = df['split_code'].str[0:3]
    df['split_code'] = df['split_code'].map(lambda x: "".join(map(str, x)))
    return(df)


def map_to_truncated(df, maps):
    """
    Takes a dataframe of icd codes which didn't map to baby sequela
    merges clean maps onto every level of truncated icd codes, from 6
    digits to 3 digits when needed. returns only the data that has
    successfully mapped to a baby sequelae.
    """
    df_list = []
    for i in [7, 6, 5, 4]:
        df['cause_code'] = df['split_code'].str[0:i]
        good = df.merge(maps, how='left', on=['cause_code',
                                              'code_system_id'])
        # keep only rows that successfully merged
        good = good[good['nonfatal_cause_name'].notnull()]
        # drop the icd codes that matched from df
        df = df[~df.cause_code.isin(good.cause_code)]
        df_list.append(good)

    dat = pd.concat(df_list)
    return(dat)


def read_clean_map():
    """
    Read in the clean map file and do some light prepping to get it ready
    to merge onto the data

    Returns:
        Pandas DataFrame containing the map
    """
    # read in clean map
    maps_path = r"FILEPATH"
    maps = pd.read_csv(maps_path, dtype={'cause_code': object})
    assert hosp_prep.verify_current_map(maps)

    map_version = int(maps['map_version'].unique())
    # cast to string
    maps['cause_code'] = maps['cause_code'].astype(str)
    maps['cause_code'] = maps['cause_code'].str.upper()

    # keep relevant columns
    maps = maps[['cause_code', 'nonfatal_cause_name',
                     'code_system_id']].copy()

    # drop duplicate values
    maps = maps.drop_duplicates()
    return maps, map_version


def prep_df(df):
    """
    Returns:
        Pandas DataFrame with ICD codes cast to type str and all uppercase
    """
     # make sure that cause_code is a string. this is a redundancy/contingency
    assert "cause_code" in df.columns, "cause_code missing from columns"

    df['cause_code'] = df['cause_code'].astype(str)
    # match on upper case
    df['cause_code'] = df['cause_code'].str.upper()
    return df


def merge_and_check(df, maps, good_map=True):
    """
    This is the standard method for merging baby sequelae onto ICD codes,
    checks to make sure that # of rows don't change and
    icd codes haven't changed either
    """
    # store variables for data check later
    before_values = list(df['cause_code'].unique())  # values before
    # merge the hospital data with excel spreadsheet maps
    pre = df.shape[0]
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])
    if good_map:
        dupes = maps[maps.duplicated(subset=['cause_code', 'code_system_id'], keep=False)].sort_values('cause_code')
        assert pre == df.shape[0],\
            "Number of rows have changed from {} to {}. "\
            "Probably due to these duplicated codes".format(\
                pre, df.shape[0], dupes)

    # this check needs to run after the merge but before we alter the icd
    # codes using the methods below.
    after_values = list(df['cause_code'].unique())  # values after
    # assert various things to verify merge
    assert set(before_values) == set(after_values), "Unique ICD codes are not the same"
    return df


def truncate_and_map(df, maps, map_version, write_unmapped=True):
    """
    Performs the actual extraction and truncation of ICD codes when they fail to map
    Writes a list of completely unmapped ICD codes to /test_and_sample_data along
    with the version of the map used
    """
    # return the df if it's just 3 codes long
    if df['cause_code'].apply(len).max() <= 3:
        return df
    # or if it's empty
    if df.shape[0] == 0:
        return df

    pre = df.shape[0]
    pre_cases = df.val.sum()
    # create a raw cause code column to remove icd codes that were
    # fixed and mapped successfully
    df['raw_cause_code'] = df['cause_code']

    for code_sys in df.code_system_id.unique():
        # get just the codes from 1 code system that didn't map
        remap = df[df.code_system_id == code_sys].copy()

        # create remap_df to retry mapping for icd n
        remap.drop('nonfatal_cause_name', axis=1, inplace=True)

        # take the first icd code when they're jammed together
        remap = extract_primary_dx(remap)

        # now do the actual remapping, losing all rows that don't map
        remap = map_to_truncated(remap, maps)
        # remove the rows where we were able to re-map to baby sequela
        df = df[~df.raw_cause_code.isin(remap.raw_cause_code)]

        # bring our split data frames back together
        df = pd.concat([df, remap])

    print(df)
    if 'split_code' in df.columns:
        # drop cols used to truncate and split
        df.drop(['raw_cause_code', 'split_code'], axis=1, inplace=True)

    assert pre == df.shape[0],\
        "Rows have changed from {} to {}".format(pre, df.shape[0])
    assert pre_cases == df.val.sum(),\
        "Cases have changed from {} to {}".format(pre_cases, df.val.sum())

    if write_unmapped:
        # write the data that didn't match for further inspection
        df[df['nonfatal_cause_name'].isnull()].\
            to_csv(r"FILEPATH/"
                   r"icds_not_in_map{}_in_{}.csv".format(root, map_version, source), index=False)
    return df


def path_finder(df, start_cases, source, maps):
    """
    There are 3 different types of mapping. This function
    determines which kind of mapping is the right path to take by detecting
    the source.  If a path that requires a special mapping is detected,
    use_special_map is called on the data.
    """
    print("beginning to merge on baby sequelae. {} rows and {} diagnoses".format(\
        df.shape[0], df.val.sum()))

    # use special map for Indonesia
    if source == "IDN_SIRS":
        # run special mapping.
        df = use_special_map(df,
                             map_path=(r"FILEPATH",
                             sheetname='indonesia_data_icd_codes',
                             asource='IDN_SIRS')

    # use special map for Vietnam
    elif source == "VNM_MOH":
        df = use_special_map(df,
                            map_path=(r"FILEPATH",
                            sheetname='TQ',
                            asource='VNM_MOH')
    elif source == "GEO_COL_00_13":
        df = use_special_map(df,
                             map_path="FILEPATH",
                             sheetname="Sheet1",
                             asource="GEO_COL_00_13")
    else:
        df = merge_and_check(df, maps)

    half_per = df.val.sum() * .005
    assert (abs(start_cases - df.val.sum()) < half_per),\
        "There was a change in the number of cases greater than 0.5 %"

    return df


def grouper(df):
    """
    set everything that's not an explicit "death2" (gbd2015 deaths) to case
    then groupby and sum val. We have four outcome types leading up to this
    function depending on when the data was formatted.
    
    """
    print("performing groupby and sum")

    df.loc[df['outcome_id'] != 'death2', 'outcome_id'] = 'case'

    groups = ['location_id', 'year_start', 'year_end', 'age_group_unit',
              'age_group_id', 'sex_id', 'source', 'nid',
              'facility_id', 'representative_id', 'diagnosis_id',
              'metric_id', 'outcome_id', 'nonfatal_cause_name']
    df = df.groupby(groups).agg({'val': 'sum'}).reset_index()

    return df


def dtypecaster(df):
    """
    make sure the datatype of every column is what we expect
    """
    # set the col types
    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_group_id', 'sex_id', 'nid', 'representative_id',
                'diagnosis_id', 'metric_id']
    float_cols = ['val']
    str_cols = ['source', 'facility_id', 'outcome_id', 'nonfatal_cause_name']

    # do the col casting
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')
    for col in str_cols:
        df[col] = df[col].astype(str)

    return df


def worker_mapping(df, write_log=False):
    """
    Main Function.  Maps from ICD codes to Nonfatal Cause Name.

    Args:
        df: (Pandas DataFrame) contains ICD level data
        write_log: (bool) if True will write a txt file at
            FILEPATH with information about the number of
            rows that didn't get mapped.

    Returns:
        A Pandas DataFrame mapped and collapsed to Nonfatal Cause Name.
    """
    # get the raw diagnoses counts to start
    start_cases = df.val.sum()

    print("beginning on source {}".format(source))

    # get the map and prep the df
    maps, map_version = read_clean_map()
    df = prep_df(df)


    df = path_finder(df, start_cases, source, maps)

    # we want only rows where nfc is null to try to truncate and map
    no_map = df[df['nonfatal_cause_name'].isnull()].copy()
    # keep only rows in df where nfc is not null
    df = df[df['nonfatal_cause_name'].notnull()].copy()

    # truncate and map again
    no_map = truncate_and_map(df=no_map, maps=maps, map_version=map_version)

    # bring them back together
    df = pd.concat([df, no_map], ignore_index=True)

    # count how many rows didn't map, ie have a null baby seq:
    no_match_count = float(df['nonfatal_cause_name'].isnull().sum())
    no_match_per = round(no_match_count/df.shape[0] * 100, 4)
    print(r"{} rows did not match a baby sequela in the map out of {}.").\
        format(no_match_count, df.shape[0])
    print("This is {}% of total rows that did not match".format(no_match_per))

    if maps.nonfatal_cause_name.unique().size > 500:
        if no_match_per < 15:
            warnings.warn(r"15% or more of rows didn't match")

    if write_log:
        print("Writing Log file")

        text = open(root + "FILEPATH" +
                    re.sub("\W", "_", str(datetime.datetime.now())) +
                    "_icd_mapping_output.txt", "w")
        text.write("""
                   Data Source: {}
                   Number of unmatched rows: {}
                   Total rows of data: {}
                   Percent of unmatched:  {}
                   """.format(df.source.unique(), no_match_count, df.shape[0], no_match_per))
        text.close()

    # map missing baby sequela to _none
    df.loc[df['nonfatal_cause_name'].isnull(), 'nonfatal_cause_name'] =\
        "_none"

    # groupby/sum and downcast
    df = grouper(df)
    df = dtypecaster(df)

    return df

if __name__ == "__main__":
    try:
        if deaths == "deaths":
            deaths = "deaths/"
        else:
            deaths = ""

        if write_log == "True":
            write_log = True
        else:
            write_log = False

        # read in source data
        filepath = "FILEPATH/{}{}.H5".\
            format(deaths, source)
        df = pd.read_hdf(filepath)

        df = worker_mapping(df=df,
                            write_log=write_log)

        # write output
        outpath = "FILEPATH/{}.H5".format(source)
        hosp_prep.write_hosp_file(df, outpath)
    except Exception as e:
        today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
        e = str(e)
        fail = pd.DataFrame({'fail_date': today, 'source': source, 'deaths': deaths, 'error': e}, index=[0])
        fail.to_hdf("FILEPATH/failed_jobs.H5",
            key='df', mode='a', format='table', append=True)
        warnings.warn("Job {} failed with error {}".format(source, e))
