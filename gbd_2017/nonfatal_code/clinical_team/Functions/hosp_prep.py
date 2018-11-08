# hospital module
import pandas as pd
import numpy as np
import platform
import itertools
import re
import glob
import datetime
import warnings
import getpass
import sys
import os
import time

if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

def read_multiple_dta(filelist, chunksize=1000, chunks=5):
    df = pd.DataFrame()
    for file in filelist:
        tmp = read_stata_chunks(file, chunksize, chunks)
        df = pd.concat([df, tmp])
    return df


def fill_nid(df, nid_dict):
    """
    Function that assigns the NID to each row based on the year. Accepts a
    dictionary that contains years as keys and nids as values, and a DataFrame.
    Returns DataFrame with a filled "nid" column. If the DataFrame didn't
    already have a "nid" column it makes one. Assumes that the DataFrame has a
    column named 'year_start'
    """
    assert 'year_start' in df.columns, ("DataFrame doesn't have a "
        "'year_start' column")
    df['nid'] = df['year_start'].map(nid_dict)
    return df


def test_age_binning(df):
    """
    Test the age binner to make sure that;
    Age is always >= age start and <= age end
    Age is never negative
    Age is never above 125
    Warn if age is between 110 and 125
    """
    # init a list to store bad age subsets to
    output_list = []
    if (df.age < 0).any():
        output_list.append(df[df.age < 0])
        print("There are some negative ages")

    if (df.age >= df.age_start).all() == False:
        output_list.append(df[df.age < df.age_start])
        print("Age is less than age start")

    if (df.age <= df.age_end).all() == False:
        output_list.append(df[df.age > df.age_end])
        print("Age is greater than age start")
    # check if impossibly large ages exist
    if (df.age > 125).any():
        output_list.append(df[df.age > 125])
        print("There are impossibly old ages present")

    # each test appends a df to a list if it fails. Concat these together
    if len(output_list) > 0:
        dat = pd.concat(output_list)
        print(dat)
        assert False, "One or more of the tests didn't pass"
    else:
        # warn if there are unusually old ages present
        if (df.age > 110).any():
            print(df[df.age > 110])
            warnings.warn("There are {} rows with very, very old ages present".
                          format(df.age[df.age > 110].size))
        return


def age_binning(df, drop_age=False, terminal_age_in_data=True):
    """
    function accepts a pandas DataFrame that contains age-detail and bins them
    into age ranges. Assumes that the DataFrame passed in contains a column
    named 'age'. terminal_age_in_data is used in formatting when the oldest
    age group present in the data is the terminal group.  Null values in 'age'
    will be given age_start and age_end representing all ages.

    Example: 32 would become 30, 35

    Example call: df = age_binning(df)

    The age_start bins are [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55,
    60, 65, 70, 75, 80, 85, 90, 95]

    The test will break if there are ages above 125
    """

    if df.age.max() > 99:
        warnings.warn("There are ages older than 99 in the data. Our terminal "
                      "age group is 95-125. Any age older than 99 will be "
                      "changed to age 99")
        df.loc[df.age > 99, 'age'] = 99

    if df.age.isnull().sum() > 0:
        warnings.warn("There are {} null ages in the data. Be aware: these "
                      "will be converted to age_start = 0 and age_end = 125 "
                      "and age will be arbitrarily converted to 0".\
                      format(df.age.isnull().sum()))

    # get the max age in the data
    max_age = df.age.max()

    df['age'] = pd.to_numeric(df['age'], errors='raise')  

    # bins that age is sorted into
    age_bins = np.append(np.array([0, 1, 5]), np.arange(10, 101, 5))  # 0, 1, 5
    # do not follow the 5 year bin pattern.

    # labels for age columns are the lower and upper ages of bin
    age_start_list = np.append(np.array([0, 1]), np.arange(5, 96, 5))
    age_end_list = np.append(np.array([1]), np.arange(5, 96, 5))
    age_end_list = np.append(age_end_list, 125)

    # Create 2 new age columns
    df['age_start'] = pd.cut(df['age'], age_bins, labels=age_start_list,
                             right=False)
    df['age_end'] = pd.cut(df['age'], age_bins, labels=age_end_list,
                           right=False)

    max_age = df.loc[df.age == max_age, 'age_start'].unique()
    assert len(max_age) == 1
    max_age = max_age[0]

    if terminal_age_in_data:
        # make the max age terminal
        df.loc[df['age_start'] == max_age, 'age_end'] = 125
    else:
        df.loc[df['age_start'] == 95, 'age_end'] = 125

    df['age_start'] = pd.to_numeric(df['age_start'], errors='raise') #,
                                   # downcast='integer')
    df['age_end'] = pd.to_numeric(df['age_end'], errors='raise') #,
                                 # downcast='integer')

    # assign null values to the any age group (0 - 125)
    df.loc[df['age'].isnull(), ['age_start', 'age_end']] = [0, 125]
    # fill nulls in age with 0 so the tests won't break
    # 0 is sort of arbitrary but it is the age_start value for these rows
    df['age'].fillna(0, inplace=True)

    # fill na start/end with age
    df.loc[(df.age_start.isnull()) | (df.age_end.isnull()),
           ['age_start', 'age_end']] =\
        df.loc[(df.age_start.isnull()) | (df.age_end.isnull()), 'age']

    # if age is between 115 and 125 assign it to group 110-115 and change age
    # to 114 as this is our maximum binable age
    if df.age.max() > 114:
        df.loc[(df.age > 114) & (df.age < 125),
               ['age', 'age_start', 'age_end']] = [114, 110, 115]
    test_df = test_age_binning(df)
    if test_df is not None:
        return test_df

    if drop_age:
        # Drop age variable
        df.drop('age', axis=1, inplace=True)

    # return dataframe with age_start,age_end features
    return(df)


def year_range(df):
    """
    Accepts a pandas DataFrame. Returns the lowest and highest values.
    Assumes that the DataFrame contains a column named 'years'
    Assumes that there is a column that contains all the years.
    Meant to be used to form year_start and year_end columns in a DataFrame.
    Drops the 'years' column out of the dataframe.

    This is a pretty dumb function that I don't think we ever used.
    """

    assert isinstance(df, pd.DataFrame), "df needs to be a pandas DataFrame"
    assert 'year' in df.columns, "'year' is not a column of the DataFrame"
    df['year_start'] = df['year'].min()
    df['year_end'] = df['year'].max()
    df.drop('year', axis=1, inplace=True)
    return df


def stack_merger(df):
    """
    Takes a dataframe, and the number of diagnosis variables present.
    Selects only the columns with diagnosis information
    Pivots along the index of the original dataframe to create a longer form
    Outer joins this long df with the original wide df
    Returns a stacked and merged data frame
    Assumes that the primary diagnosis column in diagnosis_vars is named dx_1
    Assumes that df has columns named in the format of "dx_*".
    Also compaires values before and after the stack to verify that the stack
    worked.

    The index of the input df needs to be unique.
    """

    assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
        "the index has a length of " + str(len(df.index.unique())) +
        " while the DataFrame has " + str(df.shape[0]) + " rows")

    diagnosis_vars = df.columns[df.columns.str.startswith('dx_')]  # find all
    # columns with dx_ at the start
    diag_df = df[diagnosis_vars]  # select just the dx cols

    # stack diagnosis var subset using index of original df
    stacked_df = diag_df.set_index([diag_df.index]).stack().reset_index()
    # rename stacked vars
    stacked_df = stacked_df.rename(columns={"level_0": "patient_index",
                                            'level_1': 'diagnosis_id',
                                            0: 'cause_code'})

    # replace diagnosis_id with codes, 1 for primary, 2 for secondary
    # and beyond
    stacked_df['diagnosis_id'] = np.where(stacked_df['diagnosis_id'] ==
                                          'dx_1', 1, 2)
    # merge stacked_df with the original df
    merged_df = stacked_df.merge(df, left_on='patient_index', right_index=True,
                                 how='outer')

    # verify that no data was lost
    # Check if primary diagnosis value counts are the same
    assert (diag_df[diagnosis_vars[0]].value_counts() ==
            merged_df['cause_code'].loc[merged_df['diagnosis_id'] == 1]
            .value_counts()).all(),\
        "Primary Diagnoses counts are not the same before and after"
    # check if all primary diagnosis are present before and after
    assert (diag_df[diagnosis_vars[0]].sort_values().values ==
            merged_df[merged_df['diagnosis_id'] == 1]['cause_code'].dropna()
            .sort_values().values).all(),\
        "Not all Primary Diagnoses are present before and after"

    # check if counts of all secondary diagnoses are the same before and after
    old_second_total = diag_df[diagnosis_vars[1:]]\
        .apply(pd.Series.value_counts).sum(axis=1)
    new_second_total = merged_df['cause_code']\
        .loc[merged_df['diagnosis_id'] == 2].value_counts()
    assert (old_second_total.sort_index().values ==
            new_second_total.sort_index().values).all(),\
        "The counts of Secondary Diagnoses were not the same before and after"
    # check if all secondary diagnoses are present before and after
    assert (old_second_total.sort_index().index ==
            new_second_total.sort_index().index).all(),\
        "Not all Secondary Diagnoses are present before and after"

    # drop all the diagnosis features, we don't need them anymore
    merged_df.drop(diagnosis_vars, axis=1, inplace=True)

    # remove missing cause codes this creates
    merged_df = merged_df[merged_df['cause_code'] != ""]

    return merged_df


def sanitize_diagnoses(df):
    """
    This function accepts one column of a DataFrame (a Series) and removes any
    non-alphanumeric character using the Regex '\W'
    """

    assert isinstance(df, pd.Series), "df needs to be a pandas DataFrame"
    df = df.str.replace("\W", "")  # "\W" regex represents ANY non-alphanumeric
    # character
    return df


def read_stata_chunks(fname, chunksize=1000, chunks=5):
    """
    Pass this function a filepath for a large dta file and it will read the
    first chuncksize * chunks rows.  Defaults to 5000 rows.
    """
    itr = pd.read_stata(fname, chunksize=chunksize)
    count = 0
    x = pd.DataFrame()
    for chunk in itr:
        count += 1
        x = x.append(chunk)
        if count == chunks:
            break
    return x


def read_text_delimited_chunks(filepath, delimiter, n):
    """
    initial commit function that helps inspect text data tables that are
    too large to load into memory
    """
    df = pd.read_table(filepath, sep=delimiter, nrows=n)
    return(df)


def create_cause_fraction(df, store_diagnostics=True, tol=1e-7, create_hosp_denom=False):
    """
    Takes a formatted mapped to nonfatal_cause_name dataframe of hospital data
    and returns a cause_fraction column outlined below.  A cause fraction is
    the propotion of admissions in a certain demographic that had a particular
    nonfatal_cause_name, out of all the admissions for any nonfatal_cause_name
    in that same demographic.  Also, the function sums all cause fractions
    within a demographic group to make sure that they are close to 1.

    Parameters:
        df: Pandas dataframe
            Contains the data to be made into cause fractions.  Should be at
            nonfatal_cause_name level.
        store_diagnostics: Boolean
            If True, then the sums of the cause fractions will be stored at
            FILEPATH/cause_fraction_sums.
            Only the most recent is stored; older versions are overwritten.
        tol: float
            Positive floating point number that determines how close the summed
            cause fractions must be to one
    """

    assert "nonfatal_cause_name" in df.columns, """'nonfatal_cause_name' isn't
        a column of df; Data must be at the baby sequelae level"""

    assert tol > 0, "'tol' must be positive"

    df = df[df.val >= 0]

    # check if there are duplicated identifier columns
    df_shape = df.shape[0]
    id_cols = ['age_group_id', 'diagnosis_id', 'facility_id', 'location_id',
               'nid', 'nonfatal_cause_name', 'representative_id', 'sex_id',
               'source', 'year_start', 'year_end', 'age_group_unit', 'metric_id',
               'outcome_id']
    id_shape = df[~df[id_cols].duplicated(keep='first')].shape[0]

    if df_shape != id_shape:
        user_in = raw_input("There appears to be some duplicated ID data. This may"
                            " be due to something that makes sense or not. Would you "
                            "like to run a groupby on all columns and collapse val?")
        yesses = ['yes', 'Yes', 'y', 'Y', 'YEs', 'YES', 'yEs']
        if user_in in yesses:
            group_cols = df.columns.drop('val').tolist()
            print("Beginning the groupby, groupby cols are {}".format(group_cols))
            pre_cases = df.val.sum()
            df = df.groupby(group_cols).agg({'val': 'sum'}).reset_index()
            assert round(pre_cases, 1) == round(df.val.sum(), 1),\
                "{} cases were lost in the groupby".format(pre_cases - df.val.sum())
        else:
            print("continuing to run function. The cause fractions will probably break"
                  " due to the duplicated data.")


    # select groupby features for the denominator
    denom_list = ['age_group_id', 'sex_id', 'year_start', 'year_end',
                  'location_id']

    numer_list = ['age_group_id', 'sex_id', 'year_start', 'year_end',
                  'location_id', 'source', 'nid', 'nonfatal_cause_name']

    # compute cause fractions
    df["numerator"] = df.groupby(numer_list)['val'].transform("sum")
    df["denominator"] = df.groupby(denom_list)['val'].transform("sum")
    df['cause_fraction'] = df['numerator'] / df['denominator']

    # check proportions sum to one
    test_sum = df[df.val > 0].groupby(denom_list)\
        .agg({'cause_fraction': 'sum'}).reset_index()
    test_sum_diag = df.groupby(denom_list)\
        .agg({'cause_fraction': 'sum'}).reset_index()
    # if store_diagnostics:
    test_sum.to_csv("FILEPATH", index=False)
    
    test_sum_diag.to_csv("FILEPATH", index=False)

    # test that the cause_fractions sum to one within tolerance
    np.testing.assert_allclose(actual=test_sum['cause_fraction'], desired=1,
                               rtol=tol, atol=0,
                               err_msg=("Proportions do not sum to "
                                        "1 within tolerance of {}".format(tol)),
                               verbose=True)

    if create_hosp_denom:
        # write sample size to J temp
        samp = df[['age_group_id', 'sex_id', 'year_start', 'year_end',
                   'location_id', 'denominator']].copy()
        samp.drop_duplicates(inplace=True)
        samp.to_csv("FILEPATH",index=False)

        # backup to the archive
        samp.to_csv("FILEPATH")

    # no cause fractions above 1
    assert df.cause_fraction.max() <= 1,\
        "there are fractions that are bigger than 1, that's nonsense"

    return(df)


def year_binner(df):
    """
    create 5 year bands for dismod, 88-92, 93-97, etc, bins that years are
    sorted into.  Does NOT collapse or aggregate, it simply replaces year_start
    and year_end.
    """
    assert df.year_start.min() > 1987,\
        "There is data present before our earliest year bin (1988), please remove"

    year_bins = np.arange(1988, 2019, 5)

    # labels for year columns are the start and end years of bins
    year_start_list = np.arange(1988, 2017, 5)

    # rename year columns
    df.rename(columns={'year_start': 'og_year_start',
                       'year_end': 'og_year_end'}, inplace=True)

    # Create 2 new year columns
    df['year_start'] = pd.cut(df['og_year_start'], year_bins,
                              labels=year_start_list, right=False)
    df['year_start'] = pd.to_numeric(df['year_start'])
    df['year_end'] = df['year_start'] + 4

    assert (df['og_year_start'] >= df['year_start']).all()
    assert (df['og_year_start'] <= df['year_end']).all()
    assert (df['og_year_end'] >= df['year_start']).all()
    assert (df['og_year_end'] <= df['year_end']).all()

    # drop original year columns
    df.drop(['og_year_start', 'og_year_end'], axis=1, inplace=True)

    return(df)


def apply_merged_nids(df, assert_no_nulls=True,
                      fillna=False):
    """
    Function that merges on merged NIDs.  merged nids are merged via nid.
    When creating 5 year bands we need new NIDs to fit with these new bands
    This script reads and processes the merged NID list provided

    Args:
        df: (Pandas DataFrame) Data with single year NIDs.  Should be at 5 year
            year_start and year_end, i.e., has had hosp_prep.year_binner()
            ran on it, but could probably work on year-by-year data too since
            years are not used in the merge.
        assert_no_nulls: (bool) if True, and it should always be True, will
            assert that there are no nulls in the NID column at the end.  Only
            exists for development purposes
        fillna: (bool) if True, will fill nulls in NID column with -1. Should
            always be false but exists for development purposes.

    Returns:
        dataframe df with the new NIDs

    Raises:
        AssertionError if the columns or number of now change.
    """

    columns_before = df.columns

    nids = pd.read_excel("FILEPATH")

    # after this drop,  nids has two columns: nids and merged_nids
    nids = nids.drop(['source', 'year_start', 'year_end'], axis=1)
    nids = nids.drop_duplicates()

    # rename nid so this is clear
    nids = nids.rename(columns={"nid": 'old_nid'})
    df = df.rename(columns={"nid": "old_nid"})

    # some data sources only have one year of data, so don't
    # need a merged nid
    single_years = []
    for source, a_df in df.groupby(['source']):
        if a_df.year_start.unique().size == 1:
            single_years.append(source)


    pre_shape = df.shape[0]
    df = df.merge(nids, how="left", on='old_nid')
    assert df.shape[0] == pre_shape, "Extra rows were added. That's bad!"

    # if a source has only 1 unique year_start/year_end value then we can just
    # use the standard nid
    for asource in single_years:
        if df.loc[df.source == asource, 'merged_nid'].isnull().all():
            df.loc[df.source == asource, 'merged_nid'] = df.loc[df.source == asource, 'old_nid']

    # rename merged_nid
    df = df.rename(columns={"merged_nid": "nid"})

    # drop old nid
    df = df.drop("old_nid", axis=1)

    assert_msg = """
    columns before and after don't match.  The difference is :
    {}
    """.format(set(df.columns).symmetric_difference(set(columns_before)))
    assert set(df.columns).symmetric_difference(set(columns_before)) == set()

    if fillna:
        df.loc[df.nid.isnull(), 'nid'] = -1

    if assert_no_nulls:
        assert df.nid.isnull().sum() == 0

    return df


def five_year_nids(df):
    """
    When creating 5 year bands we need new NIDs to fit with these new bands
    This script reads and processes the merged NID list provided

    returns the dataframe df with the new NIDs

    """
    # read NID lists
    nid = pd.read_excel("FILEPATH")
    nid2 = pd.read_excel("FILEPATH")
    nid3 = pd.read_excel("FILEPATH")

    # keep the two useful columns
    nid = nid[['Nid', 'Title']].copy()

    # drop a row meant for organization
    nid.drop(nid[nid.Title.str.contains("USE SINGLE NID INSTEAD OF MERGED")].index, axis=0, inplace=True)
    nid.drop(nid[nid.Title.str.contains("United Kingdom")].index, axis=0, inplace=True)

    # take the years
    nid['year'] = nid.Title.str[-9:]

    nid['year_start'] = nid.year.str.split("-", expand=True).iloc[:,0]

    nid['year_end'] = nid.year.str.split("-", expand=True).iloc[:,1]

    # manually fix the year groups
    nid.loc[nid['year_start'] == "stem 1997", ['year_start', 'year_end']] = 1993, 1997
    nid.loc[nid['year_start'] == "rges 2014", ['year_start', 'year_end']] = 2013, 2017
    nid.loc[nid['year_start'] == "aims 2012", ['year_start', 'year_end']] = 2008, 2012

    nid['year_start'] = pd.to_numeric(nid.year_start, 'integer')
    nid['year_end'] = pd.to_numeric(nid.year_end, 'integer')


    nid = year_binner(nid)
    nid2 = year_binner(nid2)
    # uk data is converted to calendar years
    nid3['year_end'] = nid3['year_start']
    nid3 = year_binner(nid3)

    # clean Title column
    nid['Title'] = nid.Title.str.replace("[0-9]|-", "")
    nid['Title'] = nid.Title.str.strip()
    nid3['title'] = nid3['title'].str.replace("[0-9]|-", "")
    nid3['title'] = nid3['title'].str.strip()

    # create dictionary of full name to shortened source name
    source_dict = {'Kyrgyzstan  Bishkek ClinicalRelated Groups Hospital Claims':'KGZ_MHIF',
                    'New Zealand National Minimum Dataset': 'NZL_NMDS',
                    'Austria Hospital Inpatient Discharges': 'AUT_HDD',
                    'Norway Patient Register': 'NOR_NIPH_08_12',
                    'Sweden National Patient Register':
                        'SWE_PATIENT_REGISTRY_98_12',
                    'United States National Hospital Discharge Survey':
                        'USA_NHDS_79_10',
                    'United States National Hospital Ambulatory Medical Care Survey':
                        'USA_NHAMCS_92_10',
                    'United States State Inpatient Databases': 'USA_HCUP_SID',
                    'Ecuador Hospital Inpatient Discharges': 'ECU_INEC_97_11',
                    'China National Health Statistical Information Reporting System':
                        'CHN_NHSIRS',
                    'Mexico Ministry of Health Hospital Discharges':
                        'MEX_SINAIS',
                    'Brazil Hospital Information System': 'BRA_SIH',
                    'India  Shillong Nazareth Hospital Inpatient Discharges':
                        'IND_SNH',
                    # new sources for nid3
                    'United Kingdom  England Hospital Episode Statistics':
                        'UK_HOSPITAL_STATISTICS',
                    'Nepal Hospital Inpatient Discharges': 'NPL_HID',
                    'Qatar Annual Inpatients Discharge Abstract': 'QAT_AIDA',
                    'Philippine Health Insurance Corporation Claims': 'PHL_HICC'}

    # add source which matches our data
    nid['source'] = nid['Title'].map(source_dict)
    nid3['source'] = nid3['title'].map(source_dict)

    nid.rename(columns={'Nid': 'new_nid'}, inplace=True)

    nid = nid[['new_nid', 'year_start', 'year_end', 'source']]

    # add part 2 nids
    
    nid2.loc[nid2['nid']==114876, 'merged_nid'] = 234742
    # use single year NIDs where appropriate
    nid2.loc[nid2.merged_nid == "Use single NID", "merged_nid"] =\
        nid2.loc[nid2.merged_nid == "Use single NID", "nid"]
    # drop single year NID columns, not needed
    nid2.drop('nid', axis=1, inplace=True)
    # rename col to match original nid df and drop duplicates
    nid2.rename(columns={'merged_nid': 'new_nid'}, inplace=True)
    nid2.drop_duplicates(inplace=True)

    # the USA NHDS source is in both data sets, get rid of it in 1
    nid2 = nid2[nid2.source != "USA_NHDS_79_10"]

    # add part 3 NIDs
    nid3.loc[nid3.merged_nid == "use single NID", "merged_nid"] =\
    nid3.loc[nid3.merged_nid == "use single NID", "nid"]
    nid3.drop('nid', axis=1, inplace=True)
    # rename col to match original nid df and drop duplicates
    nid3.rename(columns={'merged_nid': 'new_nid'}, inplace=True)
    nid3 = nid3[['new_nid', 'year_start', 'year_end', 'source']]
    nid3.drop_duplicates(inplace=True)

    nid = pd.concat([nid, nid2, nid3])

    nid['new_nid'] = pd.to_numeric(nid['new_nid']) # , downcast='integer')

    # find source with only a single year of data, we'll pass these nids to new nid
    single_years = []
    for source, a_df in df.groupby(['source']):
        if a_df.year_start.unique().size == 1:
            single_years.append(source)

    pre_shape = df.shape[0]
    df = df.merge(nid, how='left', on=['year_start', 'year_end', 'source'])
    assert df.shape[0] == pre_shape, "Extra rows were added. That's bad!"

    # if a source has only 1 unique year_start/year_end value then we can just
    # use the standard nid
    for asource in single_years:
        if df.loc[df.source == asource, 'new_nid'].isnull().all():
            df.loc[df.source == asource, 'new_nid'] = df.loc[df.source == asource, 'nid']


    df.loc[(df.new_nid.isnull()) & (df.source == 'EUR_HMDB'), 'new_nid'] = 3822

    # check if new_nid is null
    assert df.new_nid.isnull().sum() == 0,\
    "there are missing nids for {}".format(df[df.new_nid.isnull()].source.unique())
    # drop (old) nid
    df.drop('nid', axis=1, inplace=True)
    # rename new nid
    df.rename(columns={'new_nid': 'nid'}, inplace=True)

    return(df)


def hosp_check(df, maternal=False, bundles="all"):
    """
    Final check before writing data. This will run a series of check to ensure that:

    Confirm 6 unique aggregated year groups are present
    Confirm that all age groups by source are present
    Confirm that the bundles which were mapped onto the data are present
    Confirm the types of representative_name values
    Confirm that there is not demographic data duplicated
    Confirm that upper is larger than mean and mean is larger than lower
    Confirm for nulls in cols were we don't expect them.
    Confirm correct datatypes of a few columns that keep switching type
    
    Parameters:
        df: Pandas DataFrame
            a df of hospital data at the final stage of elmo formatting
        maternal: Boolean
            does df contain adjusted maternal data with restricted age groups?
    """
    start = time.time()

    # check for specific years
    good_years = [1988, 1993, 1998, 2003, 2008, 2013]
    diff_years = set(good_years).symmetric_difference(set(df['year_start'].unique()))
    assert diff_years == set(),\
        "There are differences in the years. diff years are {}".format(diff_years)
    print("Test for all year_start values present has passed")
    # every age group present for every nid. I believe this should work because
    # our age sex splitting will create every age group for every source

    # get hospital age groups
    good_ages = get_hospital_age_groups()
    good_ages.loc[good_ages.age_end > 1, 'age_end'] = good_ages.loc[good_ages.age_end > 1, 'age_end'] - 1

    if maternal:
        # keep just the maternal age groups
        good_ages = good_ages.query("age_start > 9 & age_start < 55")
        good_ages = good_ages['age_start'].unique().tolist() +\
                good_ages['age_end'].unique().tolist()

        # TODO look into why these are incomplete in the future
        exempt_nids = [321359.0, 285520.0, 68367.0, 68535.0, 96714.0]
    else:
        good_ages = good_ages['age_start'].unique().tolist() +\
            good_ages['age_end'].unique().tolist()

        # NIDs that we know aren't complete, KGZ and one recent NHDS nid
        exempt_nids = [96714.0, 234774.0]

    nids = [x for x in df.nid.unique() if x not in exempt_nids]
    bad_nids = []
    bad_groups = []
    for asource in nids:
        df_s = df[df.nid == asource]
        if maternal:
            if not len(df_s['age_start'].unique()) == 9 and len(df_s['age_end'].unique()) == 9:
                bad_nids.append(asource)

        else:
            if not len(df_s['age_start'].unique()) == 21 and len(df_s['age_end'].unique()) == 21:
                bad_nids.append(asource)

        df_ages = df_s['age_start'].unique().tolist() +\
            df_s['age_end'].unique().tolist()
        if not set(good_ages).symmetric_difference(set(df_ages)) == set():
            bad_groups.append(asource)

    assert len(bad_nids) == 0,\
        "There are nids with an incomplete number of unique ages. They are {}".\
        format(bad_nids)
    assert len(bad_groups) == 0,\
        "There are nids with a different set of unique ages then we expect. They are {}".\
        format(bad_groups)

    print("Test for all age_start/age_end values present has passed")

    # confirm all the bundles are present that should be
    if maternal:
        mat_bundles = [646, 74, 75, 76, 77, 422, 423, 667, 78, 3419]  # hard code for now
        mat_diff = set(mat_bundles).symmetric_difference(set(df.bundle_id.unique()))
        assert mat_diff == set(),\
            "The maternal bundles don't line up perfectly with our hard coding. "\
            "diff is {}".format(mat_diff)
    else:
        if bundles == "all":

            exempt_bundles = []  # [206.0, 176.0, 208.0, 766.0, 333.0]

            # read in clean maps to get all the bundles we should have
            cm = pd.read_csv("FILEPATH")
            map_bundles = set(cm.bundle_id.dropna().unique()) - set(exempt_bundles)
            df_bundles = set(df.bundle_id.dropna().unique())
            # parent injuries aren't in the map so add them in
            pc_inj = pd.read_csv("FILEPATH")
            pc_inj = pc_inj[pc_inj.parent == 1]['Level1-Bundle ID']
            map_bundles.update(pc_inj)
        else:
            assert type(bundles) == np.ndarray or type(bundles) == list,\
                "Please pass bundles as a list or ndarray"
            map_bundles = bundles

        assert map_bundles.symmetric_difference(df_bundles) == set(),\
            "There is a difference between the bundles in the map and the "\
            "bundles in the data. diff is {}".format(map_bundles.symmetric_difference(df_bundles))
    print("Test for all bundle IDs has passed")

    if not maternal:
        assert (df.loc[df['upper_raw'].notnull(), 'upper_raw'] >=\
                df.loc[df['upper_raw'].notnull(), 'mean_raw']).sum() ==\
                df[df['upper_raw'].notnull()].shape[0],\
                "Mean is larger than upper (or null) here:\n{}".format(
                    df[df['upper_raw'].notnull()].\
                    loc[~(df.loc[df['upper_raw'].notnull(), 'upper_raw'] >=\
                    df.loc[df['upper_raw'].notnull(), 'mean_raw'])])

        assert (df.loc[df['lower_raw'].notnull(), 'mean_raw'] >=\
                df.loc[df['lower_raw'].notnull(), 'lower_raw']).sum() ==\
                df[df['upper_raw'].notnull()].shape[0],\
                "Mean is smaller than lower (or null) here:\n{}".format(
                    df[df['lower_raw'].notnull()].\
                    loc[~(df.loc[df['lower_raw'].notnull(), 'lower_raw'] <=\
                    df.loc[df['lower_raw'].notnull(), 'mean_raw'])])
        print("Test for upper >= mean >= lower has passed")

    # no unforeseen null values
    nulls = df[['location_id', 'year_start', 'year_end', 'age_start', 'age_end',
                'sex', 'nid', 'bundle_id', 'measure', 'representative_name', 'mean_raw',
                'extractor', 'location_name', 'bundle_name']].isnull().sum()
    assert (nulls == 0).all(),\
        "There are null values in {}".format(nulls)
    print("Test for null values has passed")

    # check demographic cols for dtype
    demo_cols = ['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'bundle_id']
    for col in demo_cols:
        assert df[col].dtype != "object",\
            "Why the heck is {} a {} dtype".format(col, df[col].dtype)
    print("Test for object dtype in demographic columns has passed")

    # there are only two types of representativeness
    assert len(df['representative_name'].unique()) == 2
    assert (df['representative_name'].sort_values().unique() == ('Nationally representative only', 'Not representative',)).all()
    print("Test for acceptable representative names has passed")

    # all columns are present
    ordered = ['bundle_id', 'bundle_name', 'measure',
               'location_id', 'location_name', 'year_start', 'year_end',
               'age_start',
               'age_end', 'sex', 'nid',
               'representative_name',
               'mean_raw', 'lower_raw', 'upper_raw',
               'mean_indvcf', 'lower_indvcf', 'upper_indvcf', 'correction_factor_1',
               'mean_incidence', 'lower_incidence', 'upper_incidence', 'correction_factor_2',
               'mean_prevalence', 'lower_prevalence', 'upper_prevalence', 'correction_factor_3',
               'mean_inj', 'lower_inj', 'upper_inj', 'correction_factor_inj',
               'haqi_cf',
               'sample_size',
               'age_demographer',
               'source_type', 'urbanicity_type',
               'recall_type', 'unit_type', 'unit_value_as_published',
               'cases', 'is_outlier', 'seq', 'underlying_nid',
               'sampling_type', 'recall_type_value', 'uncertainty_type',
               'uncertainty_type_value', 'input_type', 'standard_error',
               'effective_sample_size', 'design_effect', 'response_rate',
               'extractor']

    # remove the injury columns for maternal data
    if maternal:
        ordered = [n for n in ordered if "_inj" not in n]

    diff_cols = set(df.columns).symmetric_difference(set(ordered))

    assert diff_cols == set(),\
        "There are different cols to start and end, diff is {}".format(diff_cols)
    print("Test for ELMO required columns has passed")

    # there should be no duplicates for demography + bundle_id
    assert df[df[['age_start', 'age_end', 'year_start', 'year_end',
                'location_id', 'sex', 'bundle_id']].\
                duplicated(keep=False)].shape[0] == 0,\
        "There is some duplicated demographic info. The dupe DF is {}".\
        format(df[df[['age_start', 'age_end', 'year_start', 'year_end',
            'location_id', 'sex', 'bundle_id']].duplicated(keep=False)])
    print("Test for duplicates of any demographic group has passed")

    wall_time = round(time.time()-start, 2)
    time_unit = "seconds"
    if wall_time > 60:
        wall_time = round(wall_time/60, 2)
        time_unit = "minutes"
    print("Tests ran in {} {}".format(wall_time, time_unit))
    return("All Checks Passed")


def create_ms_durations(df):
    """
   
    a function to write a new durations file for market scan incidence
    causes
    takes our clean_maps_`x' file and outputs a new durations file

    Parameters:
        df: Pandas DataFrame
            A df of the cleaned
    """
    
    print("printing any BIDs that have more than one duration")
    for bid in df[df.bid_measure == 'inc'].bundle_id.unique():
        if len(df[df.bundle_id == bid].bid_duration.unique()) > 1:
            print(bid, df[df.bundle_id == bid].bid_duration.unique())
            assert False

    bid_durations = pd.read_excel("FILEPATH")
    bid_durations.rename(columns={'measure': 'bid_measure'}, inplace=True)

    pre = df.shape[0]
    df = df.merge(bid_durations, how='left', on=['bid_measure', 'bundle_id'])
    assert pre == df.shape[0], "Rows were added or lost"

    # replace the missing values from the new map with the durations from the
    # older duration file
    df.loc[df.bid_duration.isnull(), 'bid_duration'] =\
        df.loc[df.duration.notnull(), 'duration']

    df.loc[df.bid_measure == 'prev', 'bid_duration'] = np.nan

    # compare durations
    p = df[['bundle_id', 'bid_measure', 'bid_duration', 'duration']].drop_duplicates()
    p = p[p.bid_measure == "inc"]
    p = p[p.bundle_id.notnull()]
    print(p[p.bid_duration != p.duration])

    # drop the older duration column
    df.drop("duration", axis=1, inplace=True)

    # check durations
    print("checking if all inc BIDs have at least one non null duration")
    assert df.loc[(df.bid_measure == 'inc')&(df.bundle_id.notnull()), 'bid_duration'].notnull().all(),\
        "We expect that 'inc' BIDs only have Non-Null durations"

    assert len(df.loc[(df.bid_measure == 'inc')&(df.bundle_id.notnull()), 'bid_duration'].unique()),\
       "We expect that 'inc' BIDs only have Non-Null durations"

    print("checking if all prev BIDs only have null durations")
    assert df.loc[df.bid_measure == 'prev', 'bid_duration'].isnull().all(),\
        "We expect that 'prev' BIDs only have null durations"

    # but check if everything has one duration
    print("checking any inc BIDs have more than one duration")
    for bid in df['bundle_id'].unique():
        if len(df[(df['bundle_id'] == bid)&(df.bid_measure == 'inc')].bid_duration.unique()) > 1:
            print(bid, df[(df['bundle_id'] == bid)&(df.bid_measure == 'inc')].bid_duration.unique())
            assert False

    df = df[['bid_measure', 'bid_duration', 'bundle_id']].copy().drop_duplicates()
    # drop rows with null bundle id and sort
    df = df[df.bundle_id.notnull()].sort_values(['bid_measure', 'bundle_id'])

    # rename cols to fit what the claims processing is currently using
    df.rename(columns={'bid_measure': 'measure', 'bid_duration': 'duration'},
              inplace=True)
    # write to J:WORK
    writer = pd.ExcelWriter("FILENAME",
                            engine='xlsxwriter')
    df.to_excel(writer, sheet_name="Sheet 1", index=False)
    writer.save()
    # backup to the archive
    backup_writer = pd.ExcelWriter('FILEPATH')
    df.to_excel(backup_writer, sheet_name="Sheet 1", index=False)
    backup_writer.save()
    return


def drop_data(df, verbose=True):
    """
    function that drops the data that we don't want. reasons are
    outlined in comments
    """


    # facility type, exclusion stats
    # hospital : inpatient
    # day clinic : EXCLUDED
    # emergency : EXCLUDED
    # clinic in hospital : outpatient, but has different pattern than other Outpatient
    # outpatient clinic : outpatient
    # inpatient unknown : inpatient
    # outpatient unknown : outpatient

    # print how many rows we're starting wtih
    print("STARTING NUMBER OF ROWS = " + str(df.shape[0]))
    print("\n")

    # drop everything but inpatient
    # our envelope is only inpatient so we only keep inpatient, for now.
    df = df[(df['facility_id'] == 'inpatient unknown') |
            (df['facility_id'] == 'hospital')]
    if verbose:
        print("DROPPING OUTPATIENT")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop secondary diagnoses
    df = df[df['diagnosis_id'] == 1]  # I actually don't remember why
    if verbose:
        print("DROPPING ALL SECONDARY DIAGNOSES")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop canada data
    df = df[df['source'] != 'CAN_NACRS_02_09']
    df = df[df['source'] != 'CAN_DAD_94_09']
    if verbose:
        print("DROPPING CANADA DATA")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop data that is at national level for HCUP 
    df = df[(df.location_id != 102) | (df.source != 'USA_HCUP_SID')]
    if verbose:
        print("DROPPING HCUP NATIONAL LEVEL USA DATA")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop data that is from before 1990, because the envelope starts at 1990
    df = df[df['year_start'] >= 1990]
    if verbose:
        print("DROPPING DATA FROM BEFORE YEAR 1990")
    print("FINAL NUMBER OF ROWS = " + str(df.shape[0]))
    print("\n")

    # check that we didn't somehow drop all rows
    assert df.shape[0] > 0, "All data was dropped, there are zero rows!"
    return(df)


def get_hospital_age_groups():
    """
    Function that returns age_group_ids that we use on the
    hosital prep team.  Use it to merge on age_group_id onto your data, or
    to merge age_start and age_end onto some covariate or model output
    """
    age = pd.DataFrame({"age_start":[ 0,  1,  5, 10, 15, 20, 25, 30, 35, 40,
                        45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
                     "age_end": [ 1,  5,  10, 15, 20, 25, 30, 35, 40, 45, 50,
                        55, 60, 65, 70, 75, 80, 85, 90, 95, 125],
                     "age_group_id": [ 28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                        15, 16, 17, 18, 19, 20, 30, 31, 32, 235]})
    age = age[['age_start', 'age_end', 'age_group_id']]
    return(age)


def group_id_start_end_switcher(df, remove_cols=True):
    """
    Takes a dataframe with age start/end OR age group ID and switches from one
    to the other
    """
    # Determine if we're going from start/end to group id or vise versa
    # if 'age_start' and 'age_end' and 'age_group_id' in df.columns:
    if sum([w in ['age_start', 'age_end', 'age_group_id'] for w in df.columns]) == 3:
        print("All age columns are present, unclear which output is desired. "
        r"Simply drop the columns you don't want")
        return
    #elif 'age_start' and 'age_end' in df.columns:
    elif sum([w in ['age_start', 'age_end'] for w in df.columns]) == 2:
        merge_on = ['age_start', 'age_end']
        switch_to = ['age_group_id']
    elif 'age_group_id' in df.columns:
        merge_on = ['age_group_id']
        switch_to = ['age_start', 'age_end']
    else:
        print("Age columns not present or named incorrectly")
        return

    # pull in our hospital age groups
    ages = get_hospital_age_groups()

    # determine if the data contains only our hosp age groups or not
    age_set = "hospital"
    for m in merge_on:
        # check that ages preset are within the set of good ages:
        if not set(df[m]).issubset(set(ages[m])):
            print("Irregular ages found. Try running the version of this"
                 " function which lives in gbd_hosp_prep.py")
            return

    # merge on the age group we want
    pre = df.shape[0]
    df = df.merge(ages, how='left', on=merge_on)
    assert pre == df.shape[0], "rows were duplicated"

    # check the merge
    for s in switch_to:
        assert df[s].isnull().sum() == 0, ("{} contains missing values from"
            "the merge").format(s)

    if remove_cols:
        df.drop(merge_on, axis=1, inplace=True)
    return(df)


def make_square(df):
    """
    takes a dataframe and returns the square of every age/sex/bundle id which
    exists in the given dataframe but only the years available for each
    location id
    """

    def expandgrid(*itrs):
        # create a template df with every possible combination of
        #  age/sex/year/location to merge results onto
        # define a function to expand a template with the cartesian product
        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

    final_df = []
    ages = df.age_group_id.unique()
    sexes = df.sex_id.unique()
    
    nfcs = df.nonfatal_cause_name.unique()
    for loc in df.location_id.unique():
        dat = pd.DataFrame(expandgrid(ages, sexes, [loc],
                                      df[df.location_id == loc].
                                      year_start.unique(),
                                      nfcs))
        dat.columns = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                       'nonfatal_cause_name']
        final_df.append(dat)

    final_df = pd.concat(final_df)
    final_df['year_end'] = final_df['year_start']

    age = pd.DataFrame({"age_start":[ 0,  1,  5, 10, 15, 20, 25, 30, 35, 40,
                        45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
                     "age_end": [ 1,  5,  10, 15, 20, 25, 30, 35, 40, 45, 50,
                        55, 60, 65, 70, 75, 80, 85, 90, 95, 125],
                     "age_group_id": [ 28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                        15, 16, 17, 18, 19, 20, 30, 31, 32, 235]})
    age = age[['age_start', 'age_end', 'age_group_id']]

    final_df = final_df.merge(age, how='left', on='age_group_id')
    return(final_df)


def make_maternal_denom():
    """
    """

    df = pd.read_csv(FILEPATH,
                     dtype={'cause_code': object})
    # drop data
    df = drop_data(df, verbose=False)
    df['cause_code'] = df['cause_code'].astype(str)
    df['cause_code'] = sanitize_diagnoses(df['cause_code'])

    # merge clean_maps onto the data to properly duplicate columns
    path_to_map = FILEPATH
    maps = pd.read_csv(path_to_map)
    maps = maps[['cause_code', 'bundle_id', 'code_system_id']]
    # drop duplicate values.  Note that subsetting a map can "create"
    # duplicated values
    maps = maps.drop_duplicates()
    # match on upper case
    df['cause_code'] = df['cause_code'].str.upper()
    maps['cause_code'] = maps['cause_code'].str.upper()
    # merge the hospital data with excel spreadsheet maps
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])
    df.drop('bundle_id', axis=1, inplace=True)

    # pull in ICD maps
    icd9 = pd.read_excel(FILEPATH
                         converters={'icd_code': str})
    icd9['code_system_id'] = 1
    icd10 = pd.read_excel(FILEPATH
                          converters={'icd_code': str})
    icd10['code_system_id'] = 2

    mat_df = pd.concat([icd9, icd10])
    mat_df['icd_code'] = mat_df['icd_code'].astype(str)
    mat_df = mat_df[['icd_code', 'code_system_id',
                     'include_in_maternal_hospital_denom_2016']].copy()
    mat_df = mat_df.query("include_in_maternal_hospital_denom_2016 == 1")
    causes = mat_df.icd_code

    # clean icd code columns
    mat_df['icd_code'] = sanitize_diagnoses(mat_df['icd_code'])
    mat_df.rename(columns={'icd_code': 'cause_code'}, inplace=True)

    # merge master data onto the mat_df object
    df['cause_code'] = df['cause_code'].str.upper()
    mat_df['cause_code'] = mat_df['cause_code'].str.upper()
    mat_df = mat_df.merge(df, how='left', on=['cause_code', 'code_system_id'])

    # select just the columns we need for the denominator
    denom_list = ['age_start', 'age_end', 'sex_id', 'year_start',
              'year_end', 'location_id']
    mat_df = mat_df[denom_list + ['val']]

    # do a groupby agg to create denoms
    mat_df = mat_df.groupby(denom_list).agg({'val': 'sum'}).reset_index()
    mat_df.rename(columns={'val': 'denominator'}, inplace=True)

    # assert data wasn't lost
    assert set(mat_df.age_start.unique()).symmetric_difference(set(df.age_start.unique())) == set()
    assert set(mat_df.age_end.unique()).symmetric_difference(set(df.age_end.unique())) == set()
    assert set(mat_df.year_start.unique()).symmetric_difference(set(df.year_start.unique())) == set()
    assert set(mat_df.year_end.unique()).symmetric_difference(set(df.year_end.unique())) == set()
    assert set(mat_df.sex_id.unique()).symmetric_difference(set(df.sex_id.unique())) == set()
    assert set(mat_df['location_id'].unique()).symmetric_difference(set(df['location_id'].unique())) == set()
    # assert cases in data and cases in maternal denom are almost identical
    assert 1 - (mat_df.denominator.sum() / df[df.cause_code.isin(causes)].val.sum()) < .0001

    # write the denominators temp/hospital
    mat_df.to_csv('FILEPATH', index=False)
    mat_df.to_csv("FILEPATH", index=False)


def apply_restrictions(df, col_to_restrict, drop_restricted=True):
    """
    reads in the file at FILEPATH

    Parameters:
        df: pandas DataFrame
            data that you want to restrict. Must have at least the columns
            "age_start", "age_end", and "sex_id"
        col_to_restrict: string
            Your main data column, the column of interest, the column you want
            to restrict.  Must be a string.
    """

    # pull in our hospital age groups
    check_ages = get_hospital_age_groups()

    # determine if the data contains only our hosp age groups or not
    age_set = "hospital"

    ages_unique = check_ages['age_group_id'].drop_duplicates()
    df_unique = df['age_group_id'].drop_duplicates()
    # if their shapes aren't the same it's irregular ages
    if set(ages_unique).symmetric_difference(set(df_unique)) != set():
            age_set = 'non_hospital'

    # if there are only 21 age groups present use the hosp prep switcher
    if age_set == "hospital":
        df = group_id_start_end_switcher(df)
    # if there are more than 21 use the gbd env switcher
    if age_set == "non_hospital":
        import gbd_hosp_prep
        # switch from age group id to age start end
        df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    assert set(['nonfatal_cause_name', 'age_start', 'age_end',
                'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
            format(df[col_to_restrict].isnull().sum()))

    # store columns that we started with
    start_cols = df.columns

    restrict = pd.read_csv("FILEPATH")
    restrict = restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    restrict = restrict.reset_index(drop=True)  
    restrict.rename(columns={'Baby Sequelae  (nonfatal_cause_name)':
                             'nonfatal_cause_name'}, inplace=True)
    restrict['nonfatal_cause_name'] =\
        restrict['nonfatal_cause_name'].str.lower()
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict = restrict.drop_duplicates()

    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict, how='left', on='nonfatal_cause_name')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")

    # check to see if any new babies don't have age/sex restrictions
    cols = ['male', 'female', 'yld_age_start', 'yld_age_end']
    for col in cols:
        if df[col].isnull().sum() > 0:
            warnings.warn("There are missing restrictions for baby(ies): {}"\
                .format(df[df[col].isnull()].nonfatal_cause_name.unique()))

    # set col_to_restrict to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), col_to_restrict] = np.nan

    # set col_to_restrict to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), col_to_restrict] = np.nan

    # set col_to_restrict to zero where age end is smaller than yld age start
    df.loc[df['age_end'] <= df['yld_age_start'], col_to_restrict] = np.nan

    # set col_to_restrict to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], col_to_restrict] = np.nan

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)

    if drop_restricted:
        # drop the restricted values
        df = df[df[col_to_restrict].notnull()]

    assert set(start_cols) == set(df.columns)

    # return to age group id
    # if there are only 21 age groups present use the hosp prep switcher
    if age_set == "hospital":
        df = group_id_start_end_switcher(df)
    # if there are more than 21 use the gbd env switcher
    if age_set == "non_hospital":
        import gbd_hosp_prep
        # switch from age group id to age start end
        df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    return(df)

def apply_bundle_restrictions(df, col_to_restrict, drop_restricted=True):
    """
    Function that applies age (and sex) restrictions.  Data is
    expected to have columns "age_start", "age_end", and "sex_id", and of
    course the column that you want to restrict.

    Parameters:
        df: pandas DataFrame
            data that you want to restrict. Must have at least the columns
            "age_start", "age_end", and "sex_id"
        col_to_restrict: string
            Your main data column, the column of interest, the column you want
            to restrict.  Must be a string.

    Returns:
        Dataframe with the other set of age columns from the one you passed in
    """
    start = time.time()
    assert set(['bundle_id', 'age_group_id', 'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

    # switch from age group id to age start/end
    df = group_id_start_end_switcher(df)
    if 'age_group_id' in df.columns:
        df = group_id_start_end_switcher(df)

    # takes about 1.5 min
    print("Age groups switched in {} min".format((time.time()-start)/60))

    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
            format(df[col_to_restrict].isnull().sum()))

    # store columns that we started with
    start_cols = df.columns

    restrict = pd.read_csv("FILEPATH")

    restrict = restrict.reset_index(drop=True)  
    restrict = restrict[restrict.bundle_id.notnull()]
    restrict = restrict.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.  This is necessary, there is a age_start = 0.1 years
    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # takes seconds
    print("Restrictions prepped in {} min".format((time.time()-start)/60))

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict, how='left', on='bundle_id')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")

    print("Restrictions merged on in {} min".format((time.time()-start)/60))

    # set col_to_restrict to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), col_to_restrict] = np.nan

    # set col_to_restrict to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), col_to_restrict] = np.nan

    # set col_to_restrict to zero where age end is smaller than yld age start
    df.loc[df['age_end'] <= df['yld_age_start'], col_to_restrict] = np.nan

    # set col_to_restrict to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], col_to_restrict] = np.nan

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)

    if drop_restricted:
        # drop the restricted values
        df = df[df[col_to_restrict].notnull()]

    assert set(start_cols) == set(df.columns)

    # switch back to age group id
    df = group_id_start_end_switcher(df)

    # Runs in about 2 minutes from print tatement above
    print("Restrictions all finished in {} min".format((time.time()-start)/60))

    return(df)


def check_parent_injuries(df, col_to_sum, verbose=False):
    """
    Function to check that the all the child values within a parent sum up to
    the parent.

    Args:
        df: (Pandas DataFrame) contains your injuries data
        col_to_sum: (str) Name of the column whose values you want to check
        verbose: (bool) If True will print out reports.
    Returns:
    """
    pc_injuries = pd.read_csv("FILEPATH")
    # create BID dict for parent causes
    bundle_dict = dict(zip(pc_injuries.e_code, pc_injuries['Level1-Bundle ID']))

    # loop over ecodes pulling every parent name
    df_list = []
    for parent in pc_injuries.loc[pc_injuries['parent'] == 1, 'e_code']:
        inj_df = pc_injuries[(pc_injuries['baby sequela'].
                              str.contains(parent)) &
                              (pc_injuries['parent'] != 1)]
        inj_df = inj_df[inj_df['Level1-Bundle ID'].notnull()]
        inj_df['bundle_id'] = bundle_dict[parent]
        df_list.append(inj_df)

    parent_df = pd.concat(df_list)
    parent_df.rename(columns={'Level1-Bundle ID': 'child_bundle_id',
                              'bundle_id': 'parent_bundle_id'}, inplace=True)
    parent_df = parent_df[['child_bundle_id', 'parent_bundle_id']]

    for parent_bundle in parent_df['parent_bundle_id'].unique():
        
        # get sum of parent cases
        parent_sum = df.loc[df.bundle_id == parent_bundle, col_to_sum].sum()
        
        # get sum of child cases
        child_ids = list(parent_df.loc[parent_df.parent_bundle_id ==
                                       parent_bundle, 'child_bundle_id'])
        child_sum = df.loc[df.bundle_id.isin(child_ids), col_to_sum].sum()

        # assert sum of parent equals sum of all children
        assert_msg = """
        The parent and child {} don't match

        They don't match for the parent bundle {}, whose children are {}

        The parent sum is {} and the child sum is {}
        """.format(col_to_sum, parent_bundle,
                   list(parent_df[parent_df.parent_bundle_id == parent_bundle].child_bundle_id.unique()),
                   parent_sum, child_sum)
        assert round(parent_sum, 5) == round(child_sum, 5), assert_msg
    if verbose:
        print("Parent injury {} sums equal child injury case sums".
              format(col_to_sum))


def make_zeros(df, cols_to_square, icd_len=5, etiology='bundle_id'):
    """
    takes a dataframe and returns the square of every
    age/sex/cause type(bundle or baby seq) which
    exists in the given dataframe but only the years
    available for each location id
    """
    if type(cols_to_square) == str:
        cols_to_square = [cols_to_square]
    def expandgrid(*itrs):
        # create a template df with every possible combination of
        #  age/sex/year/location to merge results onto
        # define a function to expand a template with the cartesian product
        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

    sqr_df = []
    ages = df.age_group_id.unique()
    sexes = df.sex_id.unique()

    # get all the unique bundles by source and location_id
    src_loc = df[['source', 'location_id']].drop_duplicates()
    src_bundle = df[['source', etiology]].drop_duplicates()
    loc_bundle = src_bundle.merge(src_loc, how='outer', on='source')
    # print("loc_bundle df info", loc_bundle.info())

    for loc in df.location_id.unique():
        # this isn't relevant to injuries now
        if icd_len == 3:
            assert False, "This is deprecated, set an icd_len value greater than 3"
        elif icd_len > 3:
            cause_type = loc_bundle.loc[loc_bundle.location_id == loc, 'bundle_id'].unique().tolist()

            dat = pd.DataFrame(expandgrid(ages, sexes, [loc],
                                  df[df.location_id == loc].
                                  year_start.unique(),
                                  cause_type))
            dat.columns = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                           etiology]
        sqr_df.append(dat)

    sqr_df = pd.concat(sqr_df)
    sqr_df['year_end'] = sqr_df['year_start']

    # get a key to fill in missing values for newly created rows
    missing_col_key = df[['location_id', 'year_start', 'facility_id', 'nid', 'representative_id', 'source']].copy()
    missing_col_key.drop_duplicates(inplace=True)

    # inner merge sqr and missing col key to get all the column info we lost
    final_df = sqr_df.merge(missing_col_key, how='inner', on=['location_id', 'year_start'])

    # apply age/sex restrictions to the template df
    # apply restrictions requires data at the baby seq level but make_zeros 
    # lets you operate at either bundle or baby seq. so this is breaking when make_zeros
    # is run at bundle level final_df = apply_restrictions(df=final_df, 
    # col_to_restrict='age_group_id', drop_restricted=True)

    # left merge on our built out template df to create zero rows
    final_df = final_df.merge(df, how='left', on=['age_group_id',
                              'sex_id', 'location_id', 'year_start',
                              'year_end', etiology, 'facility_id',
                              'nid', 'representative_id',
                              'source'])
    # fill missing values of the col of interest with zero
    for col in cols_to_square:
        final_df[col] = final_df[col].fillna(0)
    
    return(final_df)


def hosp_splitter(df, directory):
    """
    split the hospital data into smaller files by source and year
    """
    sources = df.source.unique()
    for datasource in sources:
        for year in df[df['source'] == datasource].year_start.unique():
            dat = df[(df.source == datasource) & (df.year_start == year)]
            try:
                dat.to_hdf(directory + datasource + "_" + str(year) + ".H5",
                  key='df', complib='blosc', complevel=5)
            except:
                filename = directory + str(datasource) + str(year) + ".log"
                logging.basicConfig(filename=filename, level=logging.DEBUG)


def infer_dtypes(df):
    """
    Short convienience function that returns the inferred datatypes of the
    columns of the DataFrame df

    Args:
        df (DataFrame): dataframe that you want to know about

    Returns:
        pandas Series of inferred data types.
    """
    return df.apply(lambda x: pd.lib.infer_dtype(x.values))


def natural_sort(list, key=lambda s:s):
    """
    Sort the list into reverse natural alphanumeric order.
    """
    def get_alphanum_key_func(key):
        convert = lambda text: int(text) if text.isdigit() else text
        return lambda s: [convert(c) for c in re.split('([0-9]+)', key(s))]
    sort_key = get_alphanum_key_func(key)
    list.sort(key=sort_key, reverse=True)


def verify_current_map(df):
    """
    confirm that the map a script is using is the current clean map
    returns true if it is and false if not. use with an assert

    """
    my_dir = "FILEPATH"
    
    map_files = glob.glob(my_dir)
    
    natural_sort(map_files)
    current_map = map_files[0]

    pattern = re.compile(r'FILEPATH')
    current_map_version = pattern.search(current_map).group(1)
    return(int(current_map_version) == int(df.map_version.unique()))


def get_current_map():
    """
    Get the current map version we're using and return as an int

    
    """
    my_dir = "FILEPATH"
    #print my_dir
    map_files = glob.glob(my_dir)
    #print map_files
    natural_sort(map_files)
    current_map = map_files[0]

    pattern = re.compile(r'FILEPATH')
    current_map_version = pattern.search(current_map).group(1)
    return(int(current_map_version))


def summary_stats(df, summarize_column, file_name, groupby_columns,
                  save_location=("FILEPATH"),
                  return_stats=False, wide_or_long='long'):
    """
    Writes a file containing the summary statistics for the specified
    groups at the specified location.  Filename will look like
    [file_name]_[today's date YYYY-MM-DD].  The statistics are mean,
    std, min, 25th percentile, 50th percentile, 75th percentile, and max. Can
    shape the statistics either long or wide. Can return the statistics if
    wanted.  Drops values of zero before computing statistics.

    Parameters:
        df: Pandas DataFrame
            data to summarize.
        summarize_column: String
            Name of the column to compute statistics for
        file_name: String
            Name the output file will have.
        groupby_columns: List of Strings
            Contains names of the columns that the data should be grouped
            by.  Should probably at least contain "bundle_id" or
            "nonfatal_cause_name".
        save_location: raw String
            File path to the folder where the statistics should be saved.
        return_stats: Boolean
            Option to return the statistics or not
        wide_or_long: String
            Specifies if the statistics should be made wide or long.
    """

    assert wide_or_long == 'long' or wide_or_long == 'wide', ("'wide_or_long"
        " should be either 'wide' or 'long', you put '{}'".format(wide_or_long))

    today = datetime.datetime.today().strftime("%Y_%m_%d")

    stats = df[df[summarize_column] != 0]

    stats = stats.groupby(groupby_columns)
    stats = stats[summarize_column].describe().reset_index()

    postfix_number = len(groupby_columns)  
    stats = stats.rename(columns={'level_{}'.format(postfix_number): 'stats'})

    # drop the count rows
    stats = stats[stats['stats'] != 'count']

    if wide_or_long == "wide":
        stats = stats.set_index(groupby_columns + ['stats']).\
            unstack('stats').reset_index()
        stats.columns = groupby_columns + ["25%", "50%", "75%", "max",
                                           "mean", "min", "std"]

    stats.to_hdf("{}{}_{}.H5".format(save_location, file_name, today),
                 complib='blosc', complevel=5, key='df', mode='w')

    if return_stats:
        return stats


def report_if_merge_fail(df, check_col, id_cols, store=False, filename=""):
    """
    Report a merge failure if there is one.

    Parameters:
        df: Pandas DataFrame
            Df that you want to check for nulls created in the merge.
        check_col: String
            Name of the column that was merged on.
        id_cols: List of Strings
            List containing the names of the columns that were used to merge on
            the check_col.
        store: Boolean
            If True will save a CSV copy of the rows with nulls in check_col at
            FILEPATH
        filename: String
            Filename used to save the copy of null rows if store is True.
    """
    merge_fail_text = """
        Could not find {check_col} for these values of {id_cols}:

        {values}
    """
    if df[check_col].isnull().values.any():

        # 'missing' can be df or series
        missing = df.loc[df[check_col].isnull(), id_cols]

        if store:
            missing.to_csv("FILEPATH", index=False, encoding='utf-8')

        raise AssertionError(
            merge_fail_text.format(check_col=check_col,
                                   id_cols=id_cols, values=missing)
        )
    else:
        pass


def find_missing_demos(df):
    """
    print any ages or sexes that are entirely missing by unique combos
    of datasource/year/location
    Output is just print statements for now

    Parameters:
        df: Pandas DataFrame
    """
    start = time.time()
    
    sources = df.source.unique()
    for source in sources:
        dat = df[df.source == source].copy()
        years = dat.year_start.unique()
        ages = dat.age_start.unique()
        for year in years:
            datyear = dat[dat.year_start == year]
            locs = datyear.location_id.unique()
            for loc in locs:
                datloc = datyear[datyear.location_id == loc]
                # assert all ages are present
                diff = set(datloc.age_start.unique()).symmetric_difference(set(ages))
                if diff != set():
                    print("age bad source: {} year: {} loc: {} age: {}".format(source, year, loc, diff))
                # assert all sexes are present
                diff = set(datloc.sex_id.unique()).symmetric_difference(set([1,2]))
                if diff != set():
                    print("sex bad source: {} year: {} loc: {} age: {}".format(source, year, loc, diff))
        #print(source, (time.time()-start)/60)
    return


def get_two_most_recent_metadata(which_dir):
    """
    Function that returns the two most recent files in
    FILEPATH based on the dates in the
    file names, not the dates the files were created.  User can specify
    which directory inside the main directory to look.  The function
    will look for H5 files with dates of the format YYYY_MM_DD,
    ignoring the file names, and return the files with the two
    most recent dates as pandas DataFrames

    Returns the most current, and the previous

    Example call:
        cur, prev = get_two_most_recent_metadata('summary_statistics/prepare_envelope')

    Parameters:
        which_dir: String
            string that is the file path under the directory 'diagnostics'.
            Don't put a / at the start or the end
    """
    file_path = r"FILEPATH".\
        format(which_dir)

    # check that this is a valid filepath
    assert os.path.isdir(file_path), "'{}' is not an existing filepath".\
        format(file_path)

    # get all the files in the folder
    files = glob.glob("{}*.H5".format(file_path))


    # make a list of only the dates from files
    dates = [x[-13:-3] for x in files]

    # sort the list of dates, from most recent to least:
    dates = sorted(dates, reverse=True)

    assert len(files) > 0, """There are no matching files in
        {}""".format(file_path)

    # get the two most recent dates
    current = dates[0]
    previous = dates[1]

    # get the files corresponding to those two dates
    current_glob = glob.glob("{}/*{}.H5".format(file_path, current))
    previous_glob = glob.glob("{}/*{}.H5".format(file_path, previous))

    # this should only match one file for each glob
    assert len(current_glob) == 1, """more than one file matched with
        the date {} in the folder {}""".format(current, file_path)
    assert len(previous_glob) == 1,  """more than one file matched with
        the date {} in the folder {}""".format(previous, file_path)

    # get the dataframes
    current_metadata = pd.read_hdf(current_glob[0])
    previous_metadata = pd.read_hdf(previous_glob[0])

    current_metadata['current_source_date'] = "{}, {}".format(which_dir, current)
    previous_metadata['previous_source_date'] = "{}, {}".format(which_dir, previous)

    return current_metadata, previous_metadata


def compare_statistics(current, previous, id_cols, col_to_compare,
                       percent_change_allowed, return_stats=False):
    """
    Function that takes in two sets of summary statistics and sees if
    they are more differen than some threshold.  Both sets of stats
    should have the same columns.

    The metric for computing the difference is percent change:
    percent_change = new - old / old * 100. This will result in negative numbers
    which we want, if we want to be able to see the direction that the change
    was in.  For the comparison, the absolute value of the change is used.  That
    is, the comparison is abs(new - old) / old * 100 < percent_change_allowed.


    Parameters:
        current: Pandas DataFrame
            Contains the the most recent verison of stats
        previous: Pandas DataFrame
            Contains the previous version of stats
        id_cols: List of Strings
            List of column names that identify the groups that the statistics
            represent. Will be used to merge the two sets of stats together
        col_to_compare: String
            Name of the column that contains the statistics (this should
            usually be the name of the column that the statistics were computed
            from, e.g., 'val')
        percent_change_allowed: integer or list of integers
            Determines what absolute percent change is okay. Can either be an
            integer, or a list of integers. If a list is used, then it  must
            have 7 entries, corresponding to the statistics 'mean', 'std',
            'min', '25%', '50%', '75%', 'max' and in that order. If one number
            is used then that number will be used for all the stats. For
            example, if the percent_change_allowed for 'mean' is 50 percent,
            then if abs(current mean - previous mean) / previous mean * 100 < 50
            then it's okay.
        return_stats: Boolean
            Switch that if true, will return the comparison of the statistics
    """

    # lots of asserts
    assert set(current.drop('current_source_date', axis=1).columns) ==\
        set(previous.drop('previous_source_date', axis=1).columns), """
        columns must match"""
    assert set(id_cols).issubset(current.columns), """all columns in id_cols
        have to exist"""
    assert "stats" in id_cols, "'stats' must be a column"
    assert set(current['stats'].unique()) == {'mean', 'std', 'min', '25%',
                                              '50%', '75%', 'max'}
    if not isinstance(percent_change_allowed, (list, tuple)):
        percent_change_allowed = [percent_change_allowed]
    assert len(percent_change_allowed) == 1 or\
        len(percent_change_allowed) == 7, """There needs to be 7 values in
        percent_change_allowed, or 1."""
    assert np.all(np.asarray(percent_change_allowed) <= 100), """all values
        in percent_change_allowed should be <= 100"""
    assert np.all(np.asarray(percent_change_allowed) >= 0), """all values in
        percent_change_allowed should be >= 0"""

    threshold = pd.DataFrame({"stats": ['mean', 'std', 'min', '25%', '50%',
                                        '75%', 'max']})
    if len(percent_change_allowed) == 1:
        threshold['percent_change_allowed'] = percent_change_allowed[0]
    else:
        threshold['percent_change_allowed'] = percent_change_allowed

    current = current.rename(columns={col_to_compare:
                                      col_to_compare + "_current"})
    previous = previous.rename(columns={col_to_compare:
                                        col_to_compare + "_previous"})

    stats = previous.merge(current, how='left', on=id_cols)

    # percent change = new - old / old * 100
    stats['percent_change'] = (stats[col_to_compare + "_current"] -
                               stats[col_to_compare + "_previous"]) /\
        stats[col_to_compare + "_previous"] * 100

    stats = stats.merge(threshold, how='left', on='stats')

    # make conditional mask for where threshold is broken
    over_threshold = np.abs(stats.percent_change) > stats.percent_change_allowed

    # subset data once to save computations
    over_stats = stats[over_threshold].copy()

    if over_stats.shape[0] == 0:
        return

    # make some info for the save locations/names and warning message
    fname = over_stats.current_source_date.unique()[0].\
        split(',')[0].split("/")[1]
    date_time = datetime.datetime.today().strftime("%x_%X").\
        replace("/", "_").replace(":", "_")
    save_loc = ("FILEPATH/{}_{}.H5".
                format(fname, date_time))
    warning_message =\
        """

        =======================================================================
                                       WARNING
        =======================================================================

        There are {} rows that are have changed too much out of {} rows
        That's {}% of the rows

        The mean was over in {} rows
        The std was over in {} rows
        The min was over in {} rows
        The 25th percentile was over in {} rows
        The 50th percentile was over in {} rows
        The 75th percentile was over in {} rows
        The max was over in {} rows

        The thresholds were:
        {}
        {}

        The data is saved at:
        {}

        Please remember to delete older files.

        """

    warnings.warn(
        warning_message.format(
            over_stats.shape[0],
            stats.shape[0],
            over_stats.shape[0]/float(stats.shape[0]) * 100,
            over_stats[over_stats['stats'] == 'mean'].shape[0],
            over_stats[over_stats['stats'] == 'std'].shape[0],
            over_stats[over_stats['stats'] == 'min'].shape[0],
            over_stats[over_stats['stats'] == '25%'].shape[0],
            over_stats[over_stats['stats'] == '50%'].shape[0],
            over_stats[over_stats['stats'] == '75%'].shape[0],
            over_stats[over_stats['stats'] == 'max'].shape[0],
            ['mean', 'std', 'min', '25%', '50%', '75%', 'max'],
            percent_change_allowed,
            save_loc
        )
    )

    # save
    stats.to_hdf(save_loc, key='df', complib='blosc', complevel=5, mode='w')
    if return_stats:
        return stats


def write_hosp_file(df, write_path, backup=True, include_version_info=False):
    """
    write and optionally backup a pandas dataframe using our standard HDF
    compression and mode settings

    Parameters:
        df: Pandas DataFrame
            The data to write to file
        write_path: str
            a directory and file name to write to. The directory will be used
            as the parent directory for the _archive folder
        backup: bool
            should the data be backed up in the _archive folder
    """
    # get the next version of data we'll write and the current map we're using
    write_vers = pd.read_csv("FILEPATH")
    write_vers = int(write_vers['version'].max()) + 1

    my_dir = "FILEPATH"
    map_files = glob.glob(my_dir)
    natural_sort(map_files)
    current_map = map_files[0]
    pattern = re.compile(r'FILEPATH')
    current_map_version = pattern.search(current_map).group(1)
    current_map_version

    # create the _archive dir if it doesn't exist
    if not os.path.isdir(os.path.dirname(write_path)):
        os.makedirs(os.path.dirname(write_path))

    if include_version_info:
        write_dir = os.path.dirname(write_path)
        file_name = os.path.basename(write_path)
        ext = os.path.splitext(file_name)[1]
        no_ext = os.path.splitext(file_name)[0]
        write_path = "{}/{}_v{}_mapv{}{}".format(\
            write_dir, no_ext, write_vers, current_map_version, ext)
    # write df to path
    df.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')

    if backup:
        today = datetime.datetime.today().strftime("%Y_%m_%d")
        # write df to path/_archive
        # extract write dir and file name
        main_dir = os.path.dirname(write_path)
        file_name = os.path.basename(write_path)

        archive_dir = main_dir + "/_archive"
        # create the _archive dir if it doesn't exist
        if not os.path.isdir(archive_dir):
            os.makedirs(archive_dir)
        archive_path = archive_dir + "/{}_{}".format(today, file_name)

        # write dated archive file
        df.to_hdf(archive_path, key='df', format='table',
                  complib='blosc', complevel=5, mode='w')

    return


def swap_ecode(df, icd_vers=10):
    """
    Swap the ecodes and n codes from a dataset with multiple levels of
    diagnoses stored wide

    """
    assert icd_vers in [9, 10], "That's not an acceptable ICD version"

    if icd_vers == 10:
        ncodes = ["S", "T"]
        ecodes = ["V", "W", "X", "Y"]
    if icd_vers == 9:
        ncodes = ["8", "9"]
        ecodes = ["E"]

    # set the nature code conditions outside the loop cause it's always dx 1
    nature_cond = " | ".join(["(df['dx_1'].str.startswith('{}'))".format(ncond)
                             for ncond in ncodes])
    # create list of non primary dx levels
    col_list = list(df.filter(regex="^(dx_)").columns.drop("dx_1"))
    # sort them from low to high
    natural_sort(col_list)
    col_list.reverse()
    for dxcol in col_list:

        ecode_cond = " | ".join(["(df['{}'].str.startswith('{}'))".
                                format(dxcol, cond) for cond in ecodes])
        # swap e and n codes
        df.loc[(df[dxcol].notnull()) & eval(nature_cond) & eval(ecode_cond),
               ['dx_1', dxcol]] =\
               df.loc[(df[dxcol].notnull()) & eval(nature_cond) & eval(ecode_cond),
                      [dxcol, 'dx_1']].values
    return df

def select_bundle_subset(df, bundles="all"):
    """
    We are sometimes tasked with processing only a subset of our data for
    a few bundle IDs.

    Parameters:
        df: Pandas DataFrame
            hospital data mapped to bundle id
        bundles: a list or Pandas Series
            consists of a list of bundles which should be included going forward
            ie the func will drop everything not in this list
    """

    if bundles == "all":
        bundles = df.bundle_id.unique()
        assert isinstance(bundles, np.ndarray)

    assert isinstance(bundles, np.ndarray) or\
            isinstance(bundles, list) or \
            isinstance(bundles, pd.Series),\
            "The wrong type of data was passed to bundles. "\
            "Bundles is type {}, it should be a list".format(type(bundles))
    # subset out just the bundles we want.
    df = df[df.bundle_id.isin(bundles)]

    return df


def compile_master_data():
    """
    Function that reads in all the source specific files can compiles them
    """
    
    files = glob.glob("FILEPATH/*.H5")

    for f in files:
        assert_msg = "{} isn't a file; the glob is misconfigured".format(f)
        assert os.path.isfile(f), assert_msg

    num_files = len(files)
    completed = 0

    df_list = []
    sources_read_in = []  # to keep track of what sources have been read in

    print("Beginning to read in files...")
    for f in files:
        print("Reading in {}...".format(f))

        df = pd.read_hdf(f) # read in one file

        # it should have one source
        assert len(df.source.unique()) == 1, "There's a file with more than one source and this code needs that not to happen"

        source = df.source.unique()[0]

        # a source shouldn't be read in more than once!
        assert source not in sources_read_in, "The source {} has already been read in.  Check the directory for duplicated files."

        sources_read_in.append(source)  # keep track of what has been read

        df_list.append(df)  # add to the list of dataframes

        completed += 1
        print("{:.4}% completed".format(float(completed) / float(num_files) * 100))

    print("Finished reading in files.  Concatenating...")
    df = pd.concat(df_list, ignore_index=True)  # concatenate all dataframes
    print("All done.")
    return df


def check_missing_ms_bundles():
    """
    check for missing bundles in the MS process

    """

    parent_dir = "FILEPATH"
    
    inc = "FILEPATH"
    prev = "FILEPATH"
    
    inc = glob.glob(inc)
    prev = glob.glob(prev)
    
    files = inc + prev
    
    bundles = []
    for f in files:
        f = os.path.basename(f)[:-4]
        bundles.append(f)
    
    bundles = pd.Series(bundles).unique().tolist()
    
    write_dir = r"FILEPATH"
    files = glob.glob(write_dir, recursive=True)
    
    written_bundles = []
    for f in files:
        f = os.path.basename(f)[3:8]
        written_bundles.append(f)
    
    written_bundles = pd.Series(written_bundles).unique().tolist()
    written_bundles = pd.Series(written_bundles).str.extract("(\d+)")

    diffs = set(bundles) - set(written_bundles)
    return diffs

def fix_col_dtypes(df, errors="raise"):
    """
    Takes a dataframe with mixed types and returns them with numeric type
    
    Parameters
        df: (pandas DataFrame) of hospital data
        errors: (str) a string to pass to pd.numeric to raise, coerce or ignore errors 
    """

    num_cols = ['sex_id', 'location_id', 'year_start', 'year_end', 'bundle_id',
                'nid', 'representative_id']
    # drop the cols that aren't in the data
    num_cols = [n for n in num_cols if n in df.columns]

    if len(num_cols) < 1:
        fail_msg = """
        There are not any of our expected columns in the dataframe. The columns in
        df are {}
        """.format(df.columns)
        assert False, fail_msg
    
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors=errors)

    return df


def find_bad_env_locs(break_if_bad=True):
    """
    reads through all the split envelope files and checks if the draws for any locations
    are completely missing. Trips an assert or returns a dictionary with a filepath and a
    list of all the bad location_ids
    """
    bad_locs_files = {}
    files = glob.glob("FILEPATH/*.H5")
    bad_loc_count = 0
    for f in files:
        se = pd.read_hdf(f)
        draws = se.filter(regex="^draw_").columns
        bad_locs = []
        for loc in se.location_id.unique():
            if se.loc[se.location_id == loc, draws].isnull().all().all():
                bad_locs.append(loc)
                bad_loc_count += 1

        bad_locs_files[f] = bad_locs
    if break_if_bad:
        assert bad_loc_count == 0, "at least 1 location is bad. Re-run with break_if_bad set to False"
    return bad_locs_files


def sizeof_fmt(num, suffix='B'):
    """
    convert bit size in Python outputs to human readable bytes
    Parameters
        num: (numeric) The number to be converted from bits to bytes

    """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def mem_hogs(n=10):
    """
    return a dataframe of the largest objects in memory

    Parameters
        n: (int) number of rows/objects to return
    """
    all_obj =globals()

    object_name = list(all_obj)
    object_size = [sys.getsizeof(all_obj[x]) for x in object_name]

    d = pd.DataFrame(dict(name = object_name, size = object_size))
    d.sort_values(['size'], ascending=[0],inplace=True)
    d['size'] = d['size'].apply(sizeof_fmt)

    return(d.head(n))
