# hospital module
import pandas as pd
import numpy as np
import platform
import itertools
import re
import datetime
import warnings

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

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
    column named 'year'
    """
    assert 'year_start' in df.columns, "DataFrame doesn't have a 'year_start' column"
    df['nid'] = df['year_start'].map(nid_dict)
    return df


def age_binning(df):
    """
    function accepts a pandas DataFrame that contains age-detail and bins them
    into age ranges. Assumes that the DataFrame passed in contains a column
    named 'age' Drops the 'age' column out of the dataframe and returns the
    DataFrame with two age_group columns and drops age-detail.

    Example: 32 would become 30, 34

    Example call: df = age_binning(df)

    The bins are [0, 1, 4, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70,
    75, 80, 85, 90, 95, 100, 105, 110, 115]

    This function assumes no one will be older than 115 years old.
    """

    df['age'] = pd.to_numeric(df['age'], errors='raise', downcast='integer')

    # bins that age is sorted into
    age_bins = np.append(np.array([0, 1, 4]), np.arange(10, 116, 5))  # 0, 1, 4
    # do not follow the 5 year bin pattern.

    # labels for age columns are the lower and upper ages of bin
    age_start_list = np.append(np.array([0, 1]), np.arange(5, 111, 5))
    age_end_list = np.append(np.array([1]), np.arange(4, 116, 5))
    # Create 2 new age columns
    df['age_start'] = pd.cut(df['age'], age_bins, labels=age_start_list,
                             right=False)
    df['age_end'] = pd.cut(df['age'], age_bins, labels=age_end_list,
                           right=False)
    # Drop age variable
    # df.drop('age', 1, inplace=True)

    # make age_start and age_end numeric, for some reason binning makes them
    # categorical dtype
    df['age_start'] = pd.to_numeric(df['age_start'], errors='raise', downcast='integer')
    df['age_end'] = pd.to_numeric(df['age_end'], errors='raise', downcast='integer')

    # return dataframe with age_start,age_end features
    return (df)


def year_range(df):
    """
    Accepts a pandas DataFrame. Returns the lowest and highest values.
    Assumes that the DataFrame contains a column named 'years'
    Assumes that there is a column that contains all the years.
    Meant to be used to form year_start and year_end columns in a DataFrame.
    Drops the 'years' column out of the dataframe.
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
    Also compares values before and after the stack to verify that the stack
    worked.
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
    first 5000 rows. You can choose to read more or less with the other
    arguments
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


def samp_prep(df, seed=123, frac=0.01):
    """
    Takes a DataFrame, random number seed, and sample fraction as arguments.
    Returns a (dUSERt) 1% sample of that dataframe with ordered columns and
    will map location_id to iso3 (eventually)
    """
    # make a sample
    n = int(round(df.shape[0] * frac, 0))
    df = df.sample(n=n, random_state=seed)

    # order of columns
    cols = ['location_id', 'iso3',
            'year_start', 'year_end',
            'age_group_unit', 'age_start', 'age_end',
            'sex_id',
            'source', 'nid',
            'facility_id',
            'diagnosis_id', 'code_system_id', 'metric_id', 'outcome_id',
            'val', 'cause_code', 'nonfatal_cause_name',
            'cause_fraction']
    # reorder
    df = df[cols]
    return df


def create_cause_fraction(df, maternal=False):
    """
    Takes a formatted, compiled, mapped to NF_cause dataframe of hospital data
    and returns a cause_fraction column outlined below
    """
    # select groupby features for the denominator
    denom_list = ['age_start', 'age_end', 'sex_id', 'year_start',
                  'year_end', 'location_id']

    # select groupby features for the numerator. include source and
    # nid
    numer_list = ['age_start', 'age_end', 'sex_id', 'year_start',
                  'year_end', 'location_id', 'source', 'nid',
                #   'bundle_id']  # cause fracs now at bundle level
                  'nonfatal_cause_name']  # cause fracs back at nfc level
    if not maternal:
        # compute cause fractions
        df["numerator"] = df.groupby(numer_list)['val'].transform("sum")
        df["denominator"] = df.groupby(denom_list)['val'].transform("sum")
        df['cause_fraction'] = df['numerator'] / df['denominator']

        # check proportions sum to one
        test_sum = df[df.val > 0].groupby(denom_list)\
            .agg({'cause_fraction': 'sum'}).reset_index()
        np.testing.assert_allclose(actual=test_sum['cause_fraction'], desired=1,
                                   rtol=1e-7, atol=0,
                                   err_msg="Proportions do not sum to 1 within tolerance",
                                   verbose=True)
        # write sample size to J temp
        samp = df[['age_start', 'age_end', 'sex_id', 'year_start',
                  'year_end', 'location_id', 'denominator']].copy()
        samp.drop_duplicates(inplace=True)
        samp.to_csv(root + "FILEPATH", index=False)

    if maternal:
        # want df to keep all the columns that it started with
        df = df.groupby(numer_list)['val'].transform("sum")

        # rename
        df.rename(columns={'val':'numerator'}, inplace=True)

        # do not want denom_df to have all the columns df has.
        denom_df = df[df.use_in_maternal_denom == 1].groupby(denom_list).\
        agg({'val': 'sum'}).reset_index(drop=False)
        denom_df.rename(columns={'val': 'denominator'}, inplace=True)
        df = df.merge(denom_df, how='left', on=[denom_list])

    # no cause fractions above 1
    assert df.cause_fraction.max() <= 1,\
        "there are fractions that are bigger than 1"

    return(df)

def year_binner(df):
    """
    create 5 year bands for dismod, 88-92, 93-97, etc
    bins that years are sorted into
    """
    year_bins = np.arange(1988, 2019, 5)

    # labels for year columns are the start and end years of bins
    year_start_list = np.arange(1988, 2017, 5)

    # rename year columns
    df.rename(columns={'year_start': 'og_year_start',
                       'year_end': 'og_year_end'}, inplace=True)

    # Create 2 new year columns
    df['year_start'] = pd.cut(df['og_year_start'], year_bins,
                              labels=year_start_list, right=False)
    df['year_start'] = pd.to_numeric(df['year_start'], downcast='integer')
    df['year_end'] = df['year_start'] + 4

    assert (df['og_year_start'] >= df['year_start']).all()
    assert (df['og_year_start'] <= df['year_end']).all()
    assert (df['og_year_end'] >= df['year_start']).all()
    assert (df['og_year_end'] <= df['year_end']).all()
    # drop original year columns
    df.drop(['og_year_start', 'og_year_end'], axis=1, inplace=True)
    return(df)


def five_year_nids(df):
    """
    Created on Wed Feb 15 09:32:16 2017
    When creating 5 year bands we need new NIDs to fit with these new bands
    This script reads and processes the merged NID list

    """
    # read NID lists
    nid = pd.read_excel(root + r"FILEPATH")
    nid2 = pd.read_excel(root + r"FILEPATH")
    nid3 = pd.read_excel(root + r"FILEPATH")

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

    nid['new_nid'] = pd.to_numeric(nid['new_nid'], downcast='integer')

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

    # this needs to be checked everytime new multi-year sources are added
    # it is currently hard coded to add data for EU HMDB
    df.loc[(df.new_nid.isnull()) & (df.source == 'EUR_HMDB'), 'new_nid'] = 3822
    # manually add phl health 5 year nid

    # check if new_nid is null
    assert df.new_nid.isnull().sum() == 0,\
    "there are missing nids for {}".format(df[df.new_nid.isnull()].source.unique())
    # drop (old) nid
    df.drop('nid', axis=1, inplace=True)
    # rename new nid
    df.rename(columns={'new_nid': 'nid'}, inplace=True)

    return(df)

def hosp_check(df):
    """
    final check before writing data.
    """
    # every 5 year group present
    assert len(df['year_start'].unique()) == 6
    assert len(df['year_end'].unique()) == 6

    # every age group present
    assert len(df['age_start'].unique()) == 21
    assert len(df['age_end'].unique()) == 21
    print("unique ages and years were found")

    # there are only two types of representativeness
    assert len(df['representative_name'].unique()) == 2
    assert (df['representative_name'].sort_values().unique() == ('Nationally representative only', 'Not representative',)).all()
    print("representative names verified")
    # all columns are present
    ordered = ['bundle_id', 'bundle_name', 'measure',
               'location_id', 'location_name', 'year_start', 'year_end',
               'age_start',
               'age_end', 'sex', 'nid',
               'representative_name',
               # 'mean', 'lower', 'upper', 'correction_factor', ##
               # 'mean_uncorrected', 'upper_uncorrected', 'lower_uncorrected', ##
               'mean_0', 'lower_0', 'upper_0',
               'mean_1', 'lower_1', 'upper_1', 'correction_factor_1',
               'mean_2', 'lower_2', 'upper_2', 'correction_factor_2',
               'mean_3', 'lower_3', 'upper_3', 'correction_factor_3',
               'mean_inj', 'lower_inj', 'upper_inj', 'correction_factor_inj',
               'sample_size',
               'source_type', 'urbanicity_type',
               'recall_type', 'unit_type', 'unit_value_as_published',
               'cases', 'is_outlier', 'seq', 'underlying_nid',
               'sampling_type', 'recall_type_value', 'uncertainty_type',
               'uncertainty_type_value', 'input_type', 'standard_error',
               'effective_sample_size', 'design_effect', 'response_rate',
               'extractor']

    assert set(df.columns).symmetric_difference(set(ordered))==set()
    print("all the columns we expect were found")

    # there should be no duplicates for demography + bundle_id
    assert df[df[['age_start', 'age_end', 'year_start', 'year_end', 'location_id', 'sex', 'bundle_id']].duplicated(keep=False)].shape[0] == 0
    print("there are no duplicates for any demographic groups")

    return("All Checks Passed")


def create_ms_durations(df):
    """
    a function to write a new durations file for market scan incidence
    causes
    takes our clean_maps_`x' file and outputs a new durations file
    """

    # read in baby sequela measures and durations
    nfc_measures = pd.read_excel(root +
                                r"FILEPATH")

    # keep and rename useful cols
    nfc_measures = nfc_measures[["Baby Sequelae  (nonfatal_cause_name)",
                               "level 1 measure"]]
    nfc_measures.rename(columns={"Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
                                "level 1 measure": "measure"}, inplace=True)
    # drop null baby sequela
    nfc_measures = nfc_measures[nfc_measures.nonfatal_cause_name.notnull()]
    # convert to lower case
    nfc_measures['nonfatal_cause_name'] =\
        nfc_measures['nonfatal_cause_name'].str.lower()
    # drop duplicated baby sequela
    nfc_measures.drop_duplicates(['nonfatal_cause_name'], inplace=True)
    # durations and measures are in the same column, split them into 2
    nfc_measures['nfc_measure'], nfc_measures['duration'] =\
        nfc_measures.measure.str.split("(").str
    # remove non alphanumeric from durations
    nfc_measures.duration = nfc_measures.duration.str.replace("\W", "")
    # convert duration column to float (NaNs are present)
    nfc_measures.duration = pd.to_numeric(nfc_measures.duration,
                                         downcast='float')
    nfc_measures.drop(['measure'], axis=1, inplace=True)

    # pull in nfc ID from clean maps using a left join with clean on the left
    # we'll use any baby sequela which aren't in clean maps. This is fine
    # b/c they don't have ICD codes associated with them
    df = df[['nonfatal_cause_name', 'nfc_id']]
    df.drop_duplicates('nonfatal_cause_name', inplace=True)
    nfc_measures = df.merge(nfc_measures, how='left', on='nonfatal_cause_name')
    # new measures and durations are now prepped!

    # read in existing nfc duration file
    durations = pd.read_excel(root + r"FILEPATH")

    # outer merge with all the new durations
    new_dur = nfc_measures.merge(durations, how='outer', on=['nonfatal_cause_name', 'nfc_id'])

    # check where values exist for both durations
    check = new_dur[(new_dur.duration_x.notnull()) & (new_dur.duration_y.notnull())].copy()
    assert (check['duration_x'] == check['duration_y']).all()

    # create new duration vector and assign values from dur x and y
    new_dur['duration'] = new_dur['duration_y']
    # now only where a value from y doesn't exist
    new_dur.loc[new_dur['duration_y'].isnull(), 'duration'] =\
        new_dur.loc[new_dur['duration_y'].isnull(), 'duration_x']
    # assign prev cases durations of 365 days
    new_dur.loc[new_dur['nfc_measure'] == 'prev', 'duration'] = 365

    new_dur.drop(['measure', 'duration_x', 'duration_y'],
                 axis=1, inplace=True)
    new_dur.rename(columns={'nfc_measure': 'measure'}, inplace=True)

    # write to drive
    writer = pd.ExcelWriter(root + r"FILEPATH",
                            engine='xlsxwriter')
    new_dur.to_excel(writer, sheet_name="Sheet 1", index=False)
    writer.save()
    # backup to the archive
    backup_writer = pd.ExcelWriter("FILEPATH")
    new_dur.to_excel(backup_writer, sheet_name="Sheet 1", index=False)
    backup_writer.save()

def keep_bundles(df, keep):
    """
    function that returns a dataframe with only the bundle_ids specified in the list keep

    Parameters:
    df : Dataframe
        a pandas Dataframe with all your data, has to be at the bundle level
    keep: list-like
        a list the contains the bundles you want to keep
    """

    assert "bundle_id" in df.columns, "There isn't a column named 'bundle_id'"

    print("Keeping only the bundles {}".format(keep))
    print("that's {} out of {} bundles".format(str(len(keep)), str(len(df.bundle_id.unique()))))
    print("Rows before dropping: {}".format(str(df.shape[0])))
    df = df[df.bundle_id.isin(keep)]
    print("Done dropping, there are now {} rows".format(str(df.shape[0])))
    return(df)

def drop_data(df, verbose=True):
    """function that drops the data that we don't want. reasons are
    outlined in comments"""


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
    # our envelope is only inpatient so we only keep inpatient when using this func
    df = df[(df['facility_id'] == 'inpatient unknown') |
            (df['facility_id'] == 'hospital')]
    if verbose:
        print("DROPPING OUTPATIENT")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop secondary diagnoses
    df = df[df['diagnosis_id'] == 1]
    if verbose:
        print("DROPPING ALL SECONDARY DIAGNOSES")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # drop canada data, they mix up inpatient/outpatient
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
    Function that returns the age_group_ids that we use on the
    hosital prep team.  Use it to merge on age_group_id onto your data, or
    to merge age_start and age_end onto some covariate or model output
    """
    age = pd.DataFrame({"age_start":[ 0,  1,  5, 10, 15, 20, 25, 30, 35, 40,
                        45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
                     "age_end": [ 1,  4,  9, 14, 19, 24, 29, 34, 39, 44, 49,
                        54, 59, 64, 69, 74, 79, 84, 89, 94, 99],
                     "age_group_id": [ 28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                        15, 16, 17, 18, 19, 20, 30, 31, 32, 235]})
    age = age[['age_start', 'age_end', 'age_group_id']]

    return(age)


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
    # bundles = df.bundle_id.unique()
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
                     "age_end": [ 1,  4,  9, 14, 19, 24, 29, 34, 39, 44, 49,
                        54, 59, 64, 69, 74, 79, 84, 89, 94, 99],
                     "age_group_id": [ 28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                        15, 16, 17, 18, 19, 20, 30, 31, 32, 235]})
    age = age[['age_start', 'age_end', 'age_group_id']]

    final_df = final_df.merge(age, how='left', on='age_group_id')
    return(final_df)


def make_maternal_denom():
    """
    Make the # of cases we will use in the denominator for the maternal causes
    use the ICD 9 and 10 maps that USERNAME sent to get ICD codes for denom
    This reads in all of our data so it will be relatively slow
    """

    # read in master data or the equivalent
    df = pd.read_csv(root + r"FILEPATH",
                     dtype={'cause_code': object})
    # drop data
    df = drop_data(df, verbose=False)
    df['cause_code'] = df['cause_code'].astype(str)
    df['cause_code'] = sanitize_diagnoses(df['cause_code'])

    # merge clean_maps onto the data to properly duplicate columns
    path_to_map = r"FILEPATH"
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
    icd9 = pd.read_excel(root + r"FILEPATH",
                         converters={'icd_code': str})
    icd9['code_system_id'] = 1
    icd10 = pd.read_excel(root + r"FILEPATH",
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
    mat_df.to_csv(root + r"FILEPATH", index=False)
    mat_df.to_csv("FILEPATH_backup", index=False)


def apply_restrictions(df, col_to_restrict, drop_restricted=True):
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
    """

    assert set(['nonfatal_cause_name', 'age_start', 'age_end',
                'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
            format(df[col_to_restrict].isnull().sum()))

    # store columns that we started with
    start_cols = df.columns

    restrict = pd.read_excel(root + "FILEPATH", sheetname='NE to ME1')
    restrict = restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    restrict = restrict.reset_index(drop=True)
    restrict.rename(columns={'Baby Sequelae  (nonfatal_cause_name)':
                             'nonfatal_cause_name'}, inplace=True)
    restrict['nonfatal_cause_name'] =\
        restrict['nonfatal_cause_name'].str.lower()
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict = restrict.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.  This is necessary, there is a age_start = 0.1 years
    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict, how='left', on='nonfatal_cause_name')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")

    # set col_to_restrict to NaN where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), col_to_restrict] = np.nan

    # set col_to_restrict to NaN where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), col_to_restrict] = np.nan

    # set col_to_restrict to NaN where age end is smaller than yld age start
    df.loc[df['age_end'] < df['yld_age_start'], col_to_restrict] = np.nan

    # set col_to_restrict to NaN where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], col_to_restrict] = np.nan

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)

    if drop_restricted:
        print("Rows before dropping restricted {}".format(df.shape[0]))
        # drop the restricted values
        df = df[df[col_to_restrict].notnull()]
        print("Rows after dropping restricted {}".format(df.shape[0]))

    assert set(start_cols) == set(df.columns)

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
    """

    assert set(['bundle_id', 'age_start', 'age_end',
                'sex_id', col_to_restrict]) <=\
        set(df.columns), "you're missing a column"

    if df[col_to_restrict].isnull().sum() > 0 and drop_restricted:
        warnings.warn("There are {} rows with null values which will be dropped".\
            format(df[col_to_restrict].isnull().sum()))

    # store columns that we started with
    start_cols = df.columns

    restrict = pd.read_csv(root + "FILEPATH")

    restrict = restrict.reset_index(drop=True)

    restrict = restrict[restrict.bundle_id.notnull()]
    restrict = restrict.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.  This is necessary, there is a age_start = 0.1 years
    restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'] = 0

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict, how='left', on='bundle_id')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")

    # set col_to_restrict to NaN where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), col_to_restrict] = np.nan

    # set col_to_restrict to NaN where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), col_to_restrict] = np.nan

    # set col_to_restrict to NaN where age end is smaller than yld age start
    df.loc[df['age_end'] < df['yld_age_start'], col_to_restrict] = np.nan

    # set col_to_restrict to NaN where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], col_to_restrict] = np.nan

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)

    if drop_restricted:
        print("Rows before dropping restricted {}".format(df.shape[0]))
        # drop the restricted values
        df = df[df[col_to_restrict].notnull()]
        print("Rows after dropping restricted {}".format(df.shape[0]))

    assert set(start_cols) == set(df.columns)

    return(df)

def check_parent_injuries(df, col_to_sum):
    pc_injuries = pd.read_csv(root + r"FILEPATH")
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
        assert round(parent_sum, 5) == round(child_sum, 5),\
           "The parent and child " + col_to_sum + " don't match"
    print("Parent injury " + col_to_sum + " sums equal child injury case sums")


def make_zeroes(df, level_of_analysis, cols_to_square, icd_len=5):
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

    cause_type = df[level_of_analysis].unique()

    for loc in df.location_id.unique():
        # this isn't relevant to injuries now
        if icd_len == 3:
            dat = pd.DataFrame(expandgrid(ages, sexes, [loc],
                                          df[df.location_id == loc].
                                          year_start.unique(), cause_type))
            dat.columns = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                           level_of_analysis]
        elif icd_len > 3:
            dat = pd.DataFrame(expandgrid(ages, sexes, [loc],
                                  df[df.location_id == loc].
                                  year_start.unique(),
                                  #df[df.location_id == loc].
                                  #facility_id.unique(),
                                  #df[df.location_id == loc].
                                  #representative_id.unique(),
                                  cause_type))
            dat.columns = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                           #'facility_id', 'representative_id',
                           level_of_analysis]
        sqr_df.append(dat)

    sqr_df = pd.concat(sqr_df)
    sqr_df['year_end'] = sqr_df['year_start']

    age = pd.DataFrame({"age_start":[ 0,  1,  5, 10, 15, 20, 25, 30, 35, 40,
                        45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
                     "age_end": [ 1,  4,  9, 14, 19, 24, 29, 34, 39, 44, 49,
                        54, 59, 64, 69, 74, 79, 84, 89, 94, 99],
                     "age_group_id": [ 28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                        15, 16, 17, 18, 19, 20, 30, 31, 32, 235]})
    age = age[['age_start', 'age_end', 'age_group_id']]

    sqr_df = sqr_df.merge(age, how='left', on='age_group_id')

    # get a key to fill in missing values for newly created rows
    missing_col_key = df[['location_id', 'year_start', 'facility_id', 'nid', 'representative_id', 'source']].copy()
    missing_col_key.drop_duplicates(inplace=True)

    # inner merge sqr and missing col key to get all the column info we lost
    final_df = sqr_df.merge(missing_col_key, how='inner', on=['location_id', 'year_start'])

    # left merge on our built out template df to create zero rows
    final_df = final_df.merge(df, how='left', on=['age_group_id', 'age_start', 'age_end',
                                          'sex_id', 'location_id', 'year_start',
                                          'year_end', level_of_analysis, 'facility_id',
                                          'nid', 'representative_id',
                                          'source'])
    # fill missing values of the col of interest with zero
    for col in cols_to_square:
        final_df[col] = final_df[col].fillna(0)

    return(final_df)

def hosp_splitter(df, directory):
    """split the hospital data into smaller files by source and year"""
    sources = df.source.unique()
    for datasource in sources:
        for year in df[df['source'] == datasource].year_start.unique():
            dat = df[(df.source == datasource) & (df.year_start == year)]
            try:
                dat.to_hdf(directory + datasource + "_" + str(year) + ".H5",
                  key='df', complib='blosc', complevel=5)
            except:
                filename = "FILEPATH"
                logging.basicConfig(filename=filename, level=logging.DEBUG)


def infer_dtypes(df):
    return df.apply(lambda x: pd.lib.infer_dtype(x.values))
