import pandas as pd
import numpy as np
import sys
import os
import time
import datetime
from getpass import getuser
# import ipdb

# load our functions
from clinical_info.Functions import gbd_hosp_prep, hosp_prep, stage_hosp_prep
from clinical_info.Functions import cached_pop_tools
from clinical_info.Functions.live_births import live_births


def write_file_metadata(filepath, run_id, source):
    """First attempt at tracking _exactly_ which files were used in a given run
    This will store some stat metadata like the date it was created, modified,
    filesize in bytes and stuff which should be good to identify an exact file
    using the archives"""

    stat = os.stat(filepath)

    # Parse the stat results so each stat is tied to a column name
    col_dict = {'filepath': filepath, 'source': source}

    for e in str(stat).split("(")[1].split(","):
        spl = e.split("=")
        name = spl[0].replace(" ", "")
        val = spl[1].replace(")", "")
        col_dict[name] = val

    # I'm sorry but I love dataframes
    tmp = pd.DataFrame(col_dict, index=[0])

    write_path = FILEPATH

    # we often do multiple master_data runs, so don't overwrite any metadata
    if os.path.exists(write_path):
        existing_df = pd.read_csv(write_path)
        tmp = pd.concat([tmp, existing_df], sort=False, ignore_index=True)
    tmp.to_csv(write_path, index=False)

    return


def aggregate_neonatal_groups(df, ftype):
    """
    Args:
        df: (DataFrame) clinical data, with or without neonatal age groups
                If the groupby runs on non-neonatal grouped data it shouldn't
                adjust the data in any way. use compare_df to verify.

    Breaks if the data admission counts change at all
    """
    assert df.isnull().sum().sum() == 0, "These null values will break the groupby"
    compare_df = df.copy()

    if ftype == 'h5':
        sumcol = 'val'
    elif ftype == 'stata':
        sumcol = 'cases'
    else:
        assert False, "We need a column name to sum"

    # remove neonatal start and end values
    df.loc[df['age_start'] < 1, 'age_start'] = 0
    df.loc[df['age_end'] <= 1, 'age_end'] = 1

    # set the groups and collapse/sum
    groups = df.columns.drop(sumcol).tolist()
    df = df.groupby(groups).agg({sumcol: 'sum'}).reset_index()

    test_results = stage_hosp_prep.test_case_counts(df, compare_df)
    if test_results == "test_case_counts has passed!!":
        pass
    else:
        msg = " --- ".join(test_results)
        assert False, msg
    return df


def mc_file_reader(source, ftype, verbose, head, cols_to_confirm, remove_neonatal, run_id):
    """
    A helper function that reads in Stata (.DTA) and .H5 files

    Args:
        source: (str)  defines the file path and file name of the file to be
            read.
        ftype: (str)  Defines the file type.  Must be one of "stata" or "h5"
        verbose: (bool) If True prints when finished reading the file.
        head: (bool) If True reads only the first 1000 rows of the file -
            Useful for faster debugging
        cols_to_confirm: (list) A set of columns which should be present in
            the data that is being read in
    Returns:
        Pandas DataFrame

    Raises:
        AssertionError:
            If ftype isn't of type str
            If ftype isn't one of "stata" or "h5"
    """

    assert isinstance(ftype, str), "ftype must be of type str"
    ftype = ftype.lower()  # normalize input
    assert ftype in ["stata", "h5"], "ftype must be one of 'stata' or 'h5'"

    # both branches of these if statemnts return
    if ftype == "stata":
        if verbose:
            print("Start reading {}".format(source))

        filepath = FILEPATH.format(source, source)
        write_file_metadata(filepath=filepath, run_id=run_id, source=source)

        if head:
            new_df = hosp_prep.read_stata_chunks(filepath, chunksize=1000,
                                                 chunks=1)
        else:
            new_df = pd.read_stata(filepath, preserve_dtypes=False)

        cols_to_drop = ['metric_bed_days', 'metric_day_cases', 'iso3',
                        'patient_id', 'subdiv']
        for col in cols_to_drop:
            if col in new_df.columns:
                new_df.drop(col, axis=1, inplace=True)

        if verbose:
            print("Finished reading {}".format(source))

    if ftype == "h5":
        if verbose:
            print(source)

        filepath = (FILEPATH).format(source, source)
        write_file_metadata(filepath=filepath, run_id=run_id, source=source)

        if head:
            new_df = pd.read_hdf(filepath, key="df", start=0, stop=1000)
        else:
            new_df = pd.read_hdf(filepath, key="df")

    if remove_neonatal:
        # remove the neonatal age groups from our standard process,
        # so we have the 0-1 age group
        new_df = aggregate_neonatal_groups(new_df, ftype)

    # sometimes sources have a column "deaths", sometimes they don't
    diff = set(cols_to_confirm).symmetric_difference(set(new_df.columns))
    assert diff == {'deaths'} or diff == set(),\
        "The diff is {}".format(diff)

    return new_df


def read_old_data(source, clinical_age_group_set_id, remove_neonatal, run_id, verbose=True, head=True):
    """
    Function to read in (some but not all) data prepped in GBD 2015.  All of
    these sources are Stata DTA files.

    Args:
        Both are inherited from the main function

        verbose: (bool) If true will print progress and information
        head: (bool) If true will only grab first 1000 rows of each source.

    Returns:
        Pandas DataFrame containing DTA sources
    """

    ###########################################
    # READ AND PREP 2015 GBD DATA
    ###########################################
    print("Reading gbd2015 hospital data...")

    confirm_cols = ['diagnosis_id', 'national', 'source', 'NID', 'year', 'age_start',
                    'sex', 'platform', 'icd_vers', 'cause_code', 'cases', 'deaths',
                    'age_end', 'location_id']

    if source == 'USA_NAMCS':

        # TODO why this file and not the one in 06_hospital?
        # read USA NAMCS separately
        us_filepath = (FILEPATH)
        write_file_metadata(filepath=us_filepath, run_id=run_id, source=source)

        df = pd.read_stata(us_filepath, preserve_dtypes=False)

        # drop subdiv and iso3 columns
        df.drop(['subdiv', 'iso3'], axis=1, inplace=True)

        # There is some formatting that happens while reading in sources
        # rename cols to fit current rubric
        df.rename(columns={'dx_mapped_': 'cause_code',
                           'dx_ecode_id': 'diagnosis_id'}, inplace=True)

        df = format_old_data(df)

    else:
        df = mc_file_reader(source=source, cols_to_confirm=confirm_cols, verbose=verbose,
                            head=head, ftype="stata", remove_neonatal=remove_neonatal, run_id=run_id)

        df = format_old_data(df)

    # replace the dummy placeholder values(-99) from the formatting scripts
    df.loc[df['age_start'] < 0, ['age_start', 'age_end']] = [0, 125]

    df = format_data(df, clinical_age_group_set_id)

    return df


def read_new_data(source, remove_neonatal, verbose=True, head=True):
    """
    Read in data formatted after GBD 2015.  All these sources are H5 files.

    Args:
        Both are inherited from the main function

        verbose: (bool) If true will print progress and information
        head: (bool) If true will only grab first 1000 rows of each source.

    Returns:
        Data from the H5 files
    """
    # Expected cols needs to be cols from old data sources.
    expected_cols = ['age_group_unit', 'age_start', 'age_end', 'year_start',
                     'year_end', 'location_id', 'representative_id', 'sex_id',
                     'diagnosis_id', 'metric_id', 'outcome_id', 'val', 'source',
                     'nid', 'facility_id', 'code_system_id', 'cause_code']

    filepath = FILEPATH.format(source, source)
    write_file_metadata(filepath=filepath, run_id=run_id, source=source)

    if head:
        new_df = pd.read_hdf(filepath, key="df", start=0, stop=1000)
    else:
        new_df = pd.read_hdf(filepath, key="df")

    if remove_neonatal:
        # remove the neonatal age groups from our standard process,
        # so we have the 0-1 age group, instead of groups in days
        new_df = aggregate_neonatal_groups(new_df, ftype='h5')

    diff = set(expected_cols).symmetric_difference(set(new_df.columns))
    assert diff == {'deaths'} or diff == set(),\
        "The diff is {}".format(diff)

    return new_df


def format_data(df, clinical_age_group_set_id, downsize=False):
    """
    Formatting both old and new data once they've been brought together

    Args:
        df: (Pandas DataFrame) Contains old and new data concatenated together
            with source specific formatting done
        downsize: (bool) If True will convert datatypes and downcast to smaller
            dtypes when possible to save space

    Returns:
        Pandas DataFrame with formatted data
    """
    ###########################################
    # Format Data
    ###########################################

    # standardize unknown sexes
    df.loc[(df.sex_id != 1) & (df.sex_id != 2), 'sex_id'] = 3

    # Reorder columns
    hosp_frmat_feat = ['location_id', 'year_start', 'year_end',
                       'age_group_unit', 'age_start', 'age_end', 'sex_id',
                       'source', 'nid', 'representative_id', 'facility_id',
                       'code_system_id', 'diagnosis_id', 'cause_code',
                       'outcome_id', 'metric_id', 'val', 'deaths']

    columns_before = df.columns
    df = df[hosp_frmat_feat]  # reorder
    assert set(columns_before) == set(df.columns),\
        "You accidentally dropped a column while reordering"

    # enforce some datatypes
    df['cause_code'] = df['cause_code'].astype('str')

    if downsize:
        print("Converting datatypes and downsizing...")
        # downsize. this cuts size of data in half.

        # list of columns with integer contents
        int_list = ['location_id', 'year_start', 'year_end',
                    'age_group_unit', 'age_start', 'age_end', 'sex_id', 'nid',
                    'representative_id', 'code_system_id', 'diagnosis_id',
                    'metric_id']

        for col in int_list:
            try:
                df[col] = pd.to_numeric(df[col], errors='raise')
            except:
                print("Column '{}' could not be cast to a numeric type".
                      format(col))

    # NOTE this may change as formatting inputs change
    # do some stuff to make age_end uniform
    bad_ends = df[(df.age_end == 29)].source.unique()
    if len(bad_ends) > 0:
        print("These sources have an age_end of 29 {}".format(bad_ends))
        for asource in bad_ends:
            # make age end exlusive
            df.loc[(df.age_end > 1) & (df.source == asource), 'age_end'] =\
                df.loc[(df.age_end > 1) & (
                    df.source == asource), 'age_end'] + 1

    df.loc[df.age_start > 95, 'age_start'] = 95
    df.loc[df.age_start >= 95, 'age_end'] = 125
    df.loc[df.age_end == 100, 'age_end'] = 125

    # switch from age start/end to age group id
    df = gbd_hosp_prep.all_group_id_start_end_switcher(
        df, clinical_age_group_set_id)

    return df


def extract_deaths(df):
    """
    Note: This does not modify the DataFrame df that is passed in!

    Args:
        df: (Pandas DataFrame) contains formatted data, old and new

    Returns:
        Pandas DataFrame with just deaths
    """
    ###########################################################
    # Deaths
    ###########################################################
    deaths_df = df.copy()  # copy existing data

    # keep only rows with non-null deaths
    # deaths is a column that some sources have
    deaths_df = deaths_df[deaths_df['deaths'].notnull()]
    deaths_df['outcome_id'] = 'death2'  # replace outcome id with death
    deaths_df['val'] = deaths_df['deaths']   # replace val (cases) with deaths
    deaths_df = deaths_df[deaths_df['val'] > 0]  # drop rows that are zero
    deaths_df.drop(['deaths'], axis=1, inplace=True)  # lose the deaths column

    # in other sources we put both deaths and discharges in the same column
    # (val) and then use labels in the outcome_id column

    if 'death' in df.outcome_id.unique().tolist():
        new_deaths = df[df.outcome_id == 'death'].copy()
        assert new_deaths.deaths.isnull().all(), "I want deaths to be empty"
        # lose the deaths column
        new_deaths.drop(['deaths'], axis=1, inplace=True)
        # concat the deaths df and the original data where outcome_id == death
        deaths_df = pd.concat([new_deaths, deaths_df])
        deaths_df['outcome_id'] = 'death2'
        deaths_df['outcome_id'] = deaths_df['outcome_id'].astype(str)

    return deaths_df


def format_new_data(df, clinical_age_group_set_id):
    """
    Formatting specific to the newer data

    Args:
        df: (Pandas DataFrame) contains new data

    Returns:
        Formatted new data
    """


    df = df[df.source != "GEO_COL_00_13"]


    df = df[df.location_id != 4749]


    df = df[df['val'] > 0]

    # Precondition for format_data()
    if 'deaths' not in df.columns:
        df['deaths'] = np.nan

    df = replace_rounded_under1_ages(df)

    df = format_data(df, clinical_age_group_set_id, downsize=False)

    return df


def replace_rounded_under1_ages(df):
    """The under 1 age groups have non-exact values depending on a few factors
    But the first 3 rounded digits are identifiable and let's uniformly assign these
    float values"""

    good_age_floats = [0.01917808, 0.07671233, 0.50136986]


    for f in good_age_floats:
        if len(str(f)) > 3:
            n = 3
        else:
            n = 1
        df.loc[df.age_start.round(n) == round(f, n), 'age_start'] = f
        df.loc[df.age_end.round(n) == round(f, n), 'age_end'] = f

    return df


def format_old_data(df):
    """
    Formatting specfic to the old gbd 2015 data

    Args:
        df (Pandas DataFrame)  old data to be formatted

    Returns:
        old data with formatting done
    """

    # Precondition for format_data()
    if 'deaths' not in df.columns:
        df['deaths'] = np.nan


    df.loc[df.source.str.startswith("USA_HCUP_SID_"),
           "source"] = "USA_HCUP_SID"


    df.loc[df.age_end >= 4, 'age_end'] = df.loc[df.age_end >= 4, 'age_end'] + 1

    df = replace_rounded_under1_ages(df)

    # Rename columns to match gbd 2016 requirements
    rename_dict = {
        'cases': 'val',
        'sex': 'sex_id',
        'NID': 'nid',
        'dx_ecode_id': "diagnosis_id",
        'dx_mapped_': "cause_code",
        'icd_vers': 'code_system_id',
        'platform': 'facility_id',
        'national': 'representative_id',
        'year': 'year_id'
    }
    df.rename(columns=rename_dict, inplace=True)

    # Drop unneeded columns -- This is done earlier
    # df.drop(['subdiv', 'iso3'], axis=1, inplace=True)

    # change id from 0 to 3 for "not representative"
    df.loc[df['representative_id'] == 0, 'representative_id'] = 3

    # Clean
    df['nid'] = df['nid'].astype(int)  # nid does not need to be a float

    assert_msg = "We assume that there are only 2 ICD formats present"
    assert len(df['code_system_id'].unique()) <= 2, assert_msg

    # replace ICD9 with 1 and ICD10 with 2
    df['code_system_id'].replace(['ICD9_detail', 'ICD10'], [1, 2],
                                 inplace=True)
    # replace 1 with 'inpatient unknown' and 2 with 'outpatient unknown
    df['facility_id'].replace([1, 2], ['inpatient unknown',
                                       'outpatient unknown'],
                              inplace=True)

    # Add columns
    df['outcome_id'] = 'case'  # all rows are cases (discharges and deaths)
    df['metric_id'] = 1  # all rows are counts
    df['age_group_unit'] = 1  # all ages are in years
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    df.drop('year_id', axis=1, inplace=True)


    df = df[df['val'] > 0]

    if 'deaths' not in df.columns:
        df['deaths'] = np.nan

    # Done formatting
    return df


def save_files(df, run_id, outcome_type):
    """
    saves files to prod dir and archive dir for both deaths and all cases
    """

    assert df.shape[0] > 0, "df has no rows of data."

    assert isinstance(outcome_type, str), "outcome_type must be of type str"
    outcome_type = outcome_type.lower()  # normalize input
    assert outcome_type in ["all", "deaths"],\
        "outcome_type must be one of 'all' or 'death'"

    print("Converting mixed and unicode objects to strings...")
    dtypes_df = pd.DataFrame(
        df.apply(lambda x: pd.api.types.infer_dtype(x.values)))
    dtypes_df.columns = ['dtypes']

    mixed_cols = list(dtypes_df[dtypes_df['dtypes'] == 'mixed'].index)
    # NOTE may break with python 3
    unicode_cols = list(dtypes_df[dtypes_df['dtypes'] == 'unicode'].index)

    for col in mixed_cols + unicode_cols:
        df[col] = df[col].astype(str)

    # integer dtypes
    df['code_system_id'] = df.code_system_id.astype(int)

    print("Conversion finished")
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD

    print("saving {}...".format(source))
    print("source in data is {}".format(df.source.unique()))

    directory = FILEPATH.format(
        run_id)

    if outcome_type == "all":
        file_path = "{}/formatted/{}.H5".format(directory, source)


    if outcome_type == "deaths":
        file_path = "{}/deaths/{}.H5".format(directory, source)



    if os.path.exists(file_path):
        print("There's already a file present, checking if years are different...")
        time.sleep(45)  # hopefully avoid any race conditions

        existdf = pd.read_hdf(file_path)
        assert not set(existdf['year_start'].unique()).intersection(df['year_start'].unique()),\
            "File already exists at {}, and the years overlap, delete the existing file if this was expected".format(
                file_path)
        print("Years are different, appending on additional data")
        df = pd.concat([df, existdf], ignore_index=True)

    # write main
    assert df.shape[0] > 0, "df has no rows of data."
    print("Saving to {}...".format(file_path))
    df.to_hdf(file_path, key='df', mode='w', format='table')
    print("Saved.")


def format_inpatient_main(source, source_type, run_id,
                          remove_neonatal,
                          clinical_age_group_set_id,
                          gbd_round_id, decomp_step,
                          remove_live_births,
                          write_hdf=True, downsize=False,
                          verbose=True, head=False):
    """
    Main Function that reads all our formatted data and concatenates them
    together.

    Arguments:
        write_hdf: (bool) If true, writes an HDF H5 file of the aggregated data
        downsize: (bool) If true, numeric types will be cast down to the
            smallest size
        verbose: (bool) If true will print progress and information
        head: (bool) If true will only grab first 1000 rows of each source.

    Returns:
        Pandas DataFrame with formatted data from all sources
    """

    script_start = time.time()

    if source_type == 'new':
        start = time.time()
        df = read_new_data(source, verbose=verbose,
                           head=head, remove_neonatal=remove_neonatal)
        print("Finished reading in new data in {} minutes".
              format((time.time() - start) / 60))

        start = time.time()
        df = format_new_data(df, clinical_age_group_set_id)
        print("Finished formatting data in {} minutes".
              format((time.time() - start) / 60))

    elif source_type == 'old':
        start = time.time()
        df = read_old_data(source, clinical_age_group_set_id, run_id=run_id,
                           remove_neonatal=remove_neonatal,
                           verbose=verbose, head=head)
        print("Finished reading in new data in {} minutes".
              format((time.time() - start) / 60))

    if remove_live_births:
        code_systems = set(df['code_system_id'].unique())
        sys_diff = code_systems - set([1, 2])
        if sys_diff:
            print("This is not an ICD code system and we cannot swap on the blacklist")
            pass
        else:
            print("DROPPING LIVE BIRTH PRIMARY CODES")
            # drop live births after reading in the formatted data
            df = live_births.drop_live_births(df, getuser())
    else:
        pass

    # Precondition for extract_deaths()
    if 'outcome_id' not in df.columns:
        df['outcome_id'] = np.nan

    deaths_df = extract_deaths(df)


    df.drop('deaths', axis=1, inplace=True)

    # Final checks
    # check number of unique feature levels
    assert len(df['diagnosis_id'].unique()) <= 2,\
        "diagnosis_id should have 2 or fewer feature levels"
    assert len(df['code_system_id'].unique()) <= 2,\
        "code_system_id should have 2 or fewer feature levels"

    # assert that a single source/year doesn't have cases combined with
    # anything other than "unknown"
    check_outcome_df = df[['source', 'outcome_id',
                           'year_start']].drop_duplicates()
    for asource in check_outcome_df.source.unique():
        for year in check_outcome_df[check_outcome_df['source'] == asource].year_start.unique():
            outcomes = check_outcome_df[(check_outcome_df['source'] == asource) &
                                        (check_outcome_df['year_start'] == year)].outcome_id.unique()
            if "case" in outcomes:
                assert "death" not in outcomes,\
                    "{} contains death".format(asource)
                assert "discharge" not in outcomes,\
                    "{} contains discharge".format(asource)

    # write population cache
    ppath = FILEPATH
    p = pd.read_pickle(ppath)
    pop_args = p.central_lookup['population']
    pop = cached_pop_tools.get_source_pop(file=None,
                                          df=df,
                                          gbd_round_id=pop_args['gbd_round_id'],
                                          decomp_step=pop_args['decomp_step'],
                                          pop_run_id=pop_args['run_id'])
    cached_pop_tools.write_source_pop(
        pop=pop, run_id=run_id, source_name=source)


    df = hosp_prep.fix_src_names(df)

    # save
    if write_hdf:
        write_start_time = time.time()
        print("Writing...")
        save_files(df, run_id, outcome_type="all")
        print('df finished')
        if not deaths_df.empty:
            save_files(deaths_df, run_id, outcome_type="deaths")
            print('Saved deaths')
        print("Finished writing. writing took {} minutes".
              format((time.time() - write_start_time) / 60.0))

    end = time.time()
    print("All done in {} minutes".format((end - script_start) / 60.0))
    return df


def set_neonatal_removal_status(clinical_age_group_set_id):
    """Use a clinical age group set id to determine whether or not to remove
    the Under 1 detail present in the formatted data"""

    ages = hosp_prep.get_hospital_age_groups(clinical_age_group_set_id)
    ages = ages[ages['age_start'] < 1]
    u1_ages = ages['age_group_id'].unique()

    if u1_ages.size == 1:
        remove_neonatal = True
    elif u1_ages.size > 1:
        remove_neonatal = False
    else:
        raise ValueError("This age set doesn't have under 1 data?")

    return remove_neonatal


if __name__ == '__main__':
    source = sys.argv[1]
    source_type = sys.argv[2]
    clinical_age_group_set_id = int(sys.argv[3])
    gbd_round_id = int(sys.argv[4])
    decomp_step = sys.argv[5]
    remove_live_births = sys.argv[6]
    run_id = sys.argv[7]
    run_id = run_id.replace("\r", "")  # so annoying

    if remove_live_births == 'True':
        remove_live_births = True
    elif remove_live_births == 'False':
        remove_live_births = False
    else:
        assert False, "need a real live births value"
    # Remove is harsh, it actually aggregates it
    remove_neonatal = set_neonatal_removal_status(clinical_age_group_set_id)

    print('Run_id: {r}\nSource: {s}\nSource Type: {st}'.
          format(r=run_id, s=source, st=source_type))

    df = format_inpatient_main(source=source,
                               source_type=source_type,
                               run_id=run_id,
                               remove_neonatal=remove_neonatal,
                               remove_live_births=remove_live_births,
                               clinical_age_group_set_id=clinical_age_group_set_id,
                               gbd_round_id=gbd_round_id,
                               decomp_step=decomp_step,
                               head=False, write_hdf=True)
