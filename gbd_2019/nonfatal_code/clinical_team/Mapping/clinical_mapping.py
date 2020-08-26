import pandas as pd
import numpy as np
import getpass
import warnings
import datetime
import re
import os
import sys
import glob
import pdb

user = getpass.getuser()


repo_dir = ['Functions', 'Mapping', "FILEPATH", "FILEPATH"]
base = r"FILEPATH".format(user)
list(map(sys.path.append, [base + e for e in repo_dir]))


import hosp_prep


import clinical_funcs

def clinfo_process_test(df, table, prod=True):
    """
    Organizing script for the testing suite. The tests we need to run are dependent on which table we're
    pulling data from, so this function runs the appropriate test depending on table.

    Params:
        df: (pd.DataFrame)
            data from a given table in the clinical mapping dB
        table: (str)
            The name of the table the data corresponds to in order to run the correct testing funcs
        prod: (bool)
            if true, any failed tests will break the code.
    """
    assert df.map_version.unique().size == 1, "There are too many map versions present"
    map_vers = int(df.map_version.unique())

    if table == 'cause_code_icg':
        result = test_cc_icg_map(df, prod=prod)

    if table in ['icg_durations', 'age_sex_restrictions']:
        result = test_icg_vals(df, map_version=map_vers, prod=prod)

    if table == 'icg_bundle':
        result = test_icg_bundle(df, prod=prod)

    if table == 'code_system':
        result = test_code_sys(df, prod=prod)

    if 'result' not in locals():
        result = "There is no test for this table"

    return result


def test_and_return_map_version(map_version, prod=True):
    """
    We're going to use this function in our pipeline to test map version consistency, and if
    it's consistent to record the map version
    """
    tables = ['age_sex_restrictions', 'cause_code_icg', 'code_system', 'icg_bundle', 'icg_durations']
    version = []
    for t in tables:
        tmp = get_clinical_process_data(t, map_version=map_version, prod=prod)
        version = list(set(version + tmp['map_version'].unique().tolist()))
    assert len(version) == 1, "There are multiple map versions present: {}".format(version)

    return version[0]


def get_current_map_version(table):
    """
    Converts map_version 'current' to an integer. Gets current map version by finding highest value in map_version column of the specified table.

    Args:
        table (str): table to get map_version from

    Returns:
        map_version as integer
    """

    q = "SQL".format(table)

    query_result = pd.read_sql("DB QUERY")
    assert query_result.shape[0] == 1, "Somehow the query returned more than one row."

    map_version = query_result.values.tolist()[0][0]

    map_version = int(map_version)

    print("The 'current' map version is version {}...".format(map_version))

    return map_version


def check_map_version(map_version):
    """
    Checks that map_version is an int or the string 'current'

    Args:
        table (str): table to get map_version from

    Returns:
        None

    Raises:
        ValueError: if map_version is not an int or the string 'current'
        ValueError: if map_version is less than or equal zero (and if
                    map_version was a number)

    """
    if not ((isinstance(map_version, int)) | (map_version == "current")):
        raise ValueError("map_version must be either an integer or 'current'.")
    if isinstance(map_version, int):
        if map_version <= 0:
            raise ValueError("map_version should be larger than 0.")



def get_clinical_process_data(table, map_version='current', prod=True):
    """
    Connect to the clinical data sandbox dB, pull and return the table specified
    Current options are-
        age_sex_restrictions: age and sex restrictions by nonfatal cause
        cause_code_icg: map from cause code to nonfatal cause
        code_system: map from code system id to code system name
        icg_bundle: map from nonfatal cause to bundle id
        icg_durations: duration limit in days by nonfatal cause

    Thinking about adding
        process_reqs.csv: the file with various requirements to be used in our process,
                          eg. does this bundle require 2 outpatient claims to count as a prevalence case

        Some way to organize all of our estimates. I don't think we formally store these anywhere at the
        moment. they're constantly shifting too.

    Params
        table: (str)
            the name of the table you'd like to pull, in full, from the database
        map_version: (str or int)
            identifies which version of the mapping set to use. Default is set
            to 'current', which pulls in the highest map version from each table
        prod: (bool)
            is this function running in a production-like environment? If true the
            script will break if any tests fail
    """


    if table == 'clinical_sources':
        q = """SQL""".format(table)

    else:
        check_map_version(map_version)
        if map_version == "current":
            map_version = get_current_map_version(table)


        q = """SQL""".format(table, map_version)

    df = pd.read_sql("DB QUERY")

    if df.shape[0] == 0:
        assert False, "Unable to retrieve data for given table and map_version. Please check both values\n"\
                      "The {} table was requested with map version {}".format(table, map_version)

    no_test_tables = ['clinical_sources']
    if table not in no_test_tables:

        print(clinfo_process_test(df, table, prod=prod))

    return df


def create_bundle_restrictions(map_version='current'):
    """
    The age and sex restrictions are created by intermediate cause group, as this is
    the disease grouping we process most clinical data at, however there may be points when
    we need to apply restrictions at the bundle level. In order to do this we take the broadest
    restriction possible given a set of ICGs with various restrictions.

    We will take the max of the male/female columns, the min of age start and the max of age end
    to accomplish this

    Params:
        map_version: (str or int) identifies which version of the mapping set to use. Default is set
                              to 'current', which pulls in the highest map version from each table
    """

    check_map_version(map_version)
    if map_version == "current":
        table = 'age_sex_restrictions'
        map_version = get_current_map_version(table)


    q = """SQL""".format(v=map_version)

    df = pd.read_sql("DB QUERY")

    q = """SQL""".format(map_version)

    chk = pd.read_sql("DB QUERY")


    bundle_diff = set(df.bundle_id.unique()).symmetric_difference(set(chk.bundle_id))
    assert not bundle_diff, "There are missing age sex restrictions. Please review these "\
                            "bundles: {}".format(bundle_diff)

    return df


def create_bundle_durations(map_version='current'):
    """

    Params:
        map_version: (str or int) identifies which version of the mapping set to use. Default is set
                              to 'current', which pulls in the highest map version from each table
    """

    check_map_version(map_version)
    if map_version == "current":
        table = 'icg_durations'
        map_version = get_current_map_version(table)

    q = """SQL""".format(v=map_version)

    df = pd.read_sql("DB QUERY")

    q = """SQL""".format(map_version)

    chk = pd.read_sql("DB QUERY")


    bundle_diff = set(df.bundle_id.unique()).symmetric_difference(set(chk.bundle_id))
    assert not bundle_diff, "There are missing durations. Please review these "\
                            "bundles: {}".format(bundle_diff)


    return df

def get_bundle_measure(prod=True, force_to_prev_bundles=[543, 545, 544], map_version='current'):
    """
    Bundle measures are the collection of all measures of ICGs which make up the bundle.
    This works most of the time, but the `force_to_prev_bundles` break the rule/test


    Params:
        prod: (bool)
            if true, any failed tests will break the code.
        force_to_prev_bundles: (list)
            a list of bundles to apply the prevalence measure to, regardless of their underlying
            ICG measures

    Returns:
        a pd.DataFrame with bundle measure attached
    """

    check_map_version(map_version)

    cm = get_clinical_process_data('cause_code_icg', prod=prod, map_version=map_version)
    bun = get_clinical_process_data('icg_bundle', prod=prod, map_version=map_version)

    df = cm[['icg_id', 'icg_measure']].drop_duplicates()\
            .merge(bun[['icg_id', 'bundle_id']], how='outer', on=['icg_id'])

    df = df[df['bundle_id'].notnull()]

    assert not set(df.bundle_id.unique()).symmetric_difference(set(bun.bundle_id.unique())),\
        "Some bundles appear to be missing measures"


    df.drop_duplicates(subset=['bundle_id', 'icg_measure'], inplace=True)


    dupes = df.loc[df['bundle_id'].duplicated(keep=False)].sort_values('bundle_id')

    if force_to_prev_bundles:
        warnings.warn("Dropping bundles {} from the df of mixed measure bundles and setting to prev".format(force_to_prev_bundles))
        dupes = dupes[~dupes['bundle_id'].isin(force_to_prev_bundles)]

        df.loc[df['bundle_id'].isin(force_to_prev_bundles), 'icg_measure'] = 'prev'
        df.drop_duplicates(['bundle_id', 'icg_measure'], inplace=True)
        assert df.shape[0] == df['bundle_id'].unique().size,\
            "This will duplicate rows on a merge because of these duplicate rows {}".format(dupes)


    msg = "There are {} duplicated bundles in the measure df, "\
          "meaning they have multiple measures for a single bundle. These are bundles {} and ICGs {}".\
          format(dupes.shape[0], dupes['bundle_id'].unique(), dupes['icg_id'])
    if prod:
        if dupes.shape[0] > 0:
            assert False, msg
    else:
        if dupes.shape[0] > 0:
            warnings.warn(msg)

    df.drop('icg_id', axis=1, inplace=True)

    df.reset_index(inplace=True, drop=True)

    df.rename(columns={'icg_measure': 'bundle_measure'}, inplace=True)

    return df

def confirm_code_system_id(code_system_ids):
    """
    verify that the code system in the data exists in the database
    """
    if type(code_system_ids) == int:
        code_system_ids = [code_system_ids]

    df = get_clinical_process_data('code_system')

    for code_system_id in code_system_ids:
        if code_system_id in df['code_system_id'].unique().tolist():
            tmp = df[df['code_system_id'] == code_system_id]
            assert tmp.shape[0] == 1, "There are too many code systems present"

            print(("ID {} for {} and map version {} is present in the database".\
                  format(code_system_id, tmp['code_system_name'].iloc[0], tmp['map_version'].iloc[0])))
            result = "Code System ID confirmed"
        else:
            result = "Code System ID NOT confirmed"
            assert False, "The code system ID {} is not present in the code system database".format(code_system_id)

    return result


def clean_cause_code(df):
    assert 'cause_code' in df.columns, "The cause code variable is not present"


    df['cause_code'] = df['cause_code'].astype(str)

    df['cause_code'] = df['cause_code'].str.replace("\W", "")

    df['cause_code'] = df['cause_code'].str.upper()






    if 'nid' in df.columns:
        chk = df[df['nid'].isin([206640, 299375])]

        assert 1 and 2 not in chk.code_system_id.unique(),\
            "The ICD 9 and/or 10 code systems appear to be assigned to Vietnam or Indonesia data. This is not correct"

    return df


def merge_and_check(df, maps, good_map=True):
    """
    This is the standard method for merging intermediate cause groups onto ICD codes,
    checks to make sure that
    icd codes haven't changed either
    """

    before_values = list(df['cause_code'].unique())

    pre = df.shape[0]
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])
    if good_map:
        dupes = maps[maps.duplicated(subset=['cause_code', 'code_system_id'], keep=False)].sort_values('cause_code')
        assert pre == df.shape[0],\
            "Number of rows have changed from {} to {}. "\
            "Probably due to these duplicated codes".format(\
                pre, df.shape[0], dupes)



    after_values = list(df['cause_code'].unique())

    assert set(before_values) == set(after_values), "Unique ICD codes are not the same"
    return df


def extract_primary_dx(df):
    """
    creates a new column with just the primary dx when multiple dx have been
    combined together (this happens often in the phil health data
    """

    df['split_code'] = df.cause_code.map(lambda x: re.split("([A-Z])", x))

    df['split_code'] = df['split_code'].str[0:3]
    df['split_code'] = df['split_code'].map(lambda x: "".join(map(str, x)))

    return df


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

        good = good[good['icg_name'].notnull()]

        df = df[~df.cause_code.isin(good.cause_code)]
        df_list.append(good)

    dat = pd.concat(df_list, sort=False)
    return dat


def truncate_and_map(df, maps, map_version, extract_pri_dx, write_unmapped=True):
    """
    Performs the actual extraction and truncation of ICD codes when they fail to map
    Writes a list of completely unmapped ICD codes to /test_and_sample_data along
    with the version of the map used
    """

    if df['cause_code'].apply(len).max() <= 3:
        return df

    if df.shape[0] == 0:
        return df

    if df['code_system_id'].max() > 2:
        assert False, "If this assert trips, that means that there is a problem with the special maps. Special maps should not have unmapped cause codes"

    pre_cols = df.columns
    pre = df.shape[0]
    if 'val' in pre_cols:
        pre_cases = df.val.sum()
    else:
        pre_cases = pre
        df['val'] = 1



    df['raw_cause_code'] = df['cause_code']

    for code_sys in df.code_system_id.unique():

        remap = df[df.code_system_id == code_sys].copy()


        remap.drop(['icg_name', 'icg_id', 'icg_measure', 'map_version'], axis=1, inplace=True)

        if extract_pri_dx:


            remap = extract_primary_dx(remap)


        remap = map_to_truncated(remap, maps)

        df = df[~df.raw_cause_code.isin(remap.raw_cause_code)]


        df = pd.concat([df, remap], sort=False)

    if 'split_code' in df.columns:

        df.drop(['raw_cause_code', 'split_code'], axis=1, inplace=True)


    assert pre == df.shape[0],\
        "Rows have changed from {} to {}".format(pre, df.shape[0])
    try:
        case_test = pre_cases.round(3) == df.val.sum().round(3)
    except:
        case_test = round(pre_cases, 3) == round(df.val.sum(), 3)
    assert case_test,\
        "Cases have changed from {} to {}".format(pre_cases, df.val.sum())


    if abs(pre_cases - df.val.sum()) > 5:
        warnings.warn("""

                      More than 5 cases were lost.
                      To be exact, the difference (before - after) is {}
                      """.format(pre_cases - df.val.sum()))
    if 'val' not in pre_cols:
        df.drop('val', axis=1, inplace=True)

    if write_unmapped:
        source = df.source.iloc[0]

        df[df['icg_name'].isnull()].\
            to_csv(r"FILENAME"
                   r"FILEPATH".format(map_version, source),
                   index=False)
    return df


def grouper(df, cause_type):
    """
    set everything that's not an explicit "death2" (gbd2015 deaths) to case
    then groupby and sum val. We have four outcome types leading up to this
    function depending on when the data was formatted.
    Types are discharge(2016) death(2016) case(2015) and death2(2015)
    """
    if 'val' not in df.columns:
        print("This functions sums only the val column. returning data w/o collapsing")
        return df

    print("performing groupby and sum")



    df.loc[df['outcome_id'] != 'death2', 'outcome_id'] = 'case'

    groups = ['location_id', 'year_start', 'year_end', 'age_group_unit',
              'age_group_id', 'sex_id', 'source', 'nid',
              'facility_id', 'representative_id', 'diagnosis_id',
              'metric_id', 'outcome_id'] + cause_type


    if cause_type == ['icg_name', 'icg_id']:
        groups = groups + ['icg_measure']

    df = df.groupby(groups).agg({'val': 'sum'}).reset_index()

    return df


def dtypecaster(df):
    """
    Cast the hospital data to either numeric or str
    """
    dcols = df.columns.tolist()

    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_group_id', 'sex_id', 'nid', 'representative_id',
                'diagnosis_id', 'metric_id', 'icg_id', 'bundle_id', 'val']
    int_cols = [icol for icol in int_cols if icol in dcols]

    str_cols = ['source', 'facility_id', 'outcome_id', 'icg_name', 'icg_measure']
    str_cols = [scol for scol in str_cols if scol in dcols]


    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')

    for col in str_cols:
        df[col] = df[col].astype(str)

    return df


def expand_bundles(df, prod=True, drop_null_bundles=True, map_version='current', test_merge=False):
    """
    This Function maps baby sequelae to Bundles.
    When our data is at the baby seq level there are no duplicates, one icd code
    goes to one baby sequela. At the bundle level there are multiple bundles
    going to the same ICD code so we need to duplicate rows to process every
    bundle. This function maps bundle ID onto baby sequela and duplicates rows.
    Then it can drop rows without bundle ID.  It does this by default.

    Parameters:
        df: Pandas DataFrame
            Must have icg_name, icg_id columns
        drop_null_bundles: Boolean
            If true, will drop the rows of data with baby sequelae that do not
            map to a bundle.
    """

    check_map_version(map_version)

    assert "bundle_id" not in df.columns, "bundle_id has already been attached"
    assert "icg_name" and "icg_id" in df.columns,\
        "'icg_name' must be a column"


    maps = get_clinical_process_data('icg_bundle', prod=prod)
    maps.drop('map_version', axis=1, inplace=True)

    if test_merge:
        test1 = df.merge(maps.drop('icg_name', axis=1), how='left', on=['icg_id'])
        test2 = df.merge(maps.drop('icg_id', axis=1), how='left', on=['icg_name'])



    df = df.merge(maps, how='left', on=['icg_name', 'icg_id'])

    if test_merge:
        assert df.equals(test1),\
            "The merge is performing differently between icg_id alone and icg_id/name combined"
        assert df.equals(test2),\
            "The merge is performing differently between icg_name alone and icg_id/name combined"
        del test1, test2
        print("The merge is identical between icg_name, icg_id and both")

    if drop_null_bundles:

        df = df[df.bundle_id.notnull()]

    return df


def log_unmapped_data(df, write_log):
    """
    write_log: (bool) if True will write a txt file at
            FILEPATH with information about the number of
            rows that didn't get mapped.
    """
    if 'source' not in df.columns:
        print("Source is not present in the data so we cannot store the unmapped stats")
        return
    else:

        no_match_count = float(df['icg_id'].isnull().sum())
        no_match_per = round(no_match_count/df.shape[0] * 100, 4)
        print((r"{} rows did not match a baby sequela in the map out of {}.").\
            format(no_match_count, df.shape[0]))
        print("This is {}% of total rows that did not match".format(no_match_per))


        if no_match_per > 15:
            warnings.warn(r"{}% or more of rows didn't match".format(no_match_per))

        if write_log:
            print("Writing unmapped meta log file")

            text = open("FILENAME" +
                        "{}_".format(df.source.unique()) +
                        re.sub("\W", "_", str(datetime.datetime.now())) +
                        "FILEPATH", "w")
            text.write("""
                       Data Source: {}
                       Number of unmatched rows: {}
                       Total rows of data: {}
                       Percent of unmatched:  {}
                       """.format(df.source.unique(), no_match_count, df.shape[0], no_match_per))
            text.close()

        return


def map_to_gbd_cause(df, input_type, output_type,
                     write_unmapped, truncate_cause_codes=True, extract_pri_dx=True,
                     prod=True, map_version='current', write_log=False,
                     groupby_output=False):
    """
    Params:
        df: (pd.DataFrame)
            a dataframe of clinical data, usually inpatient, outpatient or claims
        input_type: (str)
            level of aggregation present in data
        output_type: (str)
            level of aggregation desired in output data
        write_unmapped: (bool)
            if true, write a file with unmapped data
        truncate_cause_codes: (bool)
            some sources use more detailed cause codes,
            but the ICD system is a hierarchy so you can still
            move up the hierarchy to find something to map to
        extract_pri_dx: (bool)
            some sources have multiple dx in the same column (see PHL),
            pull just the primary codes for these data
        prod: (bool)
            if true, any failed tests will break the code.
        map_version:
        write_log:
        groupby_output: (bool)
            if true, the data is grouped by the output type and collapsed, summing the
            val column together and reducing the size of the df.
    """

    check_map_version(map_version)


    assert input_type in ['cause_code', 'icg'],\
        "{} is not an acceptable input type".format(input_type)
    assert output_type in ['icg', 'bundle'],\
        "{} is not an acceptable output type".format(output_type)

    if 'code_system_id' in df.columns:
        confirm_code_system_id(df['code_system_id'].unique().tolist())


    if 'val' in df.columns:
        start_cases = df.val.sum()
    else:
        start_cases = df.shape[0]

    pre_rows = df.shape[0]


    if input_type == 'cause_code' and output_type == 'icg' or input_type == 'cause_code' and output_type == 'bundle':

        maps = clean_cause_code(\
                                get_clinical_process_data('cause_code_icg',
                                                          prod=prod,
                                                          map_version=map_version))

        map_vers_int = int(maps['map_version'].unique())

        df = clean_cause_code(df)

        df = merge_and_check(df, maps, good_map=True)

        if truncate_cause_codes:

            no_map = df[df['icg_name'].isnull()].copy()

            df = df[df['icg_name'].notnull()].copy()
            no_map = truncate_and_map(df=no_map, maps=maps, map_version=map_vers_int,
                                      extract_pri_dx=extract_pri_dx, write_unmapped=write_unmapped)
            no_map.isnull().sum()


            df = pd.concat([df, no_map], ignore_index=True, sort=False)




        log_unmapped_data(df, write_log=write_log)


        df.loc[df['icg_id'].isnull(),
               ['icg_name', 'icg_id', 'icg_measure', 'map_version']] =\
        ['_none', 1, 'prev', map_vers_int]

        assert pre_rows == df.shape[0], "Row counts have changed after cleaning and merging. "\
                                        "This is unexpected."


    if output_type == 'bundle':

        df = expand_bundles(df, prod=prod, drop_null_bundles=True, map_version=map_version)
        pre_meas = df.shape[0]
        bun_meas = get_bundle_measure(prod=prod, map_version=map_version)
        df = df.merge(bun_meas, how='left', on=['bundle_id'])
        assert pre_meas == df.shape[0], "row counts have changed"


        drops = [d for d in df.columns if 'icg_' in d]
        df.drop(drops, axis=1, inplace=True)



    if output_type == 'icg':
        cause_type = ['icg_name', 'icg_id']

    if output_type == 'bundle':
        cause_type = ['bundle_id']

    if groupby_output:
        df = grouper(df, cause_type)

    if 'val' in df.columns:

        df = dtypecaster(df)

    if output_type == 'icg':
        if 'val' in df.columns:

            if set(df.nid.unique()) == set([205018, 264064]):
                assert abs(start_cases - df.val.sum()) <= 550,\
                    "More than 550 cases have been lost in USA NAMCS data"
            else:
                assert round(start_cases, 3) == round(df.val.sum(), 3),\
                    "Some cases have been lost. Review the map_to_gbd_cause function"

    return df


def apply_restrictions(df, age_set, cause_type, map_version='current', prod=True):
    """
    Apply age and sex restrictions by ICG or bundle to a dataframe of clinical data

    Params:
        df: (pd.DataFrame) clinical data
        age_set: (str) is the data in indv year ages, binned age groups with start/end or
                       age_group_ids
                        acceptable params are "indv", "binned", "age_group_id"
        cause_type: (str) do we want icg restricts or bundle restricts

    Returns:
        df: (pd.DataFrame) with rows that fall outside of age-sex restrictions dropped
    """
    warnings.warn("apply_restrictions needs a testing suite!!")
    sex_diff = set(df.sex_id.unique()).symmetric_difference([1, 2])
    if sex_diff:
        warnings.warn(f"There are sex_id values that won't have restrictions applied to them. These are {sex_diff}")

    assert age_set in ['indv', 'binned', 'age_group_id'], "{} is not an acceptable age set".format(age_set)

    check_map_version(map_version)




    start_cols = df.columns


    if age_set == "age_group_id":
        import gbd_hosp_prep

        df = gbd_hosp_prep.all_group_id_start_end_switcher(df)
    elif age_set == 'indv':
        df = hosp_prep.age_binning(df, drop_age=False, terminal_age_in_data=False)


    df['to_keep'] = 1


    if cause_type == 'icg':
        restrict = get_clinical_process_data('age_sex_restrictions',
                                             map_version, prod=prod)
    elif cause_type == 'bundle':
        restrict = create_bundle_restrictions(map_version)
    else:
        assert False, "pick an acceptable restriction type"





    assert set(restrict.loc[restrict['yld_age_start'] < 1, 'yld_age_start'].unique()) == {0}

    keep_cols = [cause_type + '_id', 'male', 'female', 'yld_age_start', 'yld_age_end']


    pre = df.shape[0]
    df = df.merge(restrict[keep_cols], how='left', on=cause_type + '_id')
    assert pre == df.shape[0], ("merge made more rows, there's something wrong"
                                " in the restrictions file")


    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'to_keep'] = np.nan


    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'to_keep'] = np.nan


    df.loc[df['age_end'] <= df['yld_age_start'], 'to_keep'] = np.nan


    df.loc[df['age_start'] > df['yld_age_end'], 'to_keep'] = np.nan


    df = df[df['to_keep'].notnull()]

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end', 'to_keep'], axis=1,
            inplace=True)

    if age_set == "age_group_id":

        df = gbd_hosp_prep.all_group_id_start_end_switcher(df)
    elif age_set == "indv":
        df.drop(['age_start', 'age_end'], axis=1, inplace=True)

    diff_cols = set(start_cols).symmetric_difference(set(df.columns))
    assert not diff_cols, "The diff columns are {}".format(diff_cols)

    return df


def apply_durations(df, cause_type, map_version, prod=True, fill_missing=False):
    """
    Takes a df with an admission date and appends on the duration limit. This is for
    use with the data that has patient identifiers, like claims, HCUP SIDS, etc.

    Params:
        df: (pd.DataFrame)
        cause_type: (str) must be 'icg' or 'bundle'
    """

    check_map_version(map_version)

    assert cause_type in ['icg', 'bundle'], "{} is not recognized".format(cause_type)
    assert '{}_id'.format(cause_type) in df.columns,\
        "{}_id isn't present in the data and the merge will fail".format(cause_type)

    if cause_type == 'icg':
        dur = get_clinical_process_data('icg_durations', prod=prod, map_version=map_version)

        if fill_missing:
            warnings.warn("There are missing ~~ICG~~ level duration values, these must be populated before a production run. "\
                         "They will be filled with 90 days for the purposes of development")
            dur.loc[dur['icg_duration'] < 0, 'icg_duration'] = 90

        assert dur[(dur['icg_measure'] == 'inc') & (dur['icg_duration'].isnull())].shape[0] == 0, 'Durations contain missing values'
        assert (dur['icg_duration'] > 0).all(), "There are durations values less than 0"

    elif cause_type == 'bundle':
        dur = create_bundle_durations(map_version=map_version)



    df = df.merge(dur[['{}_id'.format(cause_type), '{}_duration'.format(cause_type)]], how='left',
                  on='{}_id'.format(cause_type))


    temp_df = df['{}_duration'.format(cause_type)].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))


    df['adm_date'] = pd.to_datetime(df['adm_date'])

    df['adm_limit'] = df['adm_date'].add(temp_df)

    return df

def test_cc_icg_map(df, prod=True):
    """
    Runs a set of tests on the map in the cause_code_icg map. This is the workhorse map
    from a lot of different cause codes (cc) to our team's intermediate cause groups (icg)

    Params:
        df: (pd DataFrame)
            the map of cause codes (icd, etc) to intermediate cause groups
        prod: (bool)
            is this function running in a production-like environment? If true the
            script will break if any tests fail

    Returns:
        failed_tests: (list) a list with a description of each failed test
    """
    failed_tests = []


    dupes = df[df.duplicated(subset=['cause_code', 'code_system_id', 'icg_name'], keep=False)]
    if not dupes.shape[0] == 0:
        msg = "There are duplicated values of cause code and intermediate cause group breaking the many to 1 relationship"\
        "Please review this df  \n {}".format(dupes)
        warnings.warn(msg)
        failed_tests.append(msg)

    dupes = df[df.duplicated(subset=['cause_code', 'code_system_id', 'icg_id'], keep=False)]
    if not dupes.shape[0] == 0:
        msg = "There are duplicated values of cause code and intermediate cause group, breaking the many to 1 relationship"\
        "Please review this df  \n {}".format(dupes)
        warnings.warn(msg)
        failed_tests.append(msg)


    unique_df = df.groupby('icg_name').nunique()
    if not (unique_df['icg_measure'] == 1).all():
        msg = "Review the intermediate cause group cause name to measure mapping"
        warnings.warn(msg)
        failed_tests.append(msg)


    icg_measures = set(df.icg_measure.unique()).symmetric_difference(set(['inc', 'prev']))
    if icg_measures:
        msg = "There are unexpected ICG measures, please review {}".format(icg_measures)
        warnings.warn(msg)
        failed_tests.append(msg)


    if not (unique_df['map_version'] == 1).all():
        msg = "Multiple map versions are present. These are {}".format(df.map_version.unique())
        warnings.warn(msg)
        failed_tests.append(msg)


    if df.isnull().sum().sum():
        msg = "Null values are present in the cause code to intermediate cause group table {}".format(df.isnull().sum())
        warnings.warn(msg)
        failed_tests.append(msg)


    if not (unique_df['icg_id'] == 1).all():
        msg = "Review the icg name to icg id. The values for each column should be '1'. "\
              "Please review\n{}".format(unique_df.query("icg_id != 1"))
        warnings.warn(msg)
        failed_tests.append(msg)

    id_groupby = df.groupby('icg_id').nunique()
    if not (id_groupby['icg_name'] == 1).all():
        msg = "Review the icg name to icg id. The values for each column should be '1'. "\
              "Please review\n{}".format(id_groupby.query("icg_name != 1"))
        warnings.warn(msg)
        failed_tests.append(msg)


    if df.shape[0] < 63000:
        msg = "There are only {} rows, we expect atleast 62,550 in a complete map".format(df.shape[0])
        warnings.warn(msg)
        failed_tests.append(msg)

    if prod:
        if failed_tests:
            assert False, "{} tests have failed. Review the warning messages or {}".format(len(failed_tests), failed_tests)

    if failed_tests:
        result = failed_tests
    else:
        result = "tests passed"

    return result


def test_icg_vals(dat, map_version, prod=True):
    """
    Check the column values in the duration and restriction files against the
    main cause code to intermediate cause group map. The second half of the tests will
    return a list of {column_name and  test_name} if any of the tests fail.

    Params:
        dat: (pd.DataFrame)
            df with either restrictions or durations to compare to df
        map_version: (str or int)
            identifies which version of the mapping set to use. Default is set
            to 'current', which pulls in the highest map version from each table
        prod: (bool)
            if true, any failed tests will break the code.

    Returns:
        result: (str) A list of the tests which have failed or the phrase tests passed
    """

    check_map_version(map_version)


    df = get_clinical_process_data('cause_code_icg', map_version=map_version, prod=prod)


    diffs = set(df.icg_name.unique()) - set(dat.icg_name.unique())
    assert not diffs, "There are ICG names present which are not in the clean map. "\
                      "Please review this discrepancy in intermediate cause groups {}".format(diffs)


    diffs = set(df.icg_id.unique()) - set(dat.icg_id.unique())
    assert not diffs, "There are ICG IDs present which are not in the clean map. "\
                      "Please review this discrepancy in intermediate cause groups {}".format(diffs)


    chk = df.merge(dat, how='outer', on=['icg_name'])
    chk = chk[chk.code_system_id.notnull()]
    chk_series = chk['icg_id_x'] == chk['icg_id_y']
    assert chk_series.all(), " Some ids don't match for the same intermediate cause names "\
        "please review this df \n {}".format(chk[chk['icg_id_x'] != chk['icg_id_y']])


    val_cols = ['icg_duration', 'male', 'female', 'yld_age_start', 'yld_age_end']
    val_cols = [v for v in val_cols if v in dat.columns]


    failed_tests = []
    for col in val_cols:
        if col  != 'icg_measure':
            if dat[col].min() < 0:
                warnings.warn("{} has values below zero. This is probably a placeholder for "\
                            "missing values. The data is not ready for production use".format(col))
                failed_tests.append(col + "_placeholderNA")


        if col in ['male', 'female']:
            val_diff = set(dat[col].unique()).symmetric_difference(set([0, 1]))
            if val_diff:
                warnings.warn("{} has unexpected values, please review this set difference: {}".format(col, val_diff))
                failed_tests.append(col + "_unexp_vals")


        if col in ['yld_age_end']:
            if dat[col].max() > 95:
                max_df = dat[dat[col] > 95]
                warnings.warn("{} has values over 95 years. This may be acceptable but we haven't seen it before. \n{}".format(col, max_df))
                failed_tests.append(col + "_too_high")


        if col == 'icg_duration':
            if dat[col].max() > 365:
                max_df = dat[dat[col] > 365]
                warnings.warn("{} has values over 365 days. This may be acceptable but we haven't seen it before. \n{}".format(col, max_df))
                failed_tests.append(col + "_too_high")


    if 'icg_measure' in dat.columns:
        val_diff = set(dat['icg_measure'].unique()).symmetric_difference(set(['inc', 'prev']))
        if val_diff:
            warnings.warn("icg_measure has unexpected values, please review this set difference: {}".format(val_diff))
            failed_tests.append(col + "_unexp_vals")

    if prod:
        if failed_tests:
            assert False, "{} tests have failed. These are: {{column_name_test_name}} {}. Review the warnings and "\
                          "source code for more information".format(len(failed_tests), failed_tests)

    if failed_tests:
        result = failed_tests
    else:
        result = "tests passed"

    return result

def test_icg_bundle(df, prod=True):
    """
    Check the intermediate cause group to bundle mapping table

    Params:
        df: (pd.DataFrame)
            a map from ICGs to bundle IDs
        prod: (bool)
            if true, any failed tests will break the code.

    """

    failed_tests = []
    for icg in df.icg_name.unique():
        tmp = df[df.icg_name == icg]
        if tmp.shape[0] != tmp.bundle_id.unique().size:
            print("{} is bad".format(icg))
            failed_tests.append(icg)

    for b in df.bundle_id.unique():
        tmp = df[df.bundle_id == b]
        if tmp.shape[0] != tmp.icg_name.unique().size:
            print("{} is bad".format(b))
            failed_tests.append(b)

    if prod:
        if failed_tests:
            assert False, "{} tests have failed. These are: {{column_name_test_name}} {}. Review the warnings and "\
                          "source code for more information".format(len(failed_tests), failed_tests)

    if failed_tests:
        result = failed_tests
    else:
        result = "ICG bundle tests passed"

    return result

def test_code_sys(df, prod=True):
    """
    Check the code system table in the clinical mapping dB to ensure there aren't duplicated systems

    """
    no_dupes = df[['code_system_id', 'code_system_name']].drop_duplicates()

    assert df.shape[0] == no_dupes.shape[0], "There are duplicated code systems"

    return "Code system tests passed"


def test_bundle_measure():

    cc_m = get_clinical_process_data('cause_code_icg', prod=False)
    bun = get_clinical_process_data('icg_bundle', prod=False)

    m = cc_m[['code_system_id', 'icg_name', 'icg_id', 'icg_measure']].\
        drop_duplicates().merge(bun, how='outer', on=['icg_name', 'icg_id'])

    z = m[['icg_name', 'icg_measure', 'bundle_id']].drop_duplicates()
    z[z.duplicated('bundle_id', keep=False)].sort_values("bundle_id")

    return m, z
