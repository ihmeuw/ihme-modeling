import pandas as pd
import numpy as np
import warnings
import datetime
import re
import os
import sys
import glob
import pdb

from clinical_info.Functions import hosp_prep, gbd_hosp_prep
from clinical_info.Functions.Clinical_functions import clinical_funcs
from clinical_info.Upload.tools.io.database import Database


def clinfo_process_test(df, table, prod=True):
    """
    Organizing script for the testing suite.

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

    if table == "cause_code_icg":
        result = test_cc_icg_map(df, prod=prod)

    if table in ["icg_durations", "age_sex_restrictions"]:
        result = test_icg_vals(df, map_version=map_vers, prod=prod)

    if table == "icg_bundle":
        result = test_icg_bundle(df, prod=prod)

    if table == "code_system":
        result = test_code_sys(df, prod=prod)

    if "result" not in locals():
        result = "There is no test for this table"

    return result


def test_and_return_map_version(map_version, prod=True):
    tables = [
        "age_sex_restrictions",
        "cause_code_icg",
        "code_system",
        "icg_bundle",
        "icg_durations",
    ]
    version = []
    for t in tables:
        tmp = get_clinical_process_data(t, map_version=map_version, prod=prod)
        version = list(set(version + tmp["map_version"].unique().tolist()))
    assert len(version) == 1, "There are multiple map versions present: {}".format(
        version
    )

    return version[0]


def get_current_map_version(table):
    """
    Converts map_version 'current' to an integer. Gets current map version by finding highest value in map_version column of the specified table.

    Args:
        table (str): table to get map_version from

    Returns:
        map_version as integer
    """

    q = "QUERY".format(table)

    query_result = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )
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


def get_clinical_process_data(table, map_version="current", prod=True):

    # grab just the sources data, it's not versioned
    if table == "clinical_sources":
        q = "QUERY".format(table)
    # Else, we're looking at a table that has versioning
    else:
        check_map_version(map_version)
        if map_version == "current":
            map_version = get_current_map_version(table)

        # run the query on the dB
        q = "QUERY".format(table, map_version)

    df = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )

    if df.shape[0] == 0:
        assert False, (
            "Unable to retrieve data for given table and map_version. Please check both values\n"
            "The {} table was requested with map version {}".format(table, map_version)
        )

    no_test_tables = ["clinical_sources"]
    if table not in no_test_tables:
        # throw in the testing suite here
        print(clinfo_process_test(df, table, prod=prod))

    return df


def create_bundle_restrictions(map_version="current"):

    check_map_version(map_version)
    if map_version == "current":
        table = "age_sex_restrictions"
        map_version = get_current_map_version(table)

    # run the query on the dB
    q = "QUERY".format(v=map_version)

    df = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )

    q = "QUERY".format(map_version)

    chk = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )

    # there should be age/sex restrictions for ALL bundles
    bundle_diff = set(df.bundle_id.unique()).symmetric_difference(set(chk.bundle_id))
    assert not bundle_diff, (
        "There are missing age sex restrictions. Please review these "
        "bundles: {}".format(bundle_diff)
    )

    return df


def get_db_conn(odbc="clinical"):
    db = Database()
    db.load_odbc(odbc)
    return db


def get_active_bundles(map_version="current", **kwargs):
    """
    Pulls bundle_ids and releated ids from the 
    active_bundle_metadata table in the prod db

    Params:
        cols (list str) : Features to return 
        map_version (str or int) : Identifies which version of the mapping set to use. 
                                   Default is set to 'current', which pulls in the highest
                                   map version from each table
        kwargs (k: str, v:list) : features of active_bundle_metadata. additionaly the k
                                  cols will select that subset of features

    """
    if "cols" in kwargs.keys():
        cols = kwargs["cols"]
    else:
        cols = ["bundle_id", "estimate_id", "clinical_age_group_set_id", "measure_id"]

    db = get_db_conn()
    if map_version == "current":
        map_version = db.query("QUERY")["mv"].item()

    dql = f"QUERY"
    for k, v in kwargs.items():
        if k == "cols":
            continue
        dql += f"AND {k} IN ({str(v)[1:-1]})"

    df = db.query(dql)
    return df[list(cols)]


def create_bundle_durations(map_version="current"):
    """

    Params:
        map_version: (str or int) identifies which version of the mapping set to use. Default is set
                              to 'current', which pulls in the highest map version from each table
    """

    check_map_version(map_version)
    if map_version == "current":
        table = "icg_durations"
        map_version = get_current_map_version(table)

    q = "QUERY".format(v=map_version)

    df = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )

    q = "QUERY".format(map_version)

    chk = pd.read_sql(
        q,
        con=clinical_funcs.get_engine(
            db_name="FILEPATH", epi_db_name="FILEPATH"
        ).connect(),
    )

    # there should be durations for ALL bundles
    bundle_diff = set(df.bundle_id.unique()).symmetric_difference(set(chk.bundle_id))
    assert not bundle_diff, (
        "There are missing durations. Please review these "
        "bundles: {}".format(bundle_diff)
    )

    return df


def get_bundle_measure(
    prod=True,
    force_to_prev_bundles=[543, 544, 545, 3260, 6998, 6707, 7001],
    map_version="current",
):
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

    cm = get_clinical_process_data("cause_code_icg", prod=prod, map_version=map_version)
    bun = get_clinical_process_data("icg_bundle", prod=prod, map_version=map_version)

    df = (
        cm[["icg_id", "icg_measure"]]
        .drop_duplicates()
        .merge(bun[["icg_id", "bundle_id"]], how="outer", on=["icg_id"])
    )
    # remove missing bundle IDs
    df = df[df["bundle_id"].notnull()]

    assert not set(df.bundle_id.unique()).symmetric_difference(
        set(bun.bundle_id.unique())
    ), "Some bundles appear to be missing measures"

    # remove duplicates of bundle and measure
    df.drop_duplicates(subset=["bundle_id", "icg_measure"], inplace=True)

    # get duplicated bundles, ie they have more than 1 measure
    dupes = df.loc[df["bundle_id"].duplicated(keep=False)].sort_values("bundle_id")

    if force_to_prev_bundles:
        warnings.warn(
            "Dropping bundles {} from the df of mixed measure bundles and setting to prev".format(
                force_to_prev_bundles
            )
        )
        dupes = dupes[~dupes["bundle_id"].isin(force_to_prev_bundles)]
        # now set the measure to prev
        df.loc[df["bundle_id"].isin(force_to_prev_bundles), "icg_measure"] = "prev"
        df.drop_duplicates(["bundle_id", "icg_measure"], inplace=True)
        assert (
            df.shape[0] == df["bundle_id"].unique().size
        ), "This will duplicate rows on a merge because of these duplicate rows {}".format(
            dupes
        )

    # break if there are duplicated values
    msg = (
        "There are {} duplicated bundles in the measure df, "
        "meaning they have multiple measures for a single bundle. These are bundles {} and ICGs {}".format(
            dupes.shape[0], dupes["bundle_id"].unique(), dupes["icg_id"]
        )
    )
    if prod:
        if dupes.shape[0] > 0:
            assert False, msg
    else:
        if dupes.shape[0] > 0:
            warnings.warn(msg)

    df.drop("icg_id", axis=1, inplace=True)

    df.reset_index(inplace=True, drop=True)

    df.rename(columns={"icg_measure": "bundle_measure"}, inplace=True)

    return df


def confirm_code_system_id(code_system_ids):
    """
    verify that the code system in the data exists in the database
    """
    if type(code_system_ids) == int:
        code_system_ids = [code_system_ids]

    df = get_clinical_process_data("code_system")

    for code_system_id in code_system_ids:
        if code_system_id in df["code_system_id"].unique().tolist():
            tmp = df[df["code_system_id"] == code_system_id]
            assert tmp.shape[0] == 1, "There are too many code systems present"

            print(
                (
                    "ID {} for {} and map version {} is present in the database".format(
                        code_system_id,
                        tmp["code_system_name"].iloc[0],
                        tmp["map_version"].iloc[0],
                    )
                )
            )
            result = "Code System ID confirmed"
        else:
            result = "Code System ID NOT confirmed"
            assert (
                False
            ), "The code system ID {} is not present in the code system database".format(
                code_system_id
            )

    return result


def clean_cause_code(df):
    assert "cause_code" in df.columns, "The cause code variable is not present"

    # cast to string
    df["cause_code"] = df["cause_code"].astype(str)
    # remove non alphanumeric characters
    df["cause_code"] = df["cause_code"].str.replace("\W", "")
    # convert to upper case
    df["cause_code"] = df["cause_code"].str.upper()

    if "nid" in df.columns:
        chk = df[df["nid"].isin([206640, 299375])]
        assert (
            1 and 2 not in chk.code_system_id.unique()
        ), "The ICD 9 and/or 10 code systems appear to be assigned to Vietnam or Indonesia data. This is not correct"

    return df


def merge_and_check(df, maps, good_map=True):
    """
    This is the standard method for merging intermediate cause groups onto ICD codes,
    checks to make sure that # of rows don't change and
    icd codes haven't changed either
    """
    # store variables for data check later
    before_values = list(df["cause_code"].unique())  # values before
    # merge the hospital data with excel spreadsheet maps
    pre = df.shape[0]
    df = df.merge(maps, how="left", on=["cause_code", "code_system_id"])
    if good_map:
        dupes = maps[
            maps.duplicated(subset=["cause_code", "code_system_id"], keep=False)
        ].sort_values("cause_code")
        assert pre == df.shape[0], (
            "Number of rows have changed from {} to {}. "
            "Probably due to these duplicated codes".format(pre, df.shape[0], dupes)
        )

    after_values = list(df["cause_code"].unique())  # values after
    # assert various things to verify merge
    assert set(before_values) == set(after_values), "Unique ICD codes are not the same"
    return df


def extract_primary_dx(df):
    """
    Keeps only the primary dx when multiple dx have been combined together in
    one columnn (this happens often in the phil health data).
    """
    # split the icd codes on letter
    df["cause_code"] = df.cause_code.map(lambda x: re.split("([A-Z])", x))
    # keep primary icd code
    df["cause_code"] = df["cause_code"].str[0:3]
    df["cause_code"] = df["cause_code"].map(lambda x: "".join(map(str, x)))

    return df


def map_to_truncated(df, maps):
    """
    Takes a dataframe of icd codes which didn't map to baby sequela
    merges ICGs onto every level of truncated icd codes, from 6
    digits to 3 digits when needed. returns only the data that has
    successfully mapped to a baby sequelae.
    """
    df_list = []
    for i in [7, 6, 5, 4, 3, 2]:
        df["cause_code"] = df["cause_code"].str[0:i]
        good = df.merge(maps, how="left", on=["cause_code", "code_system_id"])
        # keep only rows that successfully merged
        good = good[good["icg_name"].notnull()]
        # drop the icd codes that matched from df
        df = df[~df.cause_code.isin(good.cause_code)].copy()
        df_list.append(good)

    dat = pd.concat(df_list, sort=False)
    print(f"returning dat is shape {dat.shape}, input df is {df.shape}")
    return dat


def truncate_and_map(df, maps, map_version, extract_pri_dx, write_unmapped=True):
    """
    Performs the actual extraction and truncation of ICD codes when they fail to map
    Writes a list of completely unmapped ICD codes to /test_and_sample_data along
    with the version of the map used
    """
    # return the df if it's just 3 codes long
    if df["cause_code"].apply(len).max() <= 3:
        return df
    # or if it's empty
    if df.shape[0] == 0:
        return df
    # skip the special maps
    if df["code_system_id"].max() > 2:
        assert (
            False
        ), "If this assert trips, that means that there is a problem with the special maps. Special maps should not have unmapped cause codes"

    pre_cols = df.columns
    pre = df.shape[0]
    if "val" in pre_cols:
        pre_cases = df.val.sum()
    else:
        pre_cases = pre
        df["val"] = 1

    # create a raw cause code column to remove icd codes that were
    # fixed and mapped successfully
    df["raw_cause_code"] = df["cause_code"]

    map_list = []
    for code_sys in df.code_system_id.unique():
        pre_code_sys = df.query(f"code_system_id == {code_sys}").shape
        # get just the codes from 1 code system that didn't map
        remap = df[df.code_system_id == code_sys].copy()

        # create remap_df to retry mapping for icd n
        remap.drop(
            ["icg_name", "icg_id", "icg_measure", "map_version"], axis=1, inplace=True
        )

        if extract_pri_dx:
            remap = extract_primary_dx(remap)

        # now do the actual remapping, losing all rows that don't map
        remap = map_to_truncated(remap, maps)

        # remove the rows where we were able to re-map to baby sequela
        goodmap = df[
            (~df.raw_cause_code.isin(remap.raw_cause_code))
            & (df.code_system_id == code_sys)
        ].copy()

        # sanity check on the data that will get concatted back together, no
        # rows should be lost
        assert pre_code_sys[0] == (
            len(goodmap) + len(remap)
        ), "not concatting back correctly. Rows should match perfectly"
        map_list += [goodmap, remap]

    df = pd.concat(map_list, sort=False, ignore_index=True)
    del map_list

    if "raw_cause_code" in df.columns:
        # drop cols used to truncate and split
        df.drop(["raw_cause_code"], axis=1, inplace=True)

    # we should not be losing ANY rows or cases
    assert pre == df.shape[0], "Rows have changed from {} to {}".format(
        pre, df.shape[0]
    )
    try:
        case_test = pre_cases.round(3) == df.val.sum().round(3)
    except:
        case_test = round(pre_cases, 3) == round(df.val.sum(), 3)
    assert case_test, "Cases have changed from {} to {}".format(pre_cases, df.val.sum())

    # assert that we haven't lost  more than 5 cases
    if abs(pre_cases - df.val.sum()) > 5:
        warnings.warn(
            """

                      More than 5 cases were lost.
                      To be exact, the difference (before - after) is {}
                      """.format(
                pre_cases - df.val.sum()
            )
        )
    if "val" not in pre_cols:
        df.drop("val", axis=1, inplace=True)

    if write_unmapped:
        source = df.source.iloc[0]
        # write the data that didn't match for further inspection
        df[df["icg_name"].isnull()].to_csv(
            (f"FILEPATH"), index=False,
        )
    return df


def grouper(df, cause_type):
    """
    set everything that's not an explicit "death2" (gbd2015 deaths) to case
    then groupby and sum val. We have four outcome types leading up to this
    function depending on when the data was formatted.
    Types are discharge(2016) death(2016) case(2015) and death2(2015)
    """
    if "val" not in df.columns:
        print("This functions sums only the val column. returning data w/o collapsing")
        return df

    print("performing groupby and sum")
    df.loc[df["outcome_id"] != "death2", "outcome_id"] = "case"

    groups = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_group_id",
        "sex_id",
        "source",
        "nid",
        "facility_id",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "outcome_id",
    ] + cause_type

    # retain icg measure
    if cause_type == ["icg_name", "icg_id"]:
        groups = groups + ["icg_measure"]

    df = df.groupby(groups).agg({"val": "sum"}).reset_index()

    return df


def dtypecaster(df):
    """
    Cast the hospital data to either numeric or str
    """
    dcols = df.columns.tolist()
    # set the col types
    int_cols = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_group_id",
        "sex_id",
        "nid",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "icg_id",
        "bundle_id",
        "val",
    ]  # , 'use_in_maternal_denom']
    int_cols = [icol for icol in int_cols if icol in dcols]

    str_cols = ["source", "facility_id", "outcome_id", "icg_name", "icg_measure"]
    str_cols = [scol for scol in str_cols if scol in dcols]

    # do the col casting
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="raise")

    for col in str_cols:
        df[col] = df[col].astype(str)

    return df


def expand_bundles(
    df, prod=True, drop_null_bundles=True, map_version="current", test_merge=False
):

    check_map_version(map_version)

    assert "bundle_id" not in df.columns, "bundle_id has already been attached"
    assert "icg_name" and "icg_id" in df.columns, "'icg_name' must be a column"

    # get the icg to bundle map
    maps = get_clinical_process_data("icg_bundle", prod=prod)
    maps.drop("map_version", axis=1, inplace=True)

    if test_merge:
        test1 = df.merge(maps.drop("icg_name", axis=1), how="left", on=["icg_id"])
        test2 = df.merge(maps.drop("icg_id", axis=1), how="left", on=["icg_name"])

    # this merge duplicates data as we expect. b/c a single baby sequela goes
    # to multiple bundle IDs+
    df = df.merge(maps, how="left", on=["icg_name", "icg_id"])

    if test_merge:
        assert df.equals(
            test1
        ), "The merge is performing differently between icg_id alone and icg_id/name combined"
        assert df.equals(
            test2
        ), "The merge is performing differently between icg_name alone and icg_id/name combined"
        del test1, test2
        print("The merge is identical between icg_name, icg_id and both")

    if drop_null_bundles:
        # drop rows without bundle id
        df = df[df.bundle_id.notnull()]

    return df


def log_unmapped_data(df, write_log):
    if "source" not in df.columns:
        print("Source is not present in the data so we cannot store the unmapped stats")
        return
    else:
        # count how many rows didn't map, ie have a null baby seq:
        no_match_count = float(df["icg_id"].isnull().sum())
        no_match_per = round(no_match_count / df.shape[0] * 100, 4)
        print(
            (r"{} rows did not match a baby sequela in the map out of {}.").format(
                no_match_count, df.shape[0]
            )
        )
        print("This is {}% of total rows that did not match".format(no_match_per))

        # arbitrary warning if over 15% of data unmapped
        if no_match_per > 15:
            warnings.warn(r"{}% or more of rows didn't match".format(no_match_per))

        if write_log:
            print("Writing unmapped meta log file")

            text = open("FILEPATH", "w",)
            text.write(
                """
                       Data Source: {}
                       Number of unmatched rows: {}
                       Total rows of data: {}
                       Percent of unmatched:  {}
                       """.format(
                    df.source.unique(), no_match_count, df.shape[0], no_match_per
                )
            )
            text.close()

        return


def map_to_gbd_cause(
    df,
    input_type,
    output_type,
    write_unmapped,
    truncate_cause_codes=True,
    extract_pri_dx=True,
    prod=True,
    map_version="current",
    write_log=False,
    groupby_output=False,
):
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

    # make sure the input and output values are good. V limited for now
    assert input_type in [
        "cause_code",
        "icg",
    ], "{} is not an acceptable input type".format(input_type)
    assert output_type in [
        "icg",
        "bundle",
    ], "{} is not an acceptable output type".format(output_type)

    if (
        "code_system_id" in df.columns
    ):  # this doesn't apply when mapping to bundle level from icg
        confirm_code_system_id(df["code_system_id"].unique().tolist())

    # val basically signifies whether the data is otp/int and aggregatable or claims and not
    if "val" in df.columns:
        start_cases = df.val.sum()
    else:
        start_cases = df.shape[0]

    pre_rows = df.shape[0]

    # map the data from cause codes to intermediate cause groups
    if (
        input_type == "cause_code"
        and output_type == "icg"
        or input_type == "cause_code"
        and output_type == "bundle"
    ):
        # prep the data and the map
        maps = clean_cause_code(
            get_clinical_process_data(
                "cause_code_icg", prod=prod, map_version=map_version
            )
        )

        map_vers_int = int(maps["map_version"].unique())

        df = clean_cause_code(df)

        df = merge_and_check(df, maps, good_map=True)

        if truncate_cause_codes:
            # we want only rows where nfc is null to try to truncate and map
            no_map = df[df["icg_name"].isnull()].copy()
            # keep only rows in df where nfc is not null
            df = df[df["icg_name"].notnull()].copy()
            no_map = truncate_and_map(
                df=no_map,
                maps=maps,
                map_version=map_vers_int,
                extract_pri_dx=extract_pri_dx,
                write_unmapped=write_unmapped,
            )
            no_map.isnull().sum()

            # bring them back together
            df = pd.concat([df, no_map], ignore_index=True, sort=False)
            # print(pre_cases - df.val.sum())
            # Done

        # possibly add something here to track % of data that is unmapped
        log_unmapped_data(df, write_log=write_log)

        # map missing ICG to _none
        df.loc[
            df["icg_id"].isnull(), ["icg_name", "icg_id", "icg_measure", "map_version"]
        ] = ["_none", 1, "prev", map_vers_int]

        assert pre_rows == df.shape[0], (
            "Row counts have changed after cleaning and merging. " "This is unexpected."
        )

    # map the data from intermediate cause groups to bundles
    if output_type == "bundle":
        df = expand_bundles(
            df, prod=prod, drop_null_bundles=True, map_version=map_version
        )
        pre_meas = df.shape[0]
        bun_meas = get_bundle_measure(prod=prod, map_version=map_version)
        df = df.merge(bun_meas, how="left", on=["bundle_id"])
        assert pre_meas == df.shape[0], "row counts have changed"

        # drop icg columns because data is now at the bundle level
        drops = [d for d in df.columns if "icg_" in d]
        df.drop(drops, axis=1, inplace=True)

    # set appropriate outcome ID columns
    # groupby and collapsee
    if output_type == "icg":
        cause_type = ["icg_name", "icg_id"]

    if output_type == "bundle":
        cause_type = ["bundle_id"]

    if groupby_output:
        df = grouper(df, cause_type)

    if "val" in df.columns:
        # cast dtypes
        df = dtypecaster(df)

    if output_type == "icg":
        if "val" in df.columns:
            # USA NAMCS, is breaking cause there are strange values
            if set(df.nid.unique()) == set([205018, 264064]):
                assert (
                    abs(start_cases - df.val.sum()) <= 550
                ), "More than 550 cases have been lost in USA NAMCS data"
            else:
                assert round(start_cases, 3) == round(
                    df.val.sum(), 3
                ), "Some cases have been lost. Review the map_to_gbd_cause function"

    return df


def apply_restrictions(
    df,
    age_set,
    cause_type,
    clinical_age_group_set_id,
    map_version="current",
    prod=True,
    break_if_not_contig=True,
):
    """
    Apply age and sex restrictions by ICG or bundle to a dataframe of clinical data

    Params:
        df: (pd.DataFrame) clinical data
        age_set: (str) is the data in indv year ages, binned age groups with start/end or
                       age_group_ids
                        acceptable params are "indv", "binned", "age_group_id"
        cause_type: (str) do we want icg restricts or bundle restricts
        break_if_not_contig: (bool)
            Gets passed on to confirm_contiguous_data(). If True, a ValueError
            will be raised if there are any gaps in the age pattern.

    Returns:
        df: (pd.DataFrame) with rows that fall outside of age-sex restrictions dropped
    """
    warnings.warn("apply_restrictions needs a testing suite!!")
    if clinical_age_group_set_id == 1:
        print(
            (
                "Using the GBD 2019 and earlier age set, This WILL NOT work "
                "with GBD2020 ICG restrictions for cancer"
            )
        )

    sex_diff = set(df.sex_id.unique()).symmetric_difference([1, 2])
    if sex_diff:
        warnings.warn(
            f"There are sex_id values that won't have restrictions applied to them. These are {sex_diff}"
        )

    assert age_set in [
        "indv",
        "binned",
        "age_group_id",
    ], "{} is not an acceptable age set".format(age_set)

    check_map_version(map_version)

    start_cols = df.columns

    # prep the age column, we want binned start/end groups
    if age_set == "age_group_id":
        # switch from age group id to age start end
        df = gbd_hosp_prep.all_group_id_start_end_switcher(
            df, clinical_age_group_set_id
        )
    elif age_set == "indv":
        df = hosp_prep.age_binning(
            df,
            drop_age=False,
            terminal_age_in_data=False,
            break_if_not_contig=break_if_not_contig,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

    # set a column of interest column even tho it doesn't exist since this is indv data
    df["to_keep"] = 1

    # get the df of age/sex restrictions
    if cause_type == "icg":
        restrict = get_clinical_process_data(
            "age_sex_restrictions", map_version, prod=prod
        )
    elif cause_type == "bundle":
        restrict = create_bundle_restrictions(map_version)
    else:
        assert False, "pick an acceptable restriction type"

    keep_cols = [cause_type + "_id", "male", "female", "yld_age_start", "yld_age_end"]

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict[keep_cols], how="left", on=cause_type + "_id")
    assert pre == df.shape[0], (
        "merge made more rows, there's something wrong" " in the restrictions file"
    )

    # set to_keep to zero where male in cause = 0
    df.loc[(df["male"] == 0) & (df["sex_id"] == 1), "to_keep"] = np.nan

    # set to_keep to zero where female in cause = 0
    df.loc[(df["female"] == 0) & (df["sex_id"] == 2), "to_keep"] = np.nan

    # set to_keep to zero where age end is smaller than yld age start
    df.loc[df["age_end"] <= df["yld_age_start"], "to_keep"] = np.nan

    # set to_keep to zero where age start is larger than yld age end
    df.loc[df["age_start"] > df["yld_age_end"], "to_keep"] = np.nan

    # drop the restricted values
    df = df[df["to_keep"].notnull()]

    df.drop(
        ["male", "female", "yld_age_start", "yld_age_end", "to_keep"],
        axis=1,
        inplace=True,
    )

    if age_set == "age_group_id":
        # switch from start-end to age group id
        df = gbd_hosp_prep.all_group_id_start_end_switcher(
            df, clinical_age_group_set_id
        )
    elif age_set == "indv":
        df.drop(["age_start", "age_end"], axis=1, inplace=True)

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

    assert cause_type in ["icg", "bundle"], "{} is not recognized".format(cause_type)
    assert (
        "{}_id".format(cause_type) in df.columns
    ), "{}_id isn't present in the data and the merge will fail".format(cause_type)

    if cause_type == "icg":
        dur = get_clinical_process_data(
            "icg_durations", prod=prod, map_version=map_version
        )

        if fill_missing:
            warnings.warn(
                "There are missing ~~ICG~~ level duration values, these must be populated before a production run. "
                "They will be filled with 90 days for the purposes of development"
            )
            dur.loc[dur["icg_duration"] < 0, "icg_duration"] = 90

        assert (
            dur[(dur["icg_measure"] == "inc") & (dur["icg_duration"].isnull())].shape[0]
            == 0
        ), "Durations contain missing values"
        assert (dur["icg_duration"] > 0).all(), "There are durations values less than 0"

    elif cause_type == "bundle":
        dur = create_bundle_durations(map_version=map_version)

    # merge on durations
    df = df.merge(
        dur[["{}_id".format(cause_type), "{}_duration".format(cause_type)]],
        how="left",
        on="{}_id".format(cause_type),
    )

    # convert a col of ints to a days type time object
    temp_df = (
        df["{}_duration".format(cause_type)]
        .apply(np.ceil)
        .apply(lambda x: pd.Timedelta(x, unit="D"))
    )

    # make sure adm date is a date time object
    df["adm_date"] = pd.to_datetime(df["adm_date"])
    # then add durations to adm_date to get the limit date
    df["adm_limit"] = df["adm_date"].add(temp_df)

    return df


def test_cc_icg_map(df, prod=True):
    failed_tests = []

    # many:1 relationship between ICD code and nfc. cause codes can not be duplicated
    dupes = df[
        df.duplicated(subset=["cause_code", "code_system_id", "icg_name"], keep=False)
    ]
    if not dupes.shape[0] == 0:
        msg = (
            "There are duplicated values of cause code and intermediate cause group breaking the many to 1 relationship"
            "Please review this df  \n {}".format(dupes)
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    dupes = df[
        df.duplicated(subset=["cause_code", "code_system_id", "icg_id"], keep=False)
    ]
    if not dupes.shape[0] == 0:
        msg = (
            "There are duplicated values of cause code and intermediate cause group, breaking the many to 1 relationship"
            "Please review this df  \n {}".format(dupes)
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    # no duplicated icd codes within a code system
    cc_groups = ["code_system_id", "cause_code"]
    ccdupes = df[df.duplicated(subset=cc_groups, keep=False)].sort_values(cc_groups)
    if len(ccdupes) > 0:
        msg = f"There are duplicated ICD to ICG mappings {ccdupes}"
        failed_tests.append(msg)

    # each icg is marked with only 1 unique measure
    unique_df = df.groupby("icg_name").nunique()
    if not (unique_df["icg_measure"] == 1).all():
        msg = "Review the intermediate cause group cause name to measure mapping"
        warnings.warn(msg)
        failed_tests.append(msg)

    # icg measures are only inc and prev
    icg_measures = set(df.icg_measure.unique()).symmetric_difference(
        set(["inc", "prev"])
    )
    if icg_measures:
        msg = "There are unexpected ICG measures, please review {}".format(icg_measures)
        warnings.warn(msg)
        failed_tests.append(msg)

    # the map should have only a single version present
    if not (unique_df["map_version"] == 1).all():
        msg = "Multiple map versions are present. These are {}".format(
            df.map_version.unique()
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    # no nulls are present in any record
    if df.isnull().sum().sum():
        msg = "Null values are present in the cause code to intermediate cause group table {}".format(
            df.isnull().sum()
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    # 1:1 mapping between nfc name and nfc ID
    if not (unique_df["icg_id"] == 1).all():
        msg = (
            "Review the icg name to icg id. The values for each column should be '1'. "
            "Please review\n{}".format(unique_df.query("icg_id != 1"))
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    id_groupby = df.groupby("icg_id").nunique()
    if not (id_groupby["icg_name"] == 1).all():
        msg = (
            "Review the icg name to icg id. The values for each column should be '1'. "
            "Please review\n{}".format(id_groupby.query("icg_name != 1"))
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    # the map should contain over 63,000 cause codes
    if df.shape[0] < 63000:
        msg = "There are only {} rows, we expect atleast 63,00 in a complete map".format(
            df.shape[0]
        )
        warnings.warn(msg)
        failed_tests.append(msg)

    if prod:
        if failed_tests:
            assert (
                False
            ), "{} tests have failed. Review the warning messages or {}".format(
                len(failed_tests), failed_tests
            )

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

    # get the cc to icg map
    df = get_clinical_process_data("cause_code_icg", map_version=map_version, prod=prod)

    # check diffs between icg names
    diffs = set(df.icg_name.unique()) - set(dat.icg_name.unique())
    assert not diffs, (
        "There are ICG names present which are not in the clean map. "
        "Please review this discrepancy in intermediate cause groups {}".format(diffs)
    )

    # check diffs between icg ids
    diffs = set(df.icg_id.unique()) - set(dat.icg_id.unique())
    assert not diffs, (
        "There are ICG IDs present which are not in the clean map. "
        "Please review this discrepancy in intermediate cause groups {}".format(diffs)
    )

    # compare ids by icg name for consistency
    chk = df.merge(dat, how="outer", on=["icg_name"])
    chk = chk[
        chk.code_system_id.notnull()
    ]  # drop rows that aren't in the cc to icg map
    chk_series = chk["icg_id_x"] == chk["icg_id_y"]
    assert chk_series.all(), (
        " Some ids don't match for the same intermediate cause names "
        "please review this df \n {}".format(chk[chk["icg_id_x"] != chk["icg_id_y"]])
    )

    # get a list of value columns to run tests on
    val_cols = ["icg_duration", "male", "female", "yld_age_start", "yld_age_end"]
    val_cols = [v for v in val_cols if v in dat.columns]

    # check for placeholder NA values
    failed_tests = []
    for col in val_cols:
        if col != "icg_measure":
            if dat[col].min() < 0:
                warnings.warn(
                    "{} has values below zero. This is probably a placeholder for "
                    "missing values. The data is not ready for production use".format(
                        col
                    )
                )
                failed_tests.append(col + "_placeholderNA")

        # male, female cols should only be 0 or 1.
        if col in ["male", "female"]:
            val_diff = set(dat[col].unique()).symmetric_difference(set([0, 1]))
            if val_diff:
                warnings.warn(
                    "{} has unexpected values, please review this set difference: {}".format(
                        col, val_diff
                    )
                )
                failed_tests.append(col + "_unexp_vals")

        # yld age end should be 95 at most
        if col in ["yld_age_end"]:
            if dat[col].max() > 95:
                max_df = dat[dat[col] > 95]
                warnings.warn(
                    "{} has values over 95 years. This may be acceptable but we haven't seen it before. \n{}".format(
                        col, max_df
                    )
                )
                failed_tests.append(col + "_too_high")

        # durations should be 365 days or less
        if col == "icg_duration":
            if dat[col].max() > 365:
                max_df = dat[dat[col] > 365]
                warnings.warn(
                    "{} has values over 365 days. This may be acceptable but we haven't seen it before. \n{}".format(
                        col, max_df
                    )
                )
                failed_tests.append(col + "_too_high")

    # check the values in the measure column, should only be inc or prev
    if "icg_measure" in dat.columns:
        val_diff = set(dat["icg_measure"].unique()).symmetric_difference(
            set(["inc", "prev"])
        )
        if val_diff:
            warnings.warn(
                "icg_measure has unexpected values, please review this set difference: {}".format(
                    val_diff
                )
            )
            failed_tests.append(col + "_unexp_vals")

    if prod:
        if failed_tests:
            assert False, (
                "{} tests have failed. These are: {{column_name_test_name}} {}. Review the warnings and "
                "source code for more information".format(
                    len(failed_tests), failed_tests
                )
            )

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
            assert False, (
                "{} tests have failed. These are: {{column_name_test_name}} {}. Review the warnings and "
                "source code for more information".format(
                    len(failed_tests), failed_tests
                )
            )

    if failed_tests:
        result = failed_tests
    else:
        result = "ICG bundle tests passed"

    return result


def test_code_sys(df, prod=True):
    """
    Check the code system table in the clinical mapping dB to ensure there aren't duplicated systems

    """
    no_dupes = df[["code_system_id", "code_system_name"]].drop_duplicates()

    assert df.shape[0] == no_dupes.shape[0], "There are duplicated code systems"

    return "Code system tests passed"


def test_bundle_measure():

    cc_m = get_clinical_process_data("cause_code_icg", prod=False)
    bun = get_clinical_process_data("icg_bundle", prod=False)

    m = (
        cc_m[["code_system_id", "icg_name", "icg_id", "icg_measure"]]
        .drop_duplicates()
        .merge(bun, how="outer", on=["icg_name", "icg_id"])
    )

    z = m[["icg_name", "icg_measure", "bundle_id"]].drop_duplicates()
    z[z.duplicated("bundle_id", keep=False)].sort_values("bundle_id")

    return m, z
