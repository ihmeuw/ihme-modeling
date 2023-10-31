import warnings
import pandas as pd
import datetime
import getpass
import sys
import time

# load our functions
if getpass.getuser() == "USER":
    USER_path = r"FILEPATH"
    sys.path.append(USER_path)
if getpass.getuser() == "USER":
    USER_path = r"FILEPATH"
    sys.path.append(USER_path)

import gbd_hosp_prep
import hosp_prep

#############
# FUNCTIONS #
#############


def print_diff_cols(df, df2):
    print(
        (
            "Set difference of columns: {}".format(
                set(df2.columns).symmetric_difference(set(df.columns))
            )
        )
    )


# load hospital data from gbd 2015
def format_hospital(write_hdf=False, downsize=False, verbose=True, head=False):
    """
    Function that reads all our formatted data and concatenates them together.

    Arguments:
        write_hdf: (bool) If true, writes an HDF H5 file of the aggregated data
        downsize: (bool) If true, numeric types will be cast down to the
            smallest size
        verbose: (bool) If true will print progress and information
        head: (bool) If true will only grab first 1000 rows of each source.
    """
    start = time.time()
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
    ###########################################
    # READ AND PREP 2015 GBD DATA
    ###########################################
    print("reading gbd2015 hospital data...")

    # add data for USA NAMCS
    us_filepath = r"FILEPATH"
    us = pd.read_stata(us_filepath)
    # rename cols to fit current rubric
    us.rename(
        columns={"dx_mapped_": "cause_code", "dx_ecode_id": "diagnosis_id"},
        inplace=True,
    )
    # drop the USA NAMCS data from all_hospital_epi.dta because
    # it was double counted
    # df = df[df['source'] != "USA_NAMCS"]

    # add data for NZL 2015
    # # drop the nzl data that counts day cases as inpatient cases
    # df = df[df['source'] != "NZL_NMDS"]

    loop_list = [us]
    stata_sources = [
        "NOR_NIPH_08_12",
        "USA_HCUP_SID_03",
        "USA_HCUP_SID_04",
        "USA_HCUP_SID_05",
        "USA_HCUP_SID_06",
        "USA_HCUP_SID_07",
        "USA_HCUP_SID_08",
        "USA_HCUP_SID_09",
        "USA_NHDS_79_10",
        "BRA_SIH",
        "MEX_SINAIS",
        "NZL_NMDS",
        "EUR_HMDB",
        "SWE_PATIENT_REGISTRY_98_12",
    ]

    for source in stata_sources:
        if verbose:
            print(source)
        filepath = r"FILEPATH"
        if head:
            new_df = hosp_prep.read_stata_chunks(filepath, chunksize=1000, chunks=1)
        else:
            new_df = pd.read_stata(filepath)
        if source == "EUR_HMDB":
            new_df.drop(["metric_bed_days", "metric_day_cases"], axis=1, inplace=True)
        if verbose:
            print_diff_cols(new_df, us)

        loop_list.append(new_df)

    # append them together
    # df = pd.concat([df, recovered, us, nzl])
    df = pd.concat(loop_list, ignore_index=True)

    # rename USA_HCUP sources
    df.loc[df.source.str.startswith("USA_HCUP_SID_"), "source"] = "USA_HCUP_SID"

    # make age end exlusive
    df.loc[df.age_end > 1, "age_end"] = df.loc[df.age_end > 1, "age_end"] + 1
    print(
        "Concatinated all gbd 2015 data together in {} minutes".format(
            (time.time() - start) / 60
        )
    )

    # Rename columns to match gbd 2016 requirements
    rename_dict = {
        "cases": "val",
        "sex": "sex_id",
        "NID": "nid",
        "dx_ecode_id": "diagnosis_id",
        "dx_mapped_": "cause_code",
        "icd_vers": "code_system_id",
        "platform": "facility_id",
        "national": "representative_id",
        "year": "year_id",
    }
    df.rename(columns=rename_dict, inplace=True)

    # Drop unneeded columns from the old data
    # df.drop(['year_gbd'], axis=1, inplace=True)
    df.drop(["subdiv", "iso3", "deaths"], axis=1, inplace=True)

    # change id from 0 to 3 for "not representative"
    df.loc[df["representative_id"] == 0, "representative_id"] = 3

    # Clean the old data
    df["nid"] = df["nid"].astype(int)  # nid does not need to be a float
    assert len(df["code_system_id"].unique()) <= 2, (
        "We assume that there " "only 2 ICD formats present: ICD 9 and ICD 10"
    )
    df["code_system_id"].replace(["ICD9_detail", "ICD10"], [1, 2], inplace=True)
    # replace ICD9 with 1 and ICD10 with 2
    df["facility_id"].replace(
        [1, 2], ["inpatient unknown", "outpatient unknown"], inplace=True
    )
    # replace 1 with 'inpatient unknown' and 2 with 'outpatient unknown

    # Add columns to the old data
    df["outcome_id"] = "case"  # all rows are cases (discharges and deaths)
    df["metric_id"] = 1  # all rows are counts
    df["age_group_unit"] = 1  # all ages are in years
    df["year_start"] = df["year_id"]
    df["year_end"] = df["year_id"]
    df.drop("year_id", axis=1, inplace=True)

    df = df[df["val"] > 0]

    # fix age group on nzl.  It says it's 0-1, but it's actually 0-4
    df.loc[(df.source == "NZL_NMDS") & (df.age_end == 1), "age_end"] = 5

    print("Done formating GBD 2015 in {} minutes".format((time.time() - start) / 60))

    ###########################################
    # READ NEW DATA SOURCES
    ###########################################

    print("Reading in new data")

    h5_sources = [
        "KGZ_MHIF",
        "IND_SNH",
        "CHN_NHSIRS",
        "TUR_DRGHID",
        "CHL_MOH",
        "DEU_HSRS",
        "PRT_CAHS",
        "ITA_IMCH",
        "PHL_HICC",
        "NPL_HID",
        "QAT_AIDA",
        "KEN_IMMS",
        "GEO_COL",
        "UK_HOSPITAL_STATISTICS",
        "IDN_SIRS",
        "VNM_MOH",
        "JOR_ABHD",
        # GBD 2015 sources
        "AUT_HDD",
        "ECU_INEC_97_14",
    ]

    loop_list = [df]
    for source in h5_sources:
        if verbose:
            print(source)
        filepath = r"FILEPATH"
        if head:
            new_df = pd.read_hdf(filepath, key="df", start=0, stop=1000)
        else:
            new_df = pd.read_hdf(filepath, key="df")
        assert (
            set(df.columns).symmetric_difference(set(new_df.columns)) == {"deaths"}
            or set(df.columns).symmetric_difference(set(new_df.columns)) == set()
        ), print_diff_cols(new_df, df)
        # assert set(df.columns) == set(new_df.columns),\
        #     print_diff_cols(new_df, df)
        loop_list.append(new_df)

    # concat everything together
    df = pd.concat(loop_list, ignore_index=True)
    del loop_list
    print("Done reading in new data in {} minutes".format((time.time() - start) / 60))

    # Georgia has some ages for which age_group_id doesn't exist for now
    # so for the time being drop the years where those ages exist
    df = df.loc[(df.source != "GEO_COL") | (~df.year_start.isin([2012, 2013]))]

    # England has only E codes and N codes, but they include day cases
    #  and we can't get rid of them.
    df = df[df.location_id != 4749]  # this is location_id for England

    # in case a dataset has negative values for some reason, like VNM did
    df = df[df["val"] > 0]

    ###########################################
    # Format all data sets
    ###########################################

    # standardize unknown sexes
    df.loc[(df.sex_id != 1) & (df.sex_id != 2), "sex_id"] = 3

    # Reorder columns
    hosp_frmat_feat = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_start",
        "age_end",
        "sex_id",
        "source",
        "nid",
        "representative_id",
        "facility_id",
        "code_system_id",
        "diagnosis_id",
        "cause_code",
        "outcome_id",
        "metric_id",
        "val",
    ]

    columns_before = df.columns
    df = df[hosp_frmat_feat]  # reorder
    columns_after = df.columns

    assert set(columns_before) == set(
        columns_after
    ), "You accidentally dropped a column while reordering"

    print("converting datatypes and downsizing...")
    # enforce some datatypes
    if verbose:
        print(df.info(memory_usage="deep"))
    df["cause_code"] = df["cause_code"].astype("str")
    df["source"] = df["source"].astype("category")
    df["facility_id"] = df["facility_id"].astype("category")
    df["outcome_id"] = df["outcome_id"].astype("category")

    int_list = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_start",
        "age_end",
        "sex_id",
        "nid",
        "representative_id",
        "code_system_id",
        "diagnosis_id",
        "metric_id",
    ]

    if downsize:
        # downsize. this cuts size of data in half.
        for col in int_list:
            try:
                df[col] = pd.to_numeric(df[col], errors="raise")
            except:
                print(col, "<- this col didn't work")
    if verbose:
        print(df.info(memory_usage="deep"))

    # FLAG this may change as formatting inputs change
    # do some stuff to make age_end uniform
    bad_ends = df[(df.age_end == 29)].source.unique()
    for asource in bad_ends:
        # make age end exlusive
        df.loc[(df.age_end > 1) & (df.source == asource), "age_end"] = (
            df.loc[(df.age_end > 1) & (df.source == asource), "age_end"] + 1
        )

    df.loc[df.age_start > 95, "age_start"] = 95
    df.loc[df.age_start >= 95, "age_end"] = 125
    df.loc[df.age_end == 100, "age_end"] = 125

    # switch from age start/end to age group id
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    # Final check
    # check number of unique feature levels
    assert (
        len(df["diagnosis_id"].unique()) <= 2
    ), "diagnosis_id should have 2 or fewer feature levels"
    assert (
        len(df["code_system_id"].unique()) <= 2
    ), "code_system_id should have 2 or fewer feature levels"

    # assert that a single source doesn't have cases combined with
    # anything other than "unknown"
    for asource in df.source.unique():
        outcomes = df[df["source"] == asource].outcome_id.unique()
        if "case" in outcomes:
            assert "death" not in outcomes
            assert "discharge" not in outcomes

    print(
        "Done processing formatted files in {} minutes".format(
            (time.time() - start) / 60
        )
    )
    if write_hdf == True:
        # Saving the file
        write_path = r"FILEPATH"
        category_cols = ["cause_code", "source", "facility_id", "outcome_id"]
        for col in category_cols:
            df[col] = df[col].astype(str)
        df.to_hdf(
            write_path, key="df", format="table", complib="blosc", complevel=5, mode="w"
        )

    return df
