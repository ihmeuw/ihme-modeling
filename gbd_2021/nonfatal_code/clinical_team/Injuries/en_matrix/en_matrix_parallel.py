"""
Created on 9/19/2017

Hospital data has been stored wide during the formatting process
in the following directory- FILEPATH
This script will combine and collapse these files to produce the dataframe
structure is required for producing the EN matrix.

@author:
"""
import pandas as pd
import numpy as np
import ntpath
import glob
import platform
import datetime
import getpass
import sys
import time
import multiprocessing

from clinical_info.Mapping import clinical_mapping

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# load our functions
from clinical_info.Functions import hosp_prep


def read_stata_data(fpath, dev=False):
    if dev:
        stata_df = hosp_prep.read_stata_chunks(fpath, chunksize=100000, chunks=3)
    else:
        stata_df = pd.read_stata(fpath)
    return stata_df


def create_code_system(df):
    if "icd_vers" in df.columns:
        # gbd2015 sources don't have code_system_id yet
        df.loc[df.icd_vers == "ICD9_detail", "code_system_id"] = 1
        df.loc[df.icd_vers == "ICD10", "code_system_id"] = 2
    assert df.code_system_id.isnull().sum() == 0, "code system is missing"
    assert (
        pd.Series(df.code_system_id.unique()).isin([1, 2])
    ).all(), "bad code system ids"
    return df


def map_to_nfc(df, run_id, verbose=True):
    """
    Map icd codes in wide format to baby sequelae
    Alternatively we could reshape long, do the mapping then reshape wide
    """
    dx_cols = df.filter(regex="^dx_").columns.tolist()
    ecode_cols = df.filter(regex="^ecode_").columns.tolist()
    map_cols = dx_cols + ecode_cols

    maps = clinical_mapping.get_clinical_process_data(
        "cause_code_icg", map_version=MAP_VERSION
    )
    maps.rename({"icg_name": "nonfatal_cause_name"}, axis=1, inplace=True)
    # assert hosp_prep.verify_current_map(maps)
    # cast to string
    maps["cause_code"] = maps["cause_code"].astype(str)
    maps["cause_code"] = maps["cause_code"].str.replace("\W", "")

    # keep relevant columns
    maps = maps[["cause_code", "nonfatal_cause_name", "code_system_id"]].copy()

    # drop duplicate values
    maps = maps.drop_duplicates(subset=["cause_code", "code_system_id"])

    maps["cause_code"] = maps["cause_code"].str.upper()

    if verbose:
        print("shape of maps file".format(maps.shape))

    assert (
        df.code_system_id.isnull().sum() == 0
    ), "Code system is missing which will break the merge"
    if verbose:
        print("Map is ready")
    # This is the wide way
    for col in map_cols:
        # make sure that cause_code is a string
        df[col] = df[col].astype(str)
        # match on upper case
        df[col] = df[col].str.upper()

        maps[col + "_nfc"] = maps["nonfatal_cause_name"]
        to_map = maps[[col + "_nfc", "cause_code", "code_system_id"]].copy()

        before_values = df[col].value_counts()
        pre = df.shape[0]
        # merge the hospital data with excel spreadsheet maps
        df = df.merge(
            to_map,
            how="left",
            left_on=[col, "code_system_id"],
            right_on=["cause_code", "code_system_id"],
        )
        assert pre == df.shape[0], "Rows created after merge"

        # check the icd code value counts
        after_values = df[col].value_counts()  # value counts after
        # assert various things to verify merge
        assert set(before_values.index) == set(
            after_values.index
        ), "Unique ICD codes are not the same"
        df.drop([col, "cause_code"], axis=1, inplace=True)
        if verbose:
            print("Done mapping {}".format(col))
    if verbose:
        print("All done mapping")
    return df


def cond_age_binning(df):
    """
    Some sources need age binning, some don't. Depends on how the formatting scripts were
    written (Stata vs Python)
    """
    needs_age_binning = [
        "MEX_SINAIS",
        "NZL_NMDS",
        "CAN_DAD_94_09",
        "USA_HCUP_SID_09",
        "USA_HCUP_SID_08",
        "USA_HCUP_SID_07",
        "USA_HCUP_SID_06",
        "USA_HCUP_SID_04",
        "USA_HCUP_SID_03",
        "BRA_SIH",
        "USA_HCUP_SID_05",
        "PHL_HICC",
    ]
    assert df.source.unique().size <= 1, "Process one source at a time"
    # if age is present, apply age binning
    if "age" in df.columns:
        df.loc[df.age > 95, "age"] = 95  # set the really old ages to terminal age group
        if df.source.unique()[0] not in needs_age_binning:
            print("Source {} wasn't in the needs age binning list".format(df.source[0]))
        assert (
            df.age.isnull().sum() / float(df.shape[0])
        ) <= 0.005, "More than 0.5% of ages are missing. perhaps something is wrong with the data prep"
        df = hosp_prep.age_binning(df)

    # make sure age_start/end are not missing tons of values
    if "age_start" in df.columns:
        assert "age_end" in df.columns, "Where is age end"
        assert (
            df.age_start.isnull().sum() / float(df.shape[0])
        ) <= 0.005, "More than 0.5% of ages are missing. perhaps something is wrong with the data prep"
        assert (
            df.age_end.isnull().sum() / float(df.shape[0])
        ) <= 0.005, "More than 0.5% of ages are missing. perhaps something is wrong with the data prep"
    return df


def format_old_sources(df):
    if "sex_id" not in df.columns:
        df.rename(columns={"sex": "sex_id"}, inplace=True)
    if "facility_id" not in df.columns:
        df.rename(columns={"platform": "facility_id"}, inplace=True)
    if df.source.unique()[0] == "NZL_NMDS":
        df["location_id"] = 72
    return df


def make_live_discharges(df):
    """
    The injuries process uses only live discharges per Marlena
    """
    # for the newer sources drop rows that result in death (it's all indv record)
    indv_record = ["AUT_HDD", "PHL_HICC", "PRT_CAHS", "GEO", "CHL_MOH"]
    if "metric_discharges" not in df.columns:
        if df.source[0] not in indv_record:
            print("Source {} isn't in the indv record list".format(df.source.unique()))
        # drop deaths
        df = df[df.outcome_id != "death"].copy()
        # create live discharges
        df["live_discharges"] = 1
        return df
    else:
        sources = df.source.unique().tolist()
        if "ITA_HID" in sources or "KGZ_MHIF" in sources or "IND_SNH" in sources:
            df = df[df.outcome_id != "death"]
            df["live_discharges"] = df["metric_discharges"]
        else:
            # for the older sources subtract deaths from metric discharges
            needs_discharge_subtraction = [
                "MEX_SINAIS",
                "NZL_NMDS",
                "CAN_DAD_94_09",
                "USA_HCUP_SID_09",
                "USA_HCUP_SID_08",
                "USA_HCUP_SID_07",
                "USA_HCUP_SID_06",
                "USA_HCUP_SID_04",
                "USA_HCUP_SID_03",
                "BRA_SIH",
                "USA_HCUP_SID_05",
            ]
            df["live_discharges"] = df["metric_discharges"] - df["metric_deaths"]
            if df.live_discharges.min() < 0:
                print(
                    df[df.live_discharges < 0], "Why are live discharges less than 0?"
                )
        return df


def select_cols(df, dev=False):
    """
    Some sources were processed in stata by modifying the gbd2015 scripts at a point before
    age binning was done. Some sources were processed in Python at a point after. This resulted
    in many different columns being created and we don't need them.
    """
    dx_cols = df.filter(regex="^dx_").columns.tolist()
    ecode_cols = df.filter(regex="^ecode_").columns.tolist()
    final_cols = [
        "location_id",
        "age_start",
        "age_end",
        "sex_id",
        "facility_id",
        "live_discharges",
    ]
    final_cols = final_cols + dx_cols + ecode_cols
    if dev:
        final_cols.append("source")

    # fill missing values with _none
    df[dx_cols + ecode_cols] = df[dx_cols + ecode_cols].fillna("_none")
    return df[final_cols]


def en_keeper(df):
    """
    we only need baby sequelae that begin with e-code and n-code
    FLAG -- verify there aren't odd e or n baby sequelae
    """
    print("Starting with {} rows".format(df.shape[0]))
    dx_cols = df.filter(regex="^dx_|ecode_").columns.tolist()
    # print(dx_cols)
    ecode = " | ".join(
        ["(df['{}'].str.contains('e-code'))".format(col) for col in dx_cols]
    )
    ncode = " | ".join(
        ["(df['{}'].str.contains('n-code'))".format(col) for col in dx_cols]
    )
    test = ecode + " | " + ncode
    df = df[eval(test)]
    print("Ending with {} rows".format(df.shape[0]))
    return df


def test_data_structure(df):
    assert df.live_discharges.isnull().sum() == 0, "Null discharges"
    assert (df.isnull().sum() == 0).all(), "There are null values in {}".format(
        df.columns[df.isnull().sum() > 0]
    )


def collapser(df, verbose=False):
    if verbose:
        print("Starting rows are {}".format(df.shape[0]))
    groups = df.columns.drop("live_discharges").tolist()
    # do the collapse
    df = df.groupby(groups).agg({"live_discharges": "sum"}).reset_index()
    if verbose:
        print("Ending rows are {}".format(df.shape[0]))
    return df


def main(fpath, run_id):
    print("Starting on file {}".format(fpath))
    df = read_stata_data(fpath, dev=False)
    df = create_code_system(df)
    df = map_to_nfc(df, run_id, verbose=True)
    df = cond_age_binning(df)
    df = format_old_sources(df)
    df = make_live_discharges(df)
    df = en_keeper(df)
    df = select_cols(df, dev=True)
    print("Number of cols is now {}".format(df.shape[1]))
    test_data_structure(df)
    df = collapser(df, verbose=True)
    file_name = ntpath.basename(fpath)[:-4]
    df.to_hdf(
        f"FILEPATH", key="df", mode="w",
    )


if __name__ == "__main__":
    fpath = sys.argv[1]
    run_id = sys.argv[2]
    MAP_VERSION = sys.argv[3]
    main(fpath, run_id)
