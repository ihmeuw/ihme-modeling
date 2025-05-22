import numpy as np
import pandas as pd
from crosscutting_functions.pipeline_constants import poland as constants
from loguru import logger

"""
Within the Poland data set, at least for file source id 1, we've noticed that
there are inpatient admissions with multiple primary dx for the
same bene / admission date pair.

This module groups these admissions together and reassigns a corrected dx id
"""


def _dup_visit_number(df, merged_df, gb_cols):
    """
    If the data breaks the assumpution that the visit number is incremented for each
    unique bene_id / admission_date pair then increment the visit number and multiple by -1
    to avoid any collisions
    """

    logger.info(
        f"\nThere are {df.groupby(gb_cols)[gb_cols].nunique().shape[0]} unique "
        "bene ids / admission date pair that break the assumption that visit "
        "number is incremented for duplicated visits. Assigning negative visit numbers"
    )
    df["scale"] = df.groupby(gb_cols).cumcount()
    df["visit_number"] = (df["visit_number"] + df["scale"]) * -1
    df = df.drop("scale", axis=1)

    # apply the changes onto the overall df
    temp = df[gb_cols + ["cause_code"]]
    merged_df = pd.merge(
        merged_df, temp, on=temp.columns.tolist(), how="outer", indicator=True
    )
    merged_df = merged_df[merged_df._merge == "left_only"].drop("_merge", axis=1)
    return pd.concat([merged_df, df], sort=False).reset_index(drop=True)


def _dup_primary_dx(df, merged_df, gb_cols):
    """
    If there is multiple primary dx per bene / admission_date / visit number choose
    the first entry in the data set
    """
    temp_gb_cols = gb_cols + ["diagnosis_id", "visit_number"]
    df = merged_df.merge(df, on=temp_gb_cols)

    # the index col is from the original data set. Function assumes that this index
    # represents the actual time series of admissions / encounters
    # (e.g. admissions with index 1 where before admissions with index 2)
    df.sort_values(by=["bene_id", "index"], inplace=True)
    df["scale"] = df.groupby(gb_cols).cumcount()
    df["up_dx_id"] = df["diagnosis_id"] + df["scale"]
    df = df[df.scale != 0]
    df.drop(["scale", "vals"], axis=1, inplace=True)
    merged_df = merged_df.merge(df, on=merged_df.columns.tolist(), how="outer", indicator=True)
    merged_df.loc[merged_df._merge == "both", "diagnosis_id"] = merged_df.loc[
        merged_df._merge == "both", "up_dx_id"
    ]
    merged_df["diagnosis_id"] = merged_df["diagnosis_id"].astype(int)
    return merged_df.drop(["up_dx_id", "_merge"], axis=1)


def _add_helper_cols(df, gb_cols):
    """
    For rows that contain duplicate diagnosis ids per bene id and admission date
    append helper columns that list the minimum visit number (min_visit_num) and
    max diagnoisis id for the minimum visit number (max_dx_id_per_min_visit_num)
    """
    df_min_visit_num = df[gb_cols + ["visit_number"]]
    df_min_visit_num["abs_visit_number"] = df_min_visit_num["visit_number"].abs()
    df_min_visit_num = (
        df_min_visit_num.groupby(gb_cols)["abs_visit_number"]
        .min()
        .reset_index(name="min_visit_num")
    )

    df = df.merge(df_min_visit_num, on=gb_cols, validate="m:1")

    # correct for any rows that were processed via _dup_visit_number
    df.loc[df.visit_number < 0, "min_visit_num"] = (
        df[df.visit_number < 0]["min_visit_num"] * -1
    )

    df_max_dx_id_min_visit = df[df.visit_number == df.min_visit_num]
    df_max_dx_id_min_visit = (
        df_max_dx_id_min_visit.groupby(gb_cols)
        .agg({"diagnosis_id": "max"})
        .reset_index()
        .rename({"diagnosis_id": "max_dx_id_per_min_visit_num"}, axis=1)
    )

    return df.merge(df_max_dx_id_min_visit, on=gb_cols, validate="m:1")


def _edge_cases(df, gb_cols):
    """
    Handles two edge cases:
        1. Data breaks assumption that visit number is incremented for unique
            bene / admission date pair
        2. Multiple primary dx for same bene / admission_date / visit number
            (most common in otp but there are a few inp cases)
    """
    visit_number_counts = df.groupby(gb_cols)["visit_number"].nunique().reset_index()

    if any(visit_number_counts.visit_number < 2):
        multi_primary = visit_number_counts[visit_number_counts.visit_number < 2][gb_cols]
        multi_primary = multi_primary.merge(df, on=gb_cols)
        df = _dup_visit_number(df=multi_primary, merged_df=df, gb_cols=gb_cols)

    temp = (
        df.groupby(gb_cols + ["diagnosis_id", "visit_number"]).size().reset_index(name="vals")
    )
    if any(temp[temp.diagnosis_id == 1]["vals"] > 1):
        temp = temp[(temp.diagnosis_id == 1) & (temp.vals > 1)]
        df = _dup_primary_dx(df=temp, merged_df=df, gb_cols=gb_cols)

    return df


def group_same_day_admissions(df, df_id_visits):
    """
    Re-assign 'diagnosis_id' value when there are duplicate diagnosis ids
    for a given admission date and bene id.
    Arugments:
        df_id_visits (pd.DataFrame): bend id / admission date pairs for rows that
                                     have more than one primary diagnosis
    """
    gb_cols = ["bene_id", "admission_date"]

    m = df.merge(df_id_visits, on=gb_cols)

    m = _edge_cases(m, gb_cols)

    df_corrected = _add_helper_cols(m, gb_cols)

    # reset diagnosis ids for admissions that are not attributed to minimum visit number
    # for duplicated admission dates.
    # steps:
    #
    # 1. create the temp col "up_dx_id".
    # Represents new diagnosis_id for bene admissions that are greater than the min_visit_num.
    # min_visit_num is the minimum "visit_number" for bene admissions that have the
    # same admission_date. New dx_ids are iterated from the max_dx_id_per_min_visit_num
    #
    # 2. fill null values for up_dx_id.
    # Represents the diagnosis ids when "min_visit_num" is equal to "visit_number"
    #
    # 3. choose the correct diagnosis_id.
    # When up_dx_id is 0 then the correct diagnosis id is the value in 'diagnosis_id'
    # otherwise use value in 'up_dx_id'
    df_corrected["up_dx_id"] = df_corrected.loc[
        df_corrected.min_visit_num != df_corrected.visit_number,
        ["max_dx_id_per_min_visit_num", "diagnosis_id"],
    ].sum(axis=1)
    df_corrected["up_dx_id"] = df_corrected.up_dx_id.fillna(0).astype(int)
    df_corrected["diagnosis_id"] = np.where(
        df_corrected.up_dx_id == 0, df_corrected["diagnosis_id"], df_corrected["up_dx_id"]
    )

    # for inp data verify that there is only one primary dx per bene / admission date pair.
    # Currently there is no estimate for primary dx otp data
    # (and that notion goes against the notion of otp claims data)
    if all(df_corrected.is_otp == 0):
        temp = df_corrected[df_corrected.is_dup == 0]
        verify_df = (
            temp.groupby(gb_cols)["diagnosis_id"].value_counts().reset_index(name="counts")
        )
        if not all(verify_df[verify_df.diagnosis_id == 1]["counts"] == 1):
            raise RuntimeError("There are admissions with duplicate diagnosis ids")

    cols = [e for e in df_corrected.columns if e in constants.ORDERED_ICD_MART_COLS]
    return df_corrected[cols]


def main(df):
    """
    Adjust visit_number and diagnosis id for rows that have multiple primary dx
    for a given bene / admission pair
    """
    # count the number of visits per unique id and visit date
    df_id_visits = (
        df.groupby(["bene_id", "admission_date"], dropna=False)["dx_1"]
        .nunique()
        .reset_index(name="same_day_visit")
    )
    df_id_visits = df_id_visits[df_id_visits.same_day_visit > 1]

    if not df_id_visits.empty:
        pre_shape = df.shape
        df_corrected = group_same_day_admissions(df, df_id_visits)

        # remove rows that were corrected in group_same_day_admissions
        df_uncorrected = pd.merge(
            df, df_id_visits, on=["bene_id", "admission_date"], how="outer", indicator=True
        )

        df_uncorrected = (
            df_uncorrected[df_uncorrected._merge == "left_only"]
            .drop(["_merge", "same_day_visit"], axis=1)
            .reset_index(drop=True)
        )

        df = pd.concat([df_corrected, df_uncorrected], sort=False)

        if df.shape != pre_shape:
            raise RuntimeError("Shape of data post dx postion correction does not match input")
    return df
