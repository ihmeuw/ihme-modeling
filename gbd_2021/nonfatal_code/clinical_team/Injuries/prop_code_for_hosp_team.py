"""
Author: 
Date: 9-11/2017
Purpose: Calculate injury-specific correction factor to correct for not all cases being coded to E-Code.
"""

import pandas as pd
import numpy as np
import os
import glob
import datetime
import sys
from db_queries import get_location_metadata

from clinical_info.Functions import hosp_prep, data_structure_utils


def subset_to_en(df):
    """
    Subset to only E and N codes, generate code-type var, and drop un-needed
    vars.

    Args:
        df: Pandas DataFrame
            Contains all the data.  Should be mapped to icg_name.
    """

    # subset to e-n codes and note which it is
    df = df[
        (df.icg_name.str.contains("e-code")) | (df.icg_name.str.contains("n-code"))
    ].copy()
    df.loc[(df.icg_name.str.contains("e-code")), "code"] = "E"
    df.loc[(df.icg_name.str.contains("n-code")), "code"] = "N"

    # keep necessary cols
    df = df[
        [
            "location_id",
            "year_start",
            "year_end",
            "nid",
            "facility_id",
            "diagnosis_id",
            "val",
            "code",
            "source",
        ]
    ].copy()

    # clean up facility_id
    # 'inpatient unknown', 'outpatient unknown', 'emergency', 'hospital', 'day clinic'
    df.loc[
        df.facility_id.isin(["inpatient unknown", "hospital"]), "facility_id"
    ] = "inpatient unknown"
    df.loc[
        df.facility_id.isin(["outpatient unknown", "emergency", "day clinic"]),
        "facility_id",
    ] = "outpatient unknown"

    # sum over all demographics -- don't care about age or sex anymore
    df = (
        df.groupby(
            [
                "location_id",
                "year_start",
                "year_end",
                "nid",
                "facility_id",
                "diagnosis_id",
                "code",
                "source",
            ]
        )
        .sum()
        .reset_index()
    )

    return df


def collapse_years(df):
    """
    Aggregate to dismod years so that these proportions match the inpatient
    process.  This code is basically lifted from elsewhere in our codebase.
    Drops NID from the dataframe.

    Before we have years like 1990, 1991, ..., 2014, 2015.  After we will have:
        1988-1992
        1993-1997
        1998-2002
        2003-2007
        2008-2012
        2013-2017
    """

    # change nid: NOTE that NID is not in this groupby
    groups = [
        "location_id",
        "year_start",
        "year_end",
        "facility_id",
        "diagnosis_id",
        "code",
    ]

    # first change years to 5-year bands
    df = hosp_prep.year_binner(df)

    # groupby
    df = df.groupby(groups).agg({"val": "sum"}).reset_index()

    return df


def calc_prop(df, indexcols):
    """
    Calculate proportion coding of total codes for a source.

    Args:
        df: (DataFrame) Contains the data to compute proportions
        indexcols: (list) Contains columns that identify a source of data
    """

    # get total and numerator
    df["total"] = df.groupby(indexcols)["val"].transform("sum")
    df["numerator"] = df.groupby(indexcols + ["diagnosis_id", "code"])["val"].transform(
        "sum"
    )

    # divide to get proportion
    df["prop"] = df.numerator / df.total

    return df


def calc_marginal(df_prim, df_mult, indexcols):
    """
    Calculate the marginal effect of having more than 1 diagnosis field.
    Averages over platform.

    Args:
        df_prim (DataFrame): Contains proportion coding to E-Code in primary
            diagnosis for sources with multiple diagnoses
        df_mult (DataFrame): Contains the proportion coding to E-code in all
            fields for the sources with multiple diagnoses
        indexcols: (list) Contains columns that identify a source of data
    """

    # merge primary and multiple proportions from the multiple diagnoses data
    # set
    df = df_prim.merge(df_mult, on=indexcols)

    # calculate how much *more* of a bump something needs to be consistent
    # w/ multiple diagnoses
    df["marg"] = df.prop_total / df.prop_prim

    # take the mean over ONLY facility ID. we don't have enough data to do a
    # different demographic split
    df = df[["facility_id", "marg"]]
    df = df.groupby("facility_id").mean().reset_index()

    return df


def drop_utla_data(df):
    """
    this function drops UTLA data.  Uses locatoin set 9!  same results as
    location set 2 though.
    """

    locs = get_location_metadata(location_set_id=9, gbd_round_id=4)

    england_subnat = list(
        locs[locs.path_to_top_parent.str.contains(",4749,")].location_id.unique()
    )

    df = df[~df.location_id.isin(england_subnat)].copy()

    return df


def make_inj_factors(
    out_dir,
    run_id,
    cutoff=0.15,
    return_result=False,
    save_results=True,
    pass_in_data=False,
    data=None,
    collapse_years_switch=False,
):
    """
    Main function that runs helper functions and coordinates intermediate data
    manipulations.  This function starts with formatted inpatient data that has
    been mapped to icg_name.  It calculates proportions of the time
    that sources code to E-Codes in a variety of ways.  See comments and
    docstrings for details.

    This function can be ran interactively or through a qsub.  The parameters
    pass_in_data and data are for running interactively.  If ran via qsub
    then data is read in, and there are expectations about the name and
    location of the data.

    Args:
        out_dir: (str) Location where the data will be saved.  File name is
            taken care of inside this function
        return_result: (bool) If true then the data will be returned.
        save_results: (bool) If true the data will be saved at out_dir with the
            name "inj_factors.H5" or "inj_factors_collapsed_years.H5" depending
            on collapse_years_switch.
        collapse_years_switch: (bool) If True, would collapse into 5 year bands
            1988-1992
            1993-1997
            1998-2002
            2003-2007
            2008-2012
            2013-2017

    Returns:
        Proportions of time that sources code to E-Codes, if return_result ==
        True.  Saves data at location specified by out_dir, if save_results ==
        True.
    """

    print("Starting make_inj_factors() function")

    files = glob.glob("FILEPATH".format(r=run_id))
    assert len(files) == 1, "too many files"
    data = pd.read_hdf(files[0])
    data = data_structure_utils.cat_to_str(data)

    # We can't (or don't) year bin data before 1988. We dont use it also. drop it
    # Cause there's no Envelope!
    data = data[data.year_start >= 1988]
    print("Read in {}".format(files[0]))

    # validate that needed columns are present
    required_cols = {
        "location_id",
        "year_start",
        "year_end",
        "nid",
        "facility_id",
        "val",
        "icg_name",
        "diagnosis_id",
    }

    assert_msg = """
        Not all required columns are present.
        The required columns are:
        {}
        """.format(
        required_cols
    )
    assert required_cols.issubset(set(data.columns)), assert_msg

    # needs certain number of rows
    assert data.shape[0] > 0, "Data has zero rows"

    assert_msg = "Need to have both primary and other diagnoses present in data"
    assert {1, 2}.issubset(set(data.diagnosis_id.unique())), assert_msg

    # change nid
    #     indexcols = ['location_id', 'year_start', 'year_end', 'nid', 'facility_id']
    if collapse_years_switch:
        indexcols = ["location_id", "year_start", "year_end", "facility_id"]
    if not collapse_years_switch:
        indexcols = ["location_id", "year_start", "year_end", "facility_id", "nid"]

    # subset and collapse over E-N code
    print("Keeping only E or N Code rows")
    sub = subset_to_en(data)

    # aggregate to dismod years here
    if collapse_years_switch:
        # chanage nid
        sub.drop("nid", axis=1, inplace=True)
        sub = collapse_years(sub)

    assert_msg = "Data has 0 rows after subsetting, there are no Injuries"
    assert sub.shape[0] > 0, assert_msg

    # get a skeleton of demographics to merge on to make sure we have
    # everything at the end
    skeleton = sub[indexcols].drop_duplicates()
    assert skeleton.shape[0] > 0, "Skeleton has 0 rows"

    # subset to only nid-year-platforms with multiple diagnosis fields
    print("grouping and splitting into singlediag and multdiag")
    sub["num_diag"] = sub.groupby(indexcols)["diagnosis_id"].transform("sum")
    multdiag = sub.loc[sub.num_diag >= 3]
    singlediag = sub.loc[sub.num_diag <= 2]
    assert multdiag.shape[0] > 0, "Has 0 rows"
    assert singlediag.shape[0] > 0, "Has 0 rows"

    # calculate the proportion coding to E-code in primary diagnosis for the
    # sources with multiple diagnoses
    print(
        "Calculating the proportion coding to E-Code in primary diagnosis for"
        " sources with multiple diagnoses"
    )
    prim_mult = calc_prop(multdiag.loc[multdiag.diagnosis_id == 1].copy(), indexcols)
    prim_mult = prim_mult.loc[prim_mult.code == "E"]
    prim_mult = prim_mult[indexcols + ["prop"]]
    prim_mult.rename(columns={"prop": "prop_prim"}, inplace=True)
    assert prim_mult.shape[0] > 0, "Has 0 rows"

    # calculate the proportion coding to E-code in all fields for the sources
    # with multiple diagnoses
    print(
        "Calculating the proportion coding to E-code in all fields for the"
        " sources with multiple diagnoses"
    )
    all_mult = calc_prop(multdiag.copy(), indexcols)
    all_mult = all_mult.loc[(all_mult.code == "E") & (all_mult.diagnosis_id == 1)]
    all_mult = all_mult[indexcols + ["prop"]]
    all_mult.rename(columns={"prop": "prop_total"}, inplace=True)
    assert all_mult.shape[0] > 0, "Has 0 rows"

    # calculate the "marginal" effect of having more than 1 diagnosis field
    print("Calculating the 'marginal' effect of having more than 1 diagnosis" " field")
    marg = calc_marginal(prim_mult.copy(), all_mult, indexcols)
    assert marg.shape[0] > 0, "Has 0 rows"

    # calculate the proportion coding to E-code among sources with only
    # primary diagnosis fields
    print(
        "Calculating the proportion coding to E-code among sources with only"
        " primary diagnosis fields"
    )
    prim_sing = calc_prop(singlediag.copy(), indexcols)
    prim_sing = prim_sing.loc[prim_sing.code == "E"]
    prim_sing = prim_sing.merge(marg, on=["facility_id"])
    prim_sing["prop"] = prim_sing.prop * prim_sing.marg
    assert prim_sing.shape[0] > 0, "Has 0 rows"

    # put the data frames back together -- (1) the E-prim / E-all + N-all for
    # multiple diagnoses and the (2) E-prim
    # plus bumped up w/ marginal proportion for the single diagnosis sources
    mult = all_mult[indexcols + ["prop_total"]]
    mult.rename(columns={"prop_total": "prop"}, inplace=True)

    sing = prim_sing[indexcols + ["prop"]]

    factors = mult.append(sing)

    # merge on the skeleton to make sure we didn't miss anything. We want to
    # fill NA rows with 0 because that means they didn't have
    # any e-codes at all!
    print("Merging results back onto skeleton")
    result = skeleton.merge(factors)
    assert result.shape[0] > 0, "Final results 0 rows"
    result.fillna(0, inplace=True)

    result["factor"] = 1 / result.prop
    result["remove"] = np.where(result.prop < cutoff, 1, 0)

    # drop utla data
    result = drop_utla_data(result)

    # save the factors
    if save_results:
        print("Saving...")

        if collapse_years_switch:
            extra_name = "_collapsed_years"
        else:
            extra_name = ""
        # save main
        filepath = os.path.join(out_dir, "inj_factors{}.H5".format(extra_name))
        result.to_hdf(
            filepath,
            mode="w",
            format="table",
            key="key",
            data_columns=[
                "location_id",
                "year_start",
                "year_end",
                "nid",
                "facility_id",
            ],
        )

    if return_result:
        return result


if __name__ == "__main__":
    run_id = sys.argv[1]
    run_id = run_id.replace("\r", "")

    cutoff = 0.15
    out_dir = "FILEPATH".format(run_id)

    make_inj_factors(
        cutoff=cutoff, out_dir=out_dir, collapse_years_switch=True, run_id=run_id
    )
    make_inj_factors(
        cutoff=cutoff, out_dir=out_dir, collapse_years_switch=False, run_id=run_id
    )

