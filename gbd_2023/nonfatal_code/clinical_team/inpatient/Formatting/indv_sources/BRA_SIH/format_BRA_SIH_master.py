"""
Master script for CI inpatient source BRA_SIH

ADDRESS

Notes:
- There is location info available that is more detailed than state. For
example, the year 1997 has 4959 unique locations

Raw Data lives in these folders:
FILEPATH
├── RD_1992_1999
├── RD_2000_2007
├── RD_2004_2013
└── RD_2008_2014

The worker script reads raw data, does light processing, and saves outputs in
FILEPATH
"""

import getpass
import glob
import subprocess
import warnings

import numpy as np
import pandas as pd

# load our functions
from crosscutting_functions import *


def submit_brazil_jobs():
    """
    sbatchs jobs by year for all years.

    Returns
    -------
    None
    """

    for year in list(range(1992, 2014 + 1)):

        sbatch = f"""
        sbatch
        -J BRA_SIH_{year}
        -A proj_hospital
        -o FILEPATH/{getpass.getuser()}/output/
        -e FILEPATH/{getpass.getuser()}/errors/
        ADDRESS
        FILEPATH/{getpass.getuser()}FILEPATH/python_shell_clinical_env.sh
        FILEPATH/{getpass.getuser()}FILEPATH/BRA_SIH/format_BRA_SIH.py
        {year}
        """
        sbatch = " ".join(sbatch.split())
        subprocess.call(sbatch, shell=True)
        print(f"Submitted year {year}.")

    # submit sbatch for 15 and 16
    sbatch = f"""
    sbatch
    -J BRA_SIH_15_16
    -A proj_hospital
    -o FILEPATH/{getpass.getuser()}/output/
    -e FILEPATH/{getpass.getuser()}/errors/
    ADDRESS
    FILEPATH/{getpass.getuser()}/FILEPATH/python_shell_clinical_env.sh
    FILEPATH/{getpass.getuser()}/FILEPATH/01_format_BRA_SIH_after_14.py \'15_16\'
    """

    sbatch = " ".join(sbatch.split())
    subprocess.call(sbatch, shell=True)
    print("Submitted years 2015 and 2016.")

    # submit sbatch for 17 through 20
    sbatch = f"""
    sbatch
    -J BRA_SIH_17_20
    -A proj_hospital
    -o FILEPATH/{getpass.getuser()}/output/
    -e FILEPATH/{getpass.getuser()}/errors/
    ADDRESS
    FILEPATH/{getpass.getuser()}FILEPATH/python_shell_clinical_env.sh
    FILEPATH/{getpass.getuser()}FILEPATH/01_format_BRA_SIH_after_14.py \'17_20\'
    """

    sbatch = " ".join(sbatch.split())
    subprocess.call(sbatch, shell=True)
    print("Submitted years 2017 through 2020.")


def collapse_one_year(df):
    """
    Collapses data by every column

    Parameters
    ----------
    df: Pandas DataFrame
        data

    Returns
    -------
    Pandas DataFrame collapsed. Has the same columns it began with.
    """

    df = df.fillna(value="NULL")

    assert "val" in df.columns, "val needs to be in columns"

    groupby_cols = [col for col in df.columns if col != "val"]

    df = df.groupby(groupby_cols, as_index=False).val.sum()

    return df


def read_finished_files():
    """
    Reads in results of sbatched worker scripts.

    Returns
    -------
    Pandas DataFrame made from the outputs of all worker scripts
    """
    single_year_processed_dir = (
        "FILEPATH"
        "BRA_SIH/data/intermediate/"
        "single_year_processed/"
    )
    files = glob.glob(f"{single_year_processed_dir}/*.csv")
    files = sorted(files)

    # empty dataframe
    df = pd.DataFrame()

    for f in files:
        temp = pd.read_csv(f)
        df = df.append(other=temp, ignore_index=True, sort=False)
        df = collapse_one_year(df)

    df = df.replace(to_replace="NULL", value=np.nan)

    return df


def make_nid(df):
    """
    Adds NID column to dataframe.

    Parameters
    ----------
    df: Pandas DataFrame
        data without NID column

    Returns
    -------
    Pandas DataFrame with nid column added
    """

    nid_map = {
        2014: 221323,
        2013: 104258,
        2012: 104257,
        2011: 104256,
        2010: 104255,
        2009: 26333,
        2008: 87012,
        2007: 87013,
        2006: 87014,
        2005: 104254,
        2004: 104253,
        2003: 104252,
        2002: 104251,
        2001: 104250,
        2000: 104249,
        1999: 104248,
        1998: 104247,
        1997: 104246,
        1996: 104245,
        1995: 104244,
        1994: 104243,
        1993: 104242,
        1992: 104241,
    }

    df["nid"] = df.year_id.map(nid_map)

    null_nid_years = df[df.nid.isnull()].year_id.unique().tolist()

    assert (
        df.nid.notnull().all()
    ), f"There are null NIDs for these years: {null_nid_years}"

    return df


def fill_columns(df):
    """
    Fills out extra columns needed for the inpatient pipeline.

    Parameters
    ----------
    df: Pandas DataFrame

    Returns
    -------
    Pandas DataFrame with columns expected by later steps of inpatient process
    """

    final_columns = [
        "age_group_unit",
        "age_start",
        "age_end",
        "year_start",
        "year_end",
        "location_id",
        "representative_id",
        "sex_id",
        "diagnosis_id",
        "metric_id",
        "outcome_id",
        "val",
        "source",
        "nid",
        "facility_id",
        "code_system_id",
        "cause_code",
        "dx_1",
        "dx_2",
    ]

    df["age_group_unit"] = 1
    df["year_start"] = df.year_id
    df["year_end"] = df.year_id
    df["representative_id"] = 1
    df["metric_id"] = 1
    df["source"] = "BRA_SIH"
    df["facility_id"] = "inpatient unknown"
    df["code_system_id"] = 2  # icd 10
    df.loc[df.year_start <= 1997, "code_system_id"] = 1  # icd 9

    existing_cols = df.columns.tolist()
    # safe way to drop only columns that surely exist
    drop_cols = list(set(existing_cols) - set(final_columns))
    warnings.warn(f"Dropping these columns: {drop_cols}")
    df = df.drop(drop_cols, axis=1)

    return df


def save_wide_dx(df):
    """
    Saves the data in the wide format so it can be used in other processes,
    such as the Injuries EN-Matrix

    Parameters
    ----------
    df: Pandas DataFrame
        data stored wide on diagnosis

    Returns
    -------
    None
    """
    df["dx_1"] = df.dx_1.astype(str)
    df.to_stata("FILEPATH/BRA_SIH_1992_2014.dta")
    write_path = (
        "FILEPATH" "wide_format_hdf/BRA_SIH_1992_2014.H5"
    )
    hosp_prep.write_hosp_file(df, write_path, backup=False)


def move_ecodes(df):
    """
    Wraps around hosp_prep.move_ecodes_into_primary_dx() to handle the two
    different ICD versions in our dataframe.
    """

    assert "code_system_id" in df.columns, "'code_system_id' column is missing"
    assert "val" in df.columns, "'val' column is missing"

    # make an ICD 9 df and an ICD 10 df
    df_icd9 = df[df.code_system_id == 1].copy()
    df = df[df.code_system_id == 2].copy()

    df_icd9 = hosp_prep.move_ecodes_into_primary_dx(df=df_icd9, icd_version=9)
    df = hosp_prep.move_ecodes_into_primary_dx(df=df, icd_version=10)

    # put them back together
    df = pd.concat([df_icd9, df], ignore_index=True, sort=False)

    return df


def reshape_long_dx(df):
    """
    Transforms dataframe from wide diagnosis columns to long on diagnosis.

    Parameters
    ----------
    df: Pandas DataFrame
        data stored wide on diagnosis

    Returns
    -------
    Pandas DataFrame long on diagnosis
    """

    # Find all columns with dx_ at the start
    diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

    # Remove non-alphanumeric characters from dx feats
    for feat in diagnosis_feats:
        df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
        df.loc[df[feat] == "nan", feat] = np.nan

    if len(diagnosis_feats) > 1:
        # Reshape diagnoses from wide to long
        #   - review `hosp_prep.py` for additional documentation
        df = hosp_prep.stack_merger(df)

    elif len(diagnosis_feats) == 1:
        df.rename(columns={"dx_1": "cause_code"}, inplace=True)
        df["diagnosis_id"] = 1

    else:
        print("Something went wrong, there are no ICD code features")

    # There are some rows with null cause_code and diagnosis_id = 2
    condition = (df.diagnosis_id == 2) & (df.cause_code.isnull())
    print(
        f"There are {df[condition].shape} rows where diagnosis_id is "
        "2 and cause_code is null. Dropping them."
    )
    df = df[~condition].copy()

    return df


def finalize(df):
    """
    Attaches the 2015-2016 data onto the 1992-2014 data, ensures all columns
    are present, and performs a collapse. 2015-2016 was already finalized.

    Parameters
    ----------
    df: Pandas DataFrame

    Returns
    -------
    Pandas DataFrame with all years of data ready to be saved
    """

    # Group by all features we want to keep and sums 'val'
    groupby_cols = [
        "cause_code",
        "diagnosis_id",
        "sex_id",
        "age_start",
        "age_end",
        "year_start",
        "year_end",
        "location_id",
        "nid",
        "age_group_unit",
        "source",
        "facility_id",
        "code_system_id",
        "outcome_id",
        "representative_id",
        "metric_id",
    ]

    assert set(groupby_cols) <= set(df.columns), "There are missing columns"
    assert (
        df[groupby_cols + ["val"]].notnull().all().all()
    ), "There are nulls before a groupby"
    df = df.groupby(groupby_cols).agg({"val": "sum"}).reset_index()

    # Arrange columns in our standardized feature order
    columns_before = df.columns
    hosp_frmat_feat = [
        "age_group_unit",
        "age_start",
        "age_end",
        "year_start",
        "year_end",
        "location_id",
        "representative_id",
        "sex_id",
        "diagnosis_id",
        "metric_id",
        "outcome_id",
        "val",
        "source",
        "nid",
        "facility_id",
        "code_system_id",
        "cause_code",
    ]
    df = df[hosp_frmat_feat]
    columns_after = df.columns

    # check if all columns are there
    assert set(columns_before) == set(
        columns_after
    ), "You lost or added a column when reordering"

    # get 2015 and 2016
    bra_15_16 = pd.read_hdf(
        "FILEPATH"
        "FILEPATH"
        "formatted_BRA_SIH_15_16.H5"
    )

    assert set(bra_15_16.columns) == set(df.columns), "Columns need to match"

    df = df.append(bra_15_16, ignore_index=True, sort=False)

    # get 2017 through 2020
    bra_17_20 = pd.read_hdf(
        "FILEPATH"
        "FILEPATH"
        "formatted_BRA_SIH_17_20.H5"
    )

    assert set(bra_17_20.columns) == set(df.columns), "Columns need to match"

    df = df.append(bra_17_20, ignore_index=True, sort=False)

    return df


def save_final_brazil(df):
    """
    Saves final data.

    Parameters
    ----------
    df: Pandas DataFrame
        final data

    Returns
    -------
    None
    """

    # Saving the file
    write_path = (
        "FILEPATH"
        "/data/intermediate/formatted_BRA_SIH.H5"
    )

    hosp_prep.write_hosp_file(df, write_path, backup=True)

    print(f"Saved data to {write_path}")


def main():
    """
    Processes and formats Brazil SIH data for years 1992 - 2016

    Returns
    -------
    None
    """
    years = list(range(1992, 2014 + 1))
    submit_brazil_jobs()
    hosp_prep.job_holder(job_name="BRA_SIH", sleep_time=65)

    print("Reading finished files")
    df = read_finished_files()
    assert set(years) == set(
        df.year_id.unique()
    ), "Years present don't match expectation."

    print("Making NIDs")
    df = make_nid(df)

    print("Filling columns")
    df = fill_columns(df)

    # NOTE: order of DX doesn't matter for E-N matrix code.
    print("Saving wide dx")
    save_wide_dx(df)

    print("Moving ecodes")
    df = move_ecodes(df)

    print("Reshaping long dx")
    df = reshape_long_dx(df)

    # 2015 - 2020 get added here
    print("Finalizing data")
    df = finalize(df)

    print("Saving final data")
    save_final_brazil(df)


if __name__ == "__main__":
    main()
