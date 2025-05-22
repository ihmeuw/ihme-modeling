from typing import Dict, List, Union

import numpy as np
import pandas as pd

from crosscutting_functions.general_purpose import natural_sort


def test_case_counts(
    df: pd.DataFrame, compare_df: pd.DataFrame, dfs_must_equal_eachother: bool = True
) -> Union[List[str], str]:
    """
    Make sure the inpatient data isn't losing or gaining any admissions, or somehow
    transfering cases by sex or icd
    """
    dfcols = df.columns
    if "val" in dfcols:
        sumcol = "val"
    elif "cases" in dfcols:
        sumcol = "cases"

    bad_results = []

    # test 1, total case counts
    if not df[sumcol].sum().round(3) == compare_df[sumcol].sum().round(3):
        bad_results.append("The total case count test failed")
    # test 2, probably redundant but check under 1 vals
    if not df.loc[df.age_start < 5, sumcol].sum().round(3) == compare_df.loc[
        compare_df.age_start < 5, sumcol
    ].sum().round(3):
        bad_results.append("The under 5 case count test failed")

    # test 3, case counts grouped by sex, location, year, icd code
    grouper_cols = [
        "sex",
        "sex_id",
        "location_id",
        "year",
        "year_id",
        "year_start",
        "cause_code",
    ]
    groups = [c for c in grouper_cols if c in dfcols]
    print("grouping data by {}".format(groups))

    a = df.groupby(groups).agg({sumcol: "sum"}).reset_index()
    b = compare_df.groupby(groups).agg({sumcol: "sum"}).reset_index()

    if dfs_must_equal_eachother:
        print("Confirming that the grouped data pd.DataFrame.equals() eachother")
        if not a.equals(b):
            if not (
                a[groups].equals(b[groups])
                and np.isclose(a[sumcol], b[sumcol], atol=1e-05).all()
            ):
                bad_results.append(
                    "The groupby with {} summed by {} has failed".format(sumcol, groups)
                )
    else:
        print("Merging the data together and confirming that all rows merged and equal")
        m = a.merge(b, how="inner", on=groups, suffixes=("_df", "_compare_df"), validate="1:1")
        if len(m) != len(a) or len(m) != len(b):
            bad_results.append(
                "The merge didn't produce expected results. The row counts are different"
            )

        mismatch = m[m[f"{sumcol}_df"] != m[f"{sumcol}_compare_df"]]
        if len(mismatch) > 0:
            bad_results.append(f"There are {len(mismatch)} rows that don't equal each other")

    if bad_results:
        return bad_results
    else:
        return "test_case_counts has passed!!"


def legacy_stack_merger(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe, and determines the number of diagnosis variables present.
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

    if df.shape[0] != len(df.index.unique()):
        raise ValueError(
            "index is not unique, "
            "the index has a length of "
            f"{len(df.index.unique())}"
            " while the DataFrame has "
            f"{df.shape[0]}"
            " rows"
        )

    diagnosis_vars = df.columns[df.columns.str.startswith("dx_")]  # Find all.
    # columns with dx_ at the start
    diag_df = df[diagnosis_vars]  # select just the dx cols

    if diag_df[sorted(diagnosis_vars)[0]].isnull().sum() > 0:
        raise ValueError("All primary dx must be non-null.")

    # Stack diagnosis var subset using index of original df.
    stacked_df = diag_df.set_index(diag_df.index).stack().reset_index()
    # Rename stacked vars.
    stacked_df = stacked_df.rename(
        columns={"level_0": "patient_index", "level_1": "diagnosis_id", 0: "cause_code"}
    )

    # Replace diagnosis_id with codes, 1 for primary, 2 for secondary
    # and beyond.
    stacked_df["diagnosis_id"] = np.where(stacked_df["diagnosis_id"] == "dx_1", 1, 2)
    # merge stacked_df with the original df
    merged_df = stacked_df.merge(df, left_on="patient_index", right_index=True, how="outer")

    # Check if primary diagnosis value counts are the same
    if (
        diag_df[diagnosis_vars[0]].value_counts()
        != merged_df["cause_code"].loc[merged_df["diagnosis_id"] == 1].value_counts()
    ).any():
        raise ValueError("Primary Diagnoses counts are not the same before and after")
    # check if all primary diagnosis are present before and after
    if (
        diag_df[diagnosis_vars[0]].sort_values().values
        != merged_df[merged_df["diagnosis_id"] == 1]["cause_code"]
        .dropna()
        .sort_values()
        .values
        # Ignoring instead of casting to save compute.
    ).any():  # type:ignore[union-attr]
        raise ValueError("Not all Primary Diagnoses are present before and after")

    # check if counts of all secondary diagnoses are the same before and after
    old_second_total = diag_df[diagnosis_vars[1:]].apply(pd.Series.value_counts).sum(axis=1)
    new_second_total = (
        merged_df["cause_code"].loc[merged_df["diagnosis_id"] == 2].value_counts()
    )
    if (
        old_second_total.sort_index().values
        != new_second_total.sort_index().values
        # Ignoring instead of casting to save compute.
    ).any():  # type:ignore[union-attr]
        raise ValueError(
            "The counts of Secondary Diagnoses were not the same before and after"
        )
    # check if all secondary diagnoses are present before and after
    if (old_second_total.sort_index().index != new_second_total.sort_index().index).any():
        raise ValueError("Not all Secondary Diagnoses are present before and after")

    # drop all the diagnosis features, we don't need them anymore
    merged_df.drop(diagnosis_vars, axis=1, inplace=True)

    # remove missing cause codes this creates
    merged_df = merged_df[merged_df["cause_code"] != ""]

    return merged_df


def stack_merger(
    df: pd.DataFrame, diagnosis_cols: List[str] = [], reduce_2_dx: bool = True
) -> pd.DataFrame:
    """
    Reshapes a dataframe from wide to long by diagnoses, eg from dx_1, dx_2, ... dx_n
    to the team's standard long structure of cause_code and diagnosis_id.

    Args:
        df: Input clinical dataframe containing diagnosis codes stored wide.
        diagnosis_cols: Columns storing diagnoses codes. If not empty the
                        sort order of this list is very important as it is used to create the
                        values in the output diagnosis_id column.
                        Most importantly, the zeroth element will become the primary diagnosis.
        reduce_2_dx: Secondary diagnoses, like outpatient diagnoses are a "grab bag" and have
                     no inherent order even though they're numbered. This means it is often
                     acceptable to reduce non-primary diagnosis IDs to a single ID.
                     If True the output dataframe will have only 2 diagnosis_id values.
                     Primary diagnoses identified with the value 1 and non-primary
                     with the value 2.

    Raises:
        ValueError if the length of unique values in the df index does not match rows in df.
        ValueError if diagnosis_cols are not passed and dx_1 is not present in the data.
        ValueError if any primary diagnoses are null.

    Returns:
        df: Clinical data reshaped from wide format to long by diagnosis code.
    """

    if df.shape[0] != len(df.index.unique()):
        raise ValueError(
            "The index of df is not unique. This will cause problems "
            "with the reshape operation."
        )

    if not diagnosis_cols:
        diagnosis_cols = df.columns[df.columns.str.startswith("dx_")].tolist()
        natural_sort(diagnosis_cols)
        diagnosis_cols.reverse()
        if diagnosis_cols[0] != "dx_1":
            raise ValueError(
                "The default `dx_*` pattern is being used but the primary "
                "diagnosis is not identified as `dx_1`"
            )

    primary_dx_col = diagnosis_cols[0]
    if df[primary_dx_col].isnull().sum() > 0:
        raise ValueError("All primary dx must be non-null.")

    df = df.reset_index().rename(columns={"index": "event_id"})
    index_cols = [c for c in df.columns if c not in diagnosis_cols]

    df = df.set_index(index_cols).stack().reset_index()
    df = df.rename(columns={"level_3": "diagnosis_id", 0: "cause_code"})

    # Replace diagnosis_id with codes. 1 for primary, 2, 3, 4, etc for secondary.
    replace_dict = {dx_col: i + 1 for i, dx_col in enumerate(diagnosis_cols)}
    df["diagnosis_id"] = df["diagnosis_id"].map(replace_dict)
    if reduce_2_dx:
        # All non-primary diagnoses are grouped together into diagnosis_id 2
        df.loc[df["diagnosis_id"] > 2, "diagnosis_id"] = 2

    df = df.query("cause_code != ''")

    return df


def sanitize_diagnoses(col: pd.Series) -> pd.Series:
    """
    This function accepts one column of a DataFrame (a Series) and removes any
    non-alphanumeric character and casts to uppercase.
    """
    # "\W" regex represents ANY non-alphanumeric character
    col = col.str.replace(r"\W", "", regex=True)
    col = col.str.upper()

    return col


def fill_nid(df: pd.DataFrame, nid_dict: Dict[int, int]) -> pd.DataFrame:
    """
    Function that assigns the NID to each row based on the year. Accepts a
    dictionary that contains years as keys and nids as values, and a DataFrame.
    Returns DataFrame with a filled "nid" column. If the DataFrame didn't
    already have a "nid" column it makes one.

    raises:
        KeyError if the DataFrame does not have a column named 'year_start'
    """
    if "year_start" not in df.columns:
        raise KeyError("DataFrame doesn't have a'year_start' column")
    df["nid"] = df["year_start"].map(nid_dict)
    return df


def swap_ecode_with_ncode(df: pd.DataFrame, icd_vers: int = 10) -> pd.DataFrame:
    """
    Swap the ecodes and n codes from a dataset with multiple levels of
    diagnoses stored wide
    # dummy data example/test
    test = pd.DataFrame({'dx_1': ["S20", "T30", "V1", "S8888", "T3020", "T2"],
                         'dx_2': ['Y3', 'V33', 'S20', 'A30', "B4", "B5"],
                         'dx_3': ['W30', 'B20', 'C10', 'Y20', 'C6', 'C7'],
                         'dx_4': ['W30', 'B20', 'C10', 'Y20', 'C6', 'V7'],
                         'dx_5': ['W30', 'B20', 'C10', 'Y20', 'Y6', 'C7']})
    test = swap_ecode(test, 10)
    """
    if icd_vers not in [9, 10]:
        raise ValueError("That's not an acceptable ICD version")

    if icd_vers == 10:
        # Nature of injury starts with S or T
        # cause of injury starts with V, W, X, or Y. 
        ncodes = ["S", "T"]
        ecodes = ["V", "W", "X", "Y"]
    if icd_vers == 9:
        ncodes = ["8", "9"]
        ecodes = ["E"]

    # set the nature code conditions outside the loop cause it's always dx 1
    nature_cond = " | ".join(
        ["(df['dx_1'].str.startswith('{}'))".format(ncond) for ncond in ncodes]
    )
    # create list of non primary dx levels
    col_list = list(df.filter(regex="^(dx_)").columns.drop("dx_1"))
    # sort them from low to high
    natural_sort(col_list)
    col_list.reverse()
    for dxcol in col_list:
        if df[dxcol].isnull().all():
            continue
        # create the condition for the column we're looping over to find ecodes
        ecode_cond = " | ".join(
            ["(df['{}'].str.startswith('{}'))".format(dxcol, cond) for cond in ecodes]
        )
        # swap e and n codes
        df.loc[
            (df[dxcol].notnull()) & eval(nature_cond) & eval(ecode_cond), ["dx_1", dxcol]
        ] = df.loc[
            (df[dxcol].notnull()) & eval(nature_cond) & eval(ecode_cond), [dxcol, "dx_1"]
        ].values
    return df


def move_ecodes_into_primary_dx(df: pd.DataFrame, icd_version: int) -> pd.DataFrame:
    """
    Moves any Injury ICD codes from secondary diagnosis positions
    into primary diagnosis position in data that has diangoses
    stored wide.

    Ignores null values. Starts by swapping dx_3 into dx_1, then dx_2 into
    dx_1, and so on. Note that this is the opposite order than what the other
    function swap_ecode() does. For ICD10, looks for ICD codes that start with
    "V", "W", "X", and "Y". For ICD9 looks for ICD codes that start with "E".
    """

    if icd_version not in [9, 10]:
        raise ValueError("That's not an acceptable ICD version.")

    if "dx_1" not in df.columns:
        raise KeyError("There must be a column named 'dx_1'")

    start_cols = df.columns.tolist()

    if icd_version == 10:
        # cause of injury starts with V, W, X, or Y.
        ecodes = ["V", "W", "X", "Y"]
    if icd_version == 9:
        ecodes = ["E"]

    # don't need do swap dx_1 with dx_1
    col_list = list(df.filter(regex="^(dx_)").columns.drop("dx_1"))
    if len(col_list) == 0:
        raise ValueError("Column names don't use 'dx_' naming convention.")
    natural_sort(col_list)  # reverse natural order

    if len(col_list) == 0:
        raise ValueError("col_list needs to have elements.")

    for dxcol in col_list:
        if df[dxcol].isnull().all():
            continue
        ecode_cond = " | ".join(
            [f"(df['{dxcol}'].str.startswith('{ecode}'))" for ecode in ecodes]
        )
        df.loc[(df[dxcol].notnull()) & eval(ecode_cond), ["dx_1", dxcol]] = df.loc[
            (df[dxcol].notnull()) & eval(ecode_cond), [dxcol, "dx_1"]
        ].values

    if set(df.columns) != set(start_cols):
        raise ValueError("Columns were added or lost.")

    return df
