"""

Collection of functions that can remove live births.

These are requirements on column naming and values:
    - code_system_id: 1 for ICD9, 2 for ICD10
    - diagosis columns must be named "dx_1", "dx_2", ... , "dx_{n}"

The user must know if live births are to be dropped or swapped.
"""

import logging
import re
from typing import Final, List, Set, Tuple

import pandas as pd
from crosscutting_functions.mapping.mapping import LIVE_BIRTH_CODES

from crosscutting_functions import general_purpose


def drop_live_births(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Drops live births from `cause_code`. Works on both ICD9 and ICD10.

    Arguments:
        df: Wide-format ICD-coded data.
        verbose: If True, prints out how many rows were dropped.

    Returns:
        df: Wide-format ICD-coded data with live births removed.
    """
    _validate_code_system_id(df=df)

    icd9_live_birth_codes, icd10_live_birth_codes = get_live_birth_codes()

    if verbose:
        pre = df.shape
        predx = df["val"].sum()

    icd10 = df.loc[df.code_system_id == 2, :]
    icd9 = df.loc[df.code_system_id == 1, :]

    icd10 = icd10.loc[~icd10.cause_code.isin(icd10_live_birth_codes), :]
    icd9 = icd9.loc[~icd9.cause_code.isin(icd9_live_birth_codes), :]

    df = pd.concat([icd9, icd10], sort=False, ignore_index=True)
    if verbose:
        post = df.shape
        postdx = df["val"].sum()
        msg = (
            f"We have removed {pre[0] - post[0]} rows from the data due to live-birth"
            f" codes which correspond to {predx - postdx} primary and non-primary"
            " diagnoses."
        )
        logging.info(msg)

    return df


def swap_live_births(
    df: pd.DataFrame,
    drop_if_primary_still_live: bool,
    verbose: bool = True,
    drop_null_primary: bool = False,
    swap_method: str = "csv_list",
) -> pd.DataFrame:
    """Moves live births out of non-primary diagnosis position into primary position.

    Option included to drop any remaining live births codes after swapping procedure is
    complete.

    Arguments:
        df: ICD-coded data.
        drop_if_primary_still_live: Very important argument. If True, live births will be
            dropped after the swapping procedure. If False, there's a possiblity that live
            births will remain in the primary diagnosis position after this function is run.
        verbose: If True, prints out how many rows were swapped.
        drop_null_primary: If True, drops rows where the primary diagnosis is null.
        swap_method: Method to determine how live-birth codes should be identified. Options
            are "csv_list" and "regex".

    Returns:
        df: Wide-format ICD-coded data with live births swapped.
    """
    icd9_live_birth_codes, icd10_live_birth_codes = get_live_birth_codes()

    if drop_null_primary:
        pre = df.shape
        df = df.loc[df.dx_1.notnull(), :]
        post = df.shape
        diff = pre[0] - post[0]
        if diff:
            msg = f"Dropping null primary dx. Lost {diff} rows."
            logging.info(msg)

    _validate_code_system_id(df=df)

    # Backup df for testing.
    preswap = df[df.filter(regex="dx_|code_system_id").columns].copy()

    icd10 = df.loc[df.code_system_id == 2, :].copy()
    icd9 = df.loc[df.code_system_id == 1, :].copy()

    col_list = _get_dx_cols(df=df)

    review_dx_missingness(df=df, col_list=col_list)

    for col in col_list:
        # Let "non-primary dx" be diagnosis columns that are not the primary
        # diagnosis, ordered from highest (e.g. dx_15) to lowest (e.g. dx_2).
        # These masks select rows where:
        # 1. `dx_1` is a live-birth code.
        # 2. Non-primary dx is not a live birth.
        # 3. Non-primary dx is not null. (Assumes empty strings have been changed to Null.)
        icd9_cond, icd10_cond = create_condition_by_method(swap_method=swap_method)

        icd9_swapsize = len(icd9[eval(icd9_cond)])
        icd10_swapsize = len(icd10[eval(icd10_cond)])

        if verbose:
            msg = (
                f"For column `{col}`, the live-birth swapping code will swap "
                f"{icd9_swapsize} ICD-9 rows and {icd10_swapsize} ICD-10 rows."
            )
            logging.info(msg)

        icd10.loc[eval(icd10_cond), ["dx_1", col]] = icd10.loc[
            eval(icd10_cond), [col, "dx_1"]
        ].values
        icd9.loc[eval(icd9_cond), ["dx_1", col]] = icd9.loc[
            eval(icd9_cond), [col, "dx_1"]
        ].values

    if drop_if_primary_still_live:
        # An extra step to make sure there are no live births in dx after swapping.
        if swap_method == "regex":
            icd10 = icd10.loc[~icd10.dx_1.astype(str).str.contains("^Z38"), :]
            icd9 = icd9.loc[~icd9.dx_1.astype(str).str.contains("^V3[0-9]"), :]
        elif swap_method == "csv_list":
            icd10 = icd10.loc[~icd10.dx_1.isin(icd10_live_birth_codes), :]
            icd9 = icd9.loc[~icd9.dx_1.isin(icd9_live_birth_codes), :]
        
    df = pd.concat([icd10, icd9], sort=False, ignore_index=True)
    validate_swapped_data(
        df=df, preswap=preswap, drop_if_primary_still_live=drop_if_primary_still_live
    )

    return df


def get_live_birth_codes() -> Tuple[List[str], List[str]]:
    """Returns two lists, each containing ICD codes that identify some form of live birth.

    Pulls a list of live-birth ICD-9 and -10 codes for the provided user.

    Returns:
        icd9_live_birth_codes: ICD-9 live births.
        icd10_live_birth_codes: ICD-10 live births.
    """
    icd9_live_birth_codes = LIVE_BIRTH_CODES.loc[
        LIVE_BIRTH_CODES["icd_vers"] == "ICD9_detail", "cause_code"
    ].tolist()
    icd10_live_birth_codes = LIVE_BIRTH_CODES.loc[
        LIVE_BIRTH_CODES["icd_vers"] == "ICD10", "cause_code"
    ].tolist()

    return icd9_live_birth_codes, icd10_live_birth_codes


def review_dx_missingness(df: pd.DataFrame, col_list: List[str]) -> None:
    """Determine if null values in non-primary columns are None or another dummy value.

    Arguments:
        df: Wide-format ICD-coded data.
        col_list: List of non-primary diagnosis columns.

    Raises:
        ValueError: If there are no high-frequency null codes.
    """
    null_count = 0
    non_null_freq = []
    for col in col_list:
        null_count += df[col].isnull().sum()
        tops = df[col].value_counts(dropna=False).head(5).reset_index()
        tops.columns = pd.core.indexes.base.Index(["cause_code", "row_count"])
        non_null_freq.append(tops)
    if null_count == 0:
        msg = "There are zero nulls in non-primary columns. This is nearly impossible."
        logging.warning(msg)
    non_null_freq_df = pd.concat(non_null_freq, sort=False, ignore_index=True)
    non_null_freq_df.loc[non_null_freq_df.cause_code.isnull(), "cause_code"] = "realnan"
    non_null_freq_df = (
        non_null_freq_df.groupby("cause_code")
        .row_count.sum()
        .reset_index()
        .sort_values("row_count", ascending=False)
    )
    if (non_null_freq_df.cause_code == "realnan").sum() == 0:
        raise ValueError(
            "There are no high-frequency null codes. This means the data probably contains"
            " a non-null placeholder. Review these codes to identify it. This swapping"
            f" function assumes null values.\n\n{non_null_freq_df}"
        )


def create_condition_by_method(swap_method: str) -> Tuple[str, str]:
    """Create a condition to identify live births.

    There are multiple ways to find live-birth codes. The current support methods are to
    identify them via a list stored as a CSV in the repo or via a regular expression.

    Arguments:
        swap_method: Method to determine how live-birth codes should be identified. Options
            are "csv_list" and "regex".

    Raises:
        NotImplementedError: If the swap_method is not supported.

    Returns:
        icd9_cond: Condition to identify live births in ICD-9.
        icd10_cond: Condition to identify live births in ICD-10.
    """
    if swap_method == "csv_list":
        icd10_cond = (
            "((icd10.dx_1.isin(icd10_live_birth_codes))"
            "& (~icd10[col].isin(icd10_live_birth_codes))"
            "& (icd10[col].notnull()))"
        )
        icd9_cond = (
            "((icd9.dx_1.isin(icd9_live_birth_codes))"
            "& (~icd9[col].isin(icd9_live_birth_codes))"
            "& (icd9[col].notnull()))"
        )
    elif swap_method == "regex":
        icd10_cond = (
            "((icd10.dx_1.astype(str).str.contains('^Z38'))"
            "& (~icd10[col].astype(str).str.contains('^Z38'))"
            "& (icd10[col].notnull()))"
        )
        icd9_cond = (
            "((icd9.dx_1.astype(str).str.contains('^V3[0-9]'))"
            "& (~icd9[col].astype(str).str.contains('^V3[0-9]'))"
            "& (icd9[col].notnull()))"
        )
    else:
        raise NotImplementedError(f"Unable to swap using method: '{swap_method}'.")

    return icd9_cond, icd10_cond


def validate_swapped_data(
    df: pd.DataFrame, preswap: pd.DataFrame, drop_if_primary_still_live: bool
) -> None:
    """Validate if the swapping procedure has changed the data in unexpected ways.

    Arguments:
        df: ICD-coded data after swapping procedure.
        preswap: ICD-coded data before swapping procedure.
        drop_if_primary_still_live: Very important argument. If True, live births were
            dropped after the swapping procedure. If False, there's a possiblity that live
            births remain in the primary diagnosis position.

    Raises:
        ValueError: If the swapping procedure has changed the data in unexpected ways.
    """
    failures = []
    if df[df.filter(regex="dx_|code_system_id").columns].shape[1] != preswap.shape[1]:
        failures.append("The column counts have changed.")

    if not drop_if_primary_still_live:
        if df.shape[0] != preswap.shape[0]:
            failures.append("The row counts have changed.")

        val_test = dx_validate(df1=df, df2=preswap)
        val_test = val_test.query("rows_x != rows_y")
        if len(val_test) > 0:
            failures.append(f"The swap has actually changed dx counts:\n\n{val_test}")
    if failures:
        raise ValueError("\n".join(failures))

    return


def dx_validate(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """Validate that the dx columns have changed.

    Arguments:
        df1: ICD-coded data after swapping procedure.
        df2: ICD-coded data before swapping procedure.

    Raises:
        ValueError: If diagnosis columns have changed (internal validation).
        MergeError: If value counts have multiple rows for the same cause code.

    Returns:
        val_test: Value counts for each diagnosis column.
    """
    df1_dx = df1.filter(regex="dx_").columns.tolist()
    df2_dx = df2.filter(regex="dx_").columns.tolist()
    sym_diff = list(set(df1_dx).symmetric_difference(df2_dx))
    sym_diff.sort()
    if sym_diff:
        raise ValueError(f"Diagnosis columns have changed: {sym_diff}")
    val1 = make_val_counts(df=df1, dx_cols=df1_dx)
    val2 = make_val_counts(df=df2, dx_cols=df2_dx)

    val_test = val1.merge(val2, how="outer", on="cause_code", validate="1:1")
    return val_test


def make_val_counts(df: pd.DataFrame, dx_cols: List[str]) -> pd.DataFrame:
    """Create a value count table for the dx columns.

    Arguments:
        df: ICD-coded data.
        dx_cols: List of diagnosis columns.

    Returns:
        val_counts: Value counts for each diagnosis column.
    """
    val_list = []
    for col in dx_cols:
        tmp = df[col].value_counts(dropna=False).reset_index()
        tmp.columns = pd.core.indexes.base.Index(["cause_code", "rows"])
        tmp.loc[tmp.cause_code.isnull(), "cause_code"] = "arealnullval"
        val_list.append(tmp)
    val_counts = pd.concat(val_list, sort=False, ignore_index=True)
    val_counts = val_counts.groupby("cause_code").rows.sum().reset_index()
    return val_counts


def _validate_code_system_id(df: pd.DataFrame) -> None:
    """Validate the code_system_id column.

    Arguments:
        df: ICD-coded data.

    Raises:
        ValueError: If the code_system_id column has unexpected values.
    """
    code_system_id_val_set: Final[Set[int]] = {1, 2}
    if not set(df.code_system_id.unique()) <= code_system_id_val_set:
        raise ValueError(
            f"The `code_system_id` column has unexpected values. "
            f"Expected: {code_system_id_val_set}. "
            f"Found: {set(df.code_system_id.unique())}."
        )


def _get_dx_cols(df: pd.DataFrame) -> List[str]:
    """Get the diagnosis columns (starting with "dx_"), except for 'dx_1'.

    Arguments:
        df: ICD-coded data.

    Raises:
        ValueError: If `df` doesn't have columns with 'dx_' prefixes, beyond 'dx_1'.
        ValueError: If `df` doesn't have columns with 'dx_' prefixes, beyond 'dx_1'
            after sorting (internal validation).
        ValueError: If diagnosis columns are not in ascending order (internal validation).

    Returns:
        col_list: Diagnosis columns (starting with "dx_"), except for 'dx_1'.
    """
    col_list = list(df.filter(regex="^(dx_)").columns.drop("dx_1"))
    if not len(col_list) > 0:
        raise ValueError(
            (
                "`df` needs to have columns with 'dx_' prefixes, beyond 'dx_1'."
                f" Got: {df.columns.to_list()}"
            )
        )

    pre_sort = col_list.copy()
    general_purpose.natural_sort(lst=col_list)
    col_list.reverse()

    if not len(col_list) > 0:
        raise ValueError(f"Natural sort dropped dx cols. Were: {pre_sort}. Now: {col_list}")

    start_dx = int(re.sub(r"[^\d]", "", col_list[0]))
    end_dx = int(re.sub(r"[^\d]", "", col_list[len(col_list) - 1]))
    if not start_dx <= end_dx:
        raise ValueError("Diagnosis columns are not in ascending order.")

    return col_list