"""
Collection of functions that can remove live births

These are requirements on column naming and values:
    - code_system_id: 1 for ICD9, 2 for ICD10
    - diagosis columns must be named "dx_1", "dx_2", ... , "dx_{n}"

The user must know if live births are to be dropped, or swapped.
"""

import pandas as pd
import re

from clinical_info.Functions import hosp_prep


def get_live_birth_codes(user):
    """Returns two lists each containing ICD codes that identify
    some form of live birth. Pulls a list of live birth ICD 9 and
    10 codes from the clinical_info repo for the provided user.

    Returns
    icd9_live_birth_codes (list): ICD9 live births
    icd10_live_birth_codes (list) ICD10 live briths
    """

    base = (FILEPATH)
    df = pd.read_csv(base)

    icd9_live_birth_codes = df.loc[df['icd_vers'] == 'ICD9_detail', 'cause_code'].tolist()
    icd10_live_birth_codes = df.loc[df['icd_vers'] == 'ICD10', 'cause_code'].tolist()

    return icd9_live_birth_codes, icd10_live_birth_codes


def drop_live_births(df, user, verbose=True):
    """Drops live births from cause_code. Works on both ICD9 and ICD10.

    df (Pandas DataFrame): contains wide format ICD coded data
    """

    icd9_live_birth_codes, icd10_live_birth_codes = get_live_birth_codes(user)

    if verbose:
        pre = df.shape
        predx = df['val'].sum()
    assert set(df.code_system_id.unique()) <= {1, 2}

    icd10 = df.loc[df.code_system_id == 2, :]
    icd9 = df.loc[df.code_system_id == 1, :]

    icd10 = icd10.loc[~icd10.cause_code.isin(icd10_live_birth_codes), :]
    icd9 = icd9.loc[~icd9.cause_code.isin(icd9_live_birth_codes), :]

    df = pd.concat([icd9, icd10], sort=False, ignore_index=True)
    if verbose:
        post = df.shape
        postdx = df['val'].sum()
        print((f"We have removed {pre[0] - post[0]} rows from the data due to "
               f"live birth codes which corresponds to {predx - postdx} "
               f"primary and non-primary diagnoses"))

    return df

def review_dx_missingness(df, col_list):
    """Given a dataframe and a list of nonprimary column(s) we want to determine
    if null values are actually assigned to null, or to some other dummy value"""
    null_count = 0
    non_null_freq = []
    for col in col_list:
        null_count += df[col].isnull().sum()
        tops = df[col].value_counts(dropna=False).head(5).reset_index()
        tops.columns = ['cause_code', 'row_count']
        non_null_freq.append(tops)
    if null_count == 0:

    non_null_freq = pd.concat(non_null_freq, sort=False, ignore_index=True)
    non_null_freq.loc[non_null_freq.cause_code.isnull(), 'cause_code'] = 'realnan'
    non_null_freq = non_null_freq.groupby('cause_code').row_count.sum().reset_index().sort_values('row_count', ascending=False)
    if (non_null_freq.cause_code == 'realnan').sum() == 0:
        assert False, (f"There are no high frequency null codes, this means the data probably "
                       f"contains a non-null placeholder, review these codes to identify it. This "
                       f"swapping functions assumes null values\n\n{non_null_freq}")
    else:
        pass
    return

def swap_live_births(df, drop_if_primary_still_live, user,
                     verbose=True, drop_null_primary=False):

    icd9_live_birth_codes, icd10_live_birth_codes = get_live_birth_codes(user)

    if drop_null_primary:
        pre = df.shape
        df = df.loc[df.dx_1.notnull(), :]
        post = df.shape
        diff = pre[0] - post[0]
        if diff:
            print(f"Dropping null primary dx lost {diff} rows")

    assert set(df.code_system_id.unique()) <= {1, 2}

    # backup df for testing
    preswap = df[df.filter(regex="dx_|code_system_id").columns].copy()

    icd10 = df.loc[df.code_system_id == 2, :].copy()
    icd9 = df.loc[df.code_system_id == 1, :].copy()

    col_list = list(df.filter(regex="^(dx_)").columns.drop("dx_1"))
    assert len(col_list) > 0, "Column names don't use 'dx_' naming convention."
    hosp_prep.natural_sort(col_list)  # natural order
    col_list.reverse()
    assert len(col_list) > 0, "col_list needs to have elements."
    start_dx = int(re.sub("[^\d]", "", col_list[0]))
    end_dx = int(re.sub("[^\d]", "", col_list[len(col_list) - 1]))
    assert start_dx <= end_dx, "The 'col_list' order is not naturally sorted"

    review_dx_missingness(df, col_list)

    for col in col_list:

        icd10_cond = ((icd10.dx_1.isin(icd10_live_birth_codes))
                      & (~icd10[col].isin(icd10_live_birth_codes))
                      & (icd10[col].notnull()))
        icd9_cond = ((icd9.dx_1.isin(icd9_live_birth_codes))
                     & (~icd9[col].isin(icd9_live_birth_codes))
                     & (icd9[col].notnull()))

        reg_icd10_cond = ((icd10.dx_1.astype(str).str.contains("^Z38"))
                          & (~icd10[col].astype(str).str.contains("^Z38"))
                          & (icd10[col].notnull()))
        reg_icd9_cond = ((icd9.dx_1.astype(str).str.contains("^V3[0-9]"))
                         & (~icd9[col].astype(str).str.contains("^V3[0-9]"))
                         & (icd9[col].notnull()))

        icd9_swapsize = len(icd9[icd9_cond])
        icd10_swapsize = len(icd10[icd10_cond])
        reg_icd9_swapsize = len(icd9[reg_icd9_cond])
        reg_icd10_swapsize = len(icd10[reg_icd10_cond])

        if verbose:
            msg = (f"For column {col} the live birth swapping code will swap "
                   f"{icd9_swapsize} ICD 9 rows and {icd10_swapsize} ICD 10 rows. "
                   f"regex would have swapped {reg_icd9_swapsize} and "
                   f"{reg_icd10_swapsize} respectively")
            print(msg)

        icd10.loc[icd10_cond, ['dx_1', col]] =\
            icd10.loc[icd10_cond, [col, 'dx_1']].values
        icd9.loc[icd9_cond, ['dx_1', col]] =\
            icd9.loc[icd9_cond, [col, 'dx_1']].values

    if drop_if_primary_still_live:

        icd10 = icd10.loc[~icd10.dx_1.isin(icd10_live_birth_codes), :]
        icd9 = icd9.loc[~icd9.dx_1.isin(icd9_live_birth_codes), :]

    df = pd.concat([icd10, icd9], sort=False, ignore_index=True)
    test_swapped_data(df, preswap)

    return df

def make_val_counts(df, dx_cols):
    val_list = []
    for col in dx_cols:
        tmp = df[col].value_counts(dropna=False).reset_index()
        tmp.columns = ['cause_code', 'rows']
        tmp.loc[tmp.cause_code.isnull(), 'cause_code'] = 'arealnullval'
        val_list.append(tmp)
    val_counts = pd.concat(val_list, sort=False, ignore_index=True)
    val_counts = val_counts.groupby('cause_code').rows.sum().reset_index()
    return val_counts

def dx_test(df1, df2):
    df1_dx = df1.filter(regex='dx_').columns.tolist()
    df2_dx = df2.filter(regex='dx_').columns.tolist()
    assert not set(df1_dx).symmetric_difference(df2_dx)
    val1 = make_val_counts(df1, df1_dx)
    val2 = make_val_counts(df2, df2_dx)

    val_test = val1.merge(val2, how='outer', on='cause_code', validate='1:1')
    return val_test

def test_swapped_data(df, preswap):

    failures = []
    if df.shape[0] != preswap.shape[0]:
        failures.append("The row counts have changed")
    if df[df.filter(regex="dx_|code_system_id").columns].shape[1] != preswap.shape[1]:
        failures.append("The column counts have changed")

    val_test = dx_test(df, preswap)
    val_test = val_test.query("rows_x != rows_y")
    if len(val_test) > 0:
        failures.append(f"The swap has actually changed dx counts {val_test}")
    if failures:
        raise ValueError("\n".join(failures))

    return
