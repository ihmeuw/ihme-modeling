# -*- coding: utf-8 -*-
"""
Create a condensed version of our clean map for use with collaborators

Important note, if we do not include ALL ICD codes the cause fractions will
be incomplete and unusable with the envelope

"""
import pandas as pd
import platform
import getpass
from db_tools.ezfuncs import query

if platform.system() == "Linux":
    j = "FILEPATH/j"
    h = "FILEPAHT/{}".format(getpass.getuser())

else:
    j = "J:"
    h = "H:"


def clean_reader():
    df = pd.read_csv("FILEPATH/clean_map.csv")
    return df


def select_cols(df, cols=['cause_code', 'code_system_id', 'bundle_id',
                          'bid_measure', 'bid_duration', 'map_version']):
    df = df[cols].copy()
    return df


def select_rows(df):
    """
    keep only the first duplicate of any row across all columns
    """
    df.drop_duplicates(keep='first', inplace=True)
    return df


def remove_null_bundles(df):
    """
    remove rows with missing bundle id
    """
    df = df[df.bundle_id.notnull()].copy()
    df.reset_index(drop=True, inplace=True)
    return df


def fill_prev_durations(df):
    """
    prev has a de facto 365 day duration, fill that here
    """
    df.loc[df.bid_measure == 'prev', 'bid_duration'] = 365
    assert (df.isnull().sum() == 0).all(),\
        "review the null values \n\n\n{}".format(\
            df[df.isnull().any(axis=1)])
    return df


def get_bundle_name(df):
    """
    
    """
    bnames = query("QUERY")

    pre = df.shape[0]
    df = df.merge(bnames, how='left', on=['bundle_id'])
    assert df.shape[0] == pre, "The merge added or removed rows from df, ending program"

    assert df.bundle_name.isnull().sum() == 0, "There are missing bundle names, why"

    return df


def get_proc_reqs(df):
    reqs = pd.read_csv("FILEPATH/reqs.csv")[['bundle_id', 'adj_ms_prev_otp']]

    pre = df.shape[0]
    df = df.merge(reqs, how='left', on=['bundle_id'])
    assert df.shape[0] == pre, "The merge added or removed rows from df, ending program"
    assert df.adj_ms_prev_otp.isnull().sum() == 0, "some adjustment params are missing"
    df.rename(columns={'adj_ms_prev_otp': 'require_2_otp_claims'}, inplace=True)

    return df


def write_to_excel(df):
    assert (df.loc[df.bid_measure == 'prev', 'bid_duration'] == 365).all()
    fpath="FILEPATH/condensed_clean_map_{}.xlsx".format(
                   h, df.iloc[0]['map_version'])
    writer = pd.ExcelWriter(fpath, engine='xlsxwriter')
    df.to_excel(writer, index=False)
    print(r"Now writing to {}".format(fpath))
    writer.save()


if __name__ == "__main__":
    df = clean_reader()
    df = select_cols(df)
    df = select_rows(df)
    df = remove_null_bundles(df)
    df = fill_prev_durations(df)
    df = get_bundle_name(df)
    df = get_proc_reqs(df)

    write_to_excel(df)
