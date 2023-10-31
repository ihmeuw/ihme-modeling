# coding: utf-8
"""
The data for Brazil is written in hundreds of regional DBF files which take a substantial
amount of time to load into Python, at least a few hours. This script reads all those
small files in, drops the columns we won't be using and saves an HDF file which can be
read into Python in seconds. The output of this file will feed directly into 01_format_BRA_SIH_15_16.py
"""

import pandas as pd
from simpledbf import Dbf5
import glob
import os

from clinical_info.Functions import hosp_prep


# get a list of all the 2016 files
fdir = "FILENAME"
files = glob.glob(fdir + "*.dbf")

# set directory to write to
out_dir = "FILENAME"


def get_cols_to_keep(file_path):
    """
    read in a single one of these tiny dbf files to get a list of all the column names we need to keep
    """
    dbf = Dbf5(file_path, codec="latin")
    df = dbf.to_dataframe()
    second = df.head().filter(regex="DIAGSEC").columns.tolist()
    to_keep = [
        "ANO_CMPT",
        "MUNIC_RES",
        "DT_INTER",
        "DIAG_PRINC",
        "DIAG_SECUN",
        "IDADE",
        "COD_IDADE",
        "SEXO",
        "MORTE",
        "DT_INTER",
        "DT_SAIDA",
    ] + second
    return to_keep


to_keep = get_cols_to_keep(files[0])

# loop over the 2016 files names and store them all in a list
df_list16 = []
for f in files:
    print(f)
    dbf = Dbf5(f, codec="latin")
    df = dbf.to_dataframe()
    print(df.shape)
    assert df.shape[1] == 113
    fname = os.path.basename(f)[:-4] + ".dta"
    df.to_stata(out_dir + fname, write_index=False)
    # print(df.sample(3))
    df = df[to_keep]
    df_list16.append(df)

# do the same thing for 2015 data
fdir = "FILENAME"
files = glob.glob(fdir + "*.dbf")
out_dir = "FILENAME"
df_list15 = []
for f in files:
    print(f)
    dbf = Dbf5(f, codec="latin")
    df = dbf.to_dataframe()
    print(df.shape)
    assert df.shape[1] == 113
    fname = os.path.basename(f)[:-4] + ".dta"
    df.to_stata(out_dir + fname, write_index=False)
    # print(df.sample(3))
    df = df[to_keep]
    df_list15.append(df)

# concat the 15 and 16 data into a df
df = pd.concat(df_list15 + df_list16, ignore_index=True)

# some int cols are read in as str, convert to numeric
for col in ["ANO_CMPT", "MUNIC_RES", "COD_IDADE", "SEXO"]:
    df[col] = pd.to_numeric(df[col], errors="raise")


def fix_datetime_cols(df):
    # there are two DT_INTER cols, make sure they are identical and keep 1
    assert (df.DT_INTER.iloc[:, 1] == df.DT_INTER.iloc[:, 0]).all()
    x = pd.to_datetime(df.DT_INTER.iloc[:, 1])
    df.drop("DT_INTER", axis=1, inplace=True)
    df["DT_INTER"] = x
    # convert discharge date to datetime object
    df["DT_SAIDA"] = pd.to_datetime(df["DT_SAIDA"])
    return df


df = fix_datetime_cols(df)


def drop_null_cols(df):
    # drop diagnosis columns that are entirely null
    print(df.shape)
    for col in df.columns:
        if df[col].isnull().sum() == df.shape[0]:
            df.drop(col, axis=1, inplace=True)
    print(df.shape)
    return df


df = drop_null_cols(df)
df.info(memory_usage="deep")

# convert diagnosis columns to str
cols = df.filter(regex="DIAG").columns
for col in cols:
    df[col] = df[col].astype(str)

# write to J:
hosp_prep.write_hosp_file(
    df=df, write_path="FILENAME",
)
