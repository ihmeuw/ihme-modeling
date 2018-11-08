# -*- coding: utf-8 -*-

import platform
import pandas as pd
import getpass
import sys

import hosp_prep

# print warning message about latest version of maps:
print("=====================================")
print("=== PLEASE MAKE SURE YOU ARE USING===")
print("==== LATEST VERSION OF CLEAN MAP ====")
print("=====================================")

# read in clean maps
# TODO: change this to real clean maps when time comes clean_map_9
df = pd.read_csv(root + r"FILEPATH")

map_vers = int(df.map_version.unique())
assert hosp_prep.verify_current_map(df)

df = df[['cause_code', 'bundle_id', 'code_system_id', 'bid_measure', 'level']]

# select only ICD9 parts
df = df[df['code_system_id'] == 1]

# get rid of null values
df.dropna(axis=0, inplace=True)

# make icd codes strings
df['cause_code'] = df['cause_code'].astype(str)

# drop code sys id column
df.drop(['code_system_id', 'level'], axis=1, inplace=True)

# drop duplicates
df.drop_duplicates(inplace=True)

# rename to match goal file.
df.rename(columns={'cause_code': 'icd_code', 'bid_measure': 'measure'}, inplace=True)

# save
df.to_stata(root + r"FILEPATH",
            write_index=False)
