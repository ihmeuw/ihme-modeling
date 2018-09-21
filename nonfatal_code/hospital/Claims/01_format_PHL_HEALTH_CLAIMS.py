
# coding: utf-8
"""
Created on Mon Jan 23 17:32:28 2017

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.
"""
import pandas as pd
import platform
import numpy as np
import sys
import time

# load our functions
hosp_path = "FILEPATH"
sys.path.append(hosp_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
df = pd.read_hdf(root + "FILEPATH", key='df')

# If this assert fails uncomment this line:
#df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# rename claims cols
df.rename(columns={'NEWPIN': 'enroUSER_id', 'ICDCODES': 'cause_code', 'PATAGE': 'age',
                   'PATSEX': 'sex_id', 'OPD_TST': 'is_otp', 'DATE_ADM': 'adm_date',
                   'DATE_DIS': 'dis_date', 'CLASS_DEF': 'facility_id', 'DATE_OF_DEATH': 'date_of_death'}, inplace=True)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

# These are completely dependent on data source
# df['representative_id'] = 0
df['location_id'] = 16

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'PHIL_HEALTH_CLAIMS'

df['year_start'] = 0
df.loc[df.adm_date < "2014-01-01", 'year_start'] = 2013
df.loc[df.adm_date >= "2014-01-01", 'year_start'] = 2014
df['year_end'] = df['year_start']

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2 # codebook says icd 10

# duplicate date of death col to turn it into outcome ID
df['outcome_id'] = df['date_of_death']
df.loc[df.outcome_id.notnull(), 'outcome_id'] = 'death'
df.loc[df.outcome_id.isnull(), 'outcome_id'] = 'discharge'

# drop missing sex IDS then convert to our code system
df = df.query("sex_id == 'M' | sex_id == 'F'")
df['sex_id'].replace(['F', 'M'], [2, 1], inplace=True)

#####################################################
# CLEAN VARIABLES
#####################################################

# remove age values below 0 and above 150
df = df[(df.age >= 0) & (df.age < 150)]

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age', 'sex_id']
            #'age_start', 'age_end', 'sex_id', 'nid']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

# drop unknown sex_id
df = df.query("sex_id == 1 | sex_id == 2")

# split cause code col with multi dx into a dataframe where each col is a dx
dx_df = df['cause_code'].str.split(",", expand=True)
nums = np.arange(1, dx_df.shape[1] + 1, 1)
names = ["dx_" + str(num) for num in nums]
dx_df.columns = names

# cbind back together
df.drop('cause_code', axis=1, inplace=True)
df = pd.concat([df, dx_df], axis=1)

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])
    df[feat] = df[feat].astype(str)

str_feats = ['enroUSER_id', 'is_otp', 'facility_id', 'date_of_death', 'source', 'outcome_id']
for feat in str_feats:
    df[feat] = df[feat].astype(str)

# write to file
df.to_hdf(root + "FILEPATH", key='df',
          format='table', data_columns=['enroUSER_id']
          complib='blosc', complevel=5)
