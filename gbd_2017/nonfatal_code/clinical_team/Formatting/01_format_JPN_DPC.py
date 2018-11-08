# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import platform
import numpy as np
import sys
import glob
import getpass
import warnings


# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH/Functions".format(user)
sys.path.append(prep_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = "FILEPATH/DPC2010-2015 tabulations/"
files = glob.glob(filepath + "*tabulations.dta")

df_list = []
for in_file in files:
    df = pd.read_stata(in_file)
    df['year'] = in_file.split('/')[-1].split('_')[0].split('C')[1]
    df_list.append(df)
df = pd.concat(df_list)

# If this assert fails uncomment this line:
df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# Select features from raw data to keep
keep = df.columns.tolist()
df = df[keep]

df['year'] = pd.to_numeric(df['year'], downcast='integer')
# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    # 'location_id': 'location_id',
    'representative_id': 'representative_id',
    #'year': 'year',
    'year': 'year_start',
    'year_end': 'year_end',
    'female': 'sex_id',
    'age_groups': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'diagnosis_1': 'dx_1',
    'count': 'val'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)
df['year_end'] = df['year_start']

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

df['representative_id'] = 3

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1

df['age_group_unit']

df['source'] = 'JPN_DPC'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# case is the sum of live discharges and deaths
df['outcome_id'] = df['outcome'].str.lower()
df.drop('outcome', 1)

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df['facility_id'] = 'hospital'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2015: 336200, 2014: 336199, 2013: 336198, 2012: 336197,
                2011: 336195, 2010: 336193}

# df = fill_nid(df, nid_dictionary)
df['nid'] = df['year_start'].map(nid_dictionary)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# Mapping Japan prefecutres to GBD location_ids
path_map = 'FILEPATH'
loc_map = pd.read_csv(path_map + 'jpn_prefecture_to_location_id_map.csv')
df = df.merge(loc_map, how='left', on='prefecture_id')
cols = ['location_name', 'prefecture_id']
df = df.drop(cols, 1)

# Convert sex_id to standard format
df['sex_id'].replace([0, 1], [1, 2], inplace = True)

# Case cast the cause_code
df['dx_1'] = df['dx_1'].str.upper()

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', # 'age',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']

# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ['source', 'facility_id', 'outcome_id']

if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.

df['age'] = df['age'].apply(lambda x: 0 if x.endswith('days') else x.split('_')[1])
df.loc[df['age'] == '95+years', 'age'] = 95
df['age'] = df['age'].apply(pd.to_numeric)

df = age_binning(df)
df.drop('age', 1)

# drop unknown sex_id
df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

# Create year range if the data covers multiple years
#df = year_range(df)

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
columns_before = df_agg.columns
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source', 'nid',
                   'facility_id',
                   'code_system_id', 'cause_code']
df_agg = df_agg[hosp_frmat_feat]
columns_after = df_agg.columns

# check if all columns are there
assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"

# check sex_id col for acceptable values
good_ids = [1, 2, 3]
good_in_data = [n for n in good_ids if n in df_agg.sex_id.unique()]
assert set(df_agg['sex_id'].unique()).\
    symmetric_difference(set(good_in_data)) == set(),\
    "There are unexpected sex_id values {} in the df".\
    format(df_agg.sex_id.unique())


assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_JPN_DPC.H5"
write_hosp_file(df_agg, write_path, backup=True)