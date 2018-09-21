# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017

Format Nepal data including reshaping from wide format to long
"""
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
hosp_path = "FILEPATH"
sys.path.append(hosp_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


    # read in each spreadsheet
fpath = root + "FILEPATH"
df10 = pd.read_excel(fpath,
                     sheetname='2010-11')
df11 = pd.read_excel(fpath,
                     sheetname='2011-12')
df13 = pd.read_excel(fpath,
                     sheetname='2013-14')


# age groups the data was provided in
age_groups = ['<28 days', '29 days - 1', '1 4', '5 14', '15 19', '20 29',
              '30 39', '40 49', '50 59', '60 ']
# append male ond female onto age groups
# in the correct order, female is always first
# order of these items is very important
age_sex_groups = []
for age in age_groups:
    fem = age + " female"
    ma = age + " male"
    age_sex_groups.append(fem)
    age_sex_groups.append(ma)


def name_cols(df):
    # keep just the first 21 columns. These contain counts of cases
    df = df.iloc[:, 0:22]
    # name the cols using list created above
    df.columns = ['cause_code', 'icd_name'] + age_sex_groups
    return(df)
# add correct column names
df10 = name_cols(df10)
df11 = name_cols(df11)
df13 = name_cols(df13)

# skip the first n rows which contain totals and col headers
df10 = df10.iloc[5:, :]
df11 = df11.iloc[3:, :]
df13 = df13.iloc[5:, :]

# add years as strings
df10['year_start'] = 2010
df11['year_start'] = 2011
df13['year_start'] = 2013


def reshape_long(df):
    # convert what will be 'val' column of counts to numeric
    for col in age_sex_groups:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # combine counts for under age 1
    df['0 1 male'] = df['<28 days male'].fillna(0) + df['29 days - 1 male'].fillna(0)
    df['0 1 female'] = df['<28 days female'].fillna(0) + df['29 days - 1 female'].fillna(0)
    df.drop(labels=['<28 days female', '<28 days male', '29 days - 1 female',
                    '29 days - 1 male'], axis=1, inplace=True)

    # reshape long to create age start, age end, sex and val columns
    df = df.set_index(['cause_code', 'icd_name', 'year_start']).stack().\
        reset_index()
    df.rename(columns={'level_3': 'age_sex', 0: 'val'}, inplace=True)
    # turn column of age start, age end, sex values into separate columns
    splits = df.age_sex.str.split(" ", expand=True)
    splits.columns = ['age_start', 'age_end', 'sex_id']
    # append back onto data
    df = pd.concat([df, splits], axis=1)
    # clean values
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    df.age_end.replace([""], ["99"], inplace=True)
    # drop unneeded columns
    df.drop(['age_sex', 'icd_name'], axis=1, inplace=True)
    # year end col is year start + 1
    df['year_end'] = df['year_start'] + 1
    return(df)

df_list = []
for df in [df10, df11, df13]:
    df_list.append(reshape_long(df))

df = pd.concat(df_list)

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
df.reset_index(drop=True, inplace=True)

# lose 0 values
df = df[df.val != 0]

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    # 'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    # 'age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'cause_code': 'dx_1'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

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

# These are completely dependent on data source

# -1: "Not Set",
# 0: "Unknown",
# 1: "Nationally representative only",
# 2: "Representative for subnational location only",
# 3: "Not representative",
# 4: "Nationally and subnationally representative",
# 5: "Nationally and urban/rural representative",
# 6: "Nationally, subnationally and urban/rural representative",
# 7: "Representative for subnational location and below",
# 8: "Representative for subnational location and urban/rural",
# 9: "Representative for subnational location, urban/rural and below",
# 10: "Representative of urban areas only",
# 11: "Representative of rural areas only"

df['representative_id'] = 3
df['location_id'] = 164

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'NPL_HID'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df['facility_id'] = 'hospital'
df['outcome_id'] = 'discharge'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2010: 292436, 2011: 292435, 2013: 292437}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
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

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

# ensure that cause code is all upper case
df.dx_1 = df.dx_1.str.upper()

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
# df['val'] = 1  # Nepal data is tabulated

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

# Group by all features we want to keep and sums 'val'
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
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################
df_agg['cause_code'] = df_agg['cause_code'].astype(str)
# Saving the file
write_path = root + r"FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
# df_agg.to_csv(write_path, index = False)
