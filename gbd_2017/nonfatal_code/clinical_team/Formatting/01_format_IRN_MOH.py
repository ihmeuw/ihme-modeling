# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

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

# read locally until the J drive issues are fixed
filepath = "FILEPATH/IRN_HOSPITAL_DATA_2001_2010_"\
                    "AGE_SEX_PROVINCE_ICD10_Y2017M01D09.DTA"

df = pd.read_stata(filepath)

# If this assert fails uncomment this line:
#df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


df.drop('resential_province', axis=1, inplace=True)

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year_start',
    'sex': 'sex_id',
    'age_group': 'age_start',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'ICD': 'dx_1',
    'count': 'val'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

start_cases = df.val.sum()

df['representative_id'] = 3  # Do not take this as gospel, it's guesswork
df['location_id'] = 142

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'IRN_MOH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# case is the sum of live discharges and deaths
df['outcome_id'] = "case"

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df['facility_id'] = 'hospital'

df['year_start'] = df['year_start'] + 621

# set up the age end var
df['age_end'] = df['age_start'] + 5
df.loc[df.age_start == 0, 'age_end'] = 1
df.loc[df.age_start == 1, 'age_end'] = 5
# set missing ages to the widest range
df.loc[df.age_start.isnull(), 'age_start'] = 0
df.loc[df.age_end.isnull(), 'age_end'] = 125

# in Iran females are coded to 0
df.sex_id = df.sex_id.replace([0], [2])

df['year_end'] = df['year_start']


# There's only 1 NID for Iran
df['nid'] = 331084

assert not df.nid.isnull().any(), "Null NIDs are present"

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
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
# drop unknown sex_id
df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3

assert start_cases == df.val.sum(), "case counts have changed"


df = df[df.dx_1 != "."].copy()
inter_cases = df.val.sum()
#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
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

assert inter_cases == df_agg.val.sum()

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_IRN_MOH.H5"

write_hosp_file(df_agg, write_path, backup=True)

