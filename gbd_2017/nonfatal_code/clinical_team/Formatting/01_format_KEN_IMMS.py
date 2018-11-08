# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings

# load our functions
hosp_path = r"FILEPATH/Functions"
sys.path.append(hosp_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"


# read in spreadsheet
fpath = "FILEPATH/KEN_1999_HOSPITAL_INPATIENT_MORBIDITY_MORTALITY.XLS"
df = pd.read_excel(fpath,
                     sheetname='original', header=2)

df.reset_index(drop=True, inplace=True)

# manually add column names
col_names = ['groups_and_disease_name', 'icd_code',
             'grand_total', 'total_alive', 'total_dead',
             'male_alive', 'male_dead', 'female_alive', 'female_dead',
             '0-1-alive', '0-1-dead', '1-5-alive', '1-5-dead',
             '5-15-alive', '5-15-dead', '15-25-alive', '15-25-dead',
             '25-35-alive', '25-35-dead', '35-45-alive', '35-45-dead',
             '45-55-alive', '45-55-dead', '55-65-alive', '55-65-dead',
             '65-125-alive', '65-125-dead', '0-125-alive', '0-125-dead',
             'als', 'empty_col', 'total_calc_alive', 'total_calc_dead',
             'total', 'empty_col2', 'difference']

df.columns = col_names

# keep rows where icd codes exist This drops 4 rows, including the "grand_total"
df = df[df.icd_code.notnull()]

# add year
df['year_start'], df['year_end'] = [1999, 1999]

# drop the first 2 rows
df = df.iloc[2:, :]

assert df[df.total_dead != df.total_calc_dead].shape[0] == 0
assert df[df.total_alive != df.total_calc_alive].shape[0] == 0
assert df['icd_code'].isnull().sum() == 0

# split off the cols that need age splitting
df_no_age = df[['icd_code', 'male_alive', 'male_dead', 'female_alive',
                'female_dead', 'year_start', 'year_end']].copy()
df_no_age = df_no_age.iloc[2:, :]

# alive cols
alive_cols = df.filter(regex="^[0-9].*alive$").columns
# dead cols
dead_cols = df.filter(regex="^[0-9].*dead$").columns
# now keep just the cols for sex splitting
age_cols = df.filter(regex="^[0-9]").columns
df['sex_id'] = 3

# drop the empty columns and name columns
df.drop(['empty_col', 'empty_col2', 'groups_and_disease_name',
         'difference', 'als', 'total', 'total_calc_dead', 'total_calc_alive',
         'grand_total', 'total_alive', 'total_dead', 'male_alive', 'male_dead',
         'female_alive', 'female_dead'], axis=1, inplace=True)
assert (df.isnull().sum() == 0).all()


def reshape_long(df):
    # convert what will be 'val' column of counts to numeric
    for col in age_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')

    # reshape long to create age start, age end, sex and val columns
    df = df.set_index(['sex_id', 'year_start', 'year_end', 'icd_code']).stack().\
        reset_index()
    df.rename(columns={'level_4': 'age_outcome', 0: 'val'}, inplace=True)

    df['age_start'], df['age_end'], df['outcome'] = df['age_outcome'].str.split("-", 2).str

    # drop unneeded columns
    df.drop(['age_outcome'], axis=1, inplace=True)
    return(df)

df = reshape_long(df)


df.to_hdf("FILEPATH/formatted_KEN_IMMS.H5", key="df")

df_no_age['age_start'] = 0
df_no_age['age_end'] = 99
for col in ['male_alive', 'male_dead', 'female_alive', 'female_dead']:
    df_no_age[col] = pd.to_numeric(df_no_age[col], errors='raise')

# write the data that needs complete age splitting too
df_no_age.to_hdf("FILEPATH/formatted_KEN_IMMS_only_sexes.H5", key="df")

#####################################################
# KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# take the sum of all cases
val_sum = df.val.sum()

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
    'outcome': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'icd_code': 'dx_1'}

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

df['representative_id'] = 3  # "Not representative" cuz we don't know about source
df['location_id'] = 180

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'KEN_IMMS'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df['facility_id'] = "inpatient unknown"

# Create a dictionary with year-nid as key-value pairs
df['nid'] = 133665  # there's only one year, one nid

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
if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}".format(str_cols),
                  " These nulls will be converted to the string 'nan'")
for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

df['outcome_id'].replace(['alive', 'dead'], ['discharge', 'death'],
                         inplace=True)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

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

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
# this data needs sex splitting
# assert len(df_agg['sex_id'].unique()) == 2,\
#    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
assert val_sum == df_agg.val.sum()

assert df.query("outcome_id == 'discharge'").val.sum() >\
    df.query("outcome_id == 'death'").val.sum(),\
    "There are more deaths than discharges"

#####################################################
# WRITE TO FILE
#####################################################
df.apply(lambda x: pd.lib.infer_dtype(x.values))
df['cause_code'] = df['cause_code'].astype(str) # needed if runnin python 2
# Saving the file
write_path = "FILEPATH/formatted_KEN_IMMS.H5"
write_hosp_file(df_agg, write_path, backup=True)
