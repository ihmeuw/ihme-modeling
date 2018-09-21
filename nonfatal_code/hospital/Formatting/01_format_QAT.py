# -*- coding: utf-8 -*-
"""
Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

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

# read in each file
df02 = pd.read_excel("FILEPATH", header=1)
df03 = pd.read_excel("FILEPATH", header=1)
# this data is for a women's hospital and uses aggregated ICD codes
df03w = pd.read_excel("FILEPATH", header=1)

df02['year_start'] = 2002
df03['year_start'] = 2003
df03w['year_start'] = 2003
df03w['SEX'] = 'W'


def reshape_qatar(df):
    # forward fill the icd codes
    df['ICD 9 CODE'] = df['ICD 9 CODE'].fillna(method='ffill')

    # drop rows which don't have any cases
    df.dropna(subset=['NAT', 'SEX'], inplace=True)
    assert sorted(df.SEX.unique()) == sorted(['F', 'M'])
    assert sorted(df.NAT.unique()) == sorted(['Q', 'NQ'])
    age_cols = df.filter(regex="^[0-9]").columns

    for col in age_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')

    df['sum_calc'] = df[age_cols].sum(axis=1)
    assert (df.SUM == df.sum_calc).all()
    # drop aggregation columns
    df.drop(axis=1, labels=["DISHCARGE DIAGNOSIS", "SUM", "sum_calc",
                            "AVG LOS", "NO. OF DEATHS"], inplace=True)

    df.rename(columns={'ICD 9 CODE': 'cause_code', 'NAT': 'nat', 'SEX': 'sex_id'},
               inplace=True)

    df = df.set_index(['cause_code', 'year_start', 'nat', 'sex_id']).stack().\
        reset_index()
    df.rename(columns={'level_4': 'age', 0: 'val'}, inplace=True)

    # fit terminal age group in the same format as every other age group
    df.loc[df.age == "85+", "age"] = "85-99"

    # conver val to numeric
    df.val = pd.to_numeric(df.val)

    # drop zero counts
    df = df[df.val != 0]

    df['age_start'], df['age_end'] = df['age'].str.split("-", 1).str

    # keep nationality distinction or count everything?
    df.drop(['age', 'nat'], axis=1, inplace=True)

    pre = df.cause_code.value_counts()
    pre.index = pre.index.astype(str)
    pre = pre.sort_index()
    # make ICD codes all uppper case
    df['cause_code'] = df['cause_code'].astype(str)
    df['cause_code'] = df['cause_code'].str.upper()
    post = df.cause_code.value_counts().sort_index()
    assert (pre == post).all()

    return(df)


df_list = []
for df in [df02, df03]:
    df_list.append(reshape_qatar(df))

df = pd.concat(df_list)

df.to_hdf(root + "FILEPATH",
          key="df")


#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# If this assert fails uncomment this line:
df.reset_index(drop=True, inplace=True)
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

# These are completely dependent on data source
df['representative_id'] = 3  # "Not representative"
df['location_id'] = 151

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'QAT_AIDA'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 1

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df['facility_id'] = 'hospital'
df['year_end'] = df['year_start']
df['outcome_id'] = 'discharge'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2002: 68367, 2003: 68535}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################
df['sex_id'].replace(['M','F'],[1,2], inplace = True)


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

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################


# Replace feature levels manually
# Make the values contained in the data match the shared tables
#   - E.g.: "MALE" should become 1, "FEMALE" should become 2
# These are merely examples

# df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],
#                          ["death","discharge","discharge"], inplace = True)
# df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS','1 - ROUTINE','2 - EMERGENCY TO 24 HOURS'],
#                           ['hospital','hospital','emergency'], inplace=True)




# Manually verify the replacements
# assert len(df['sex_id'].unique()) == 2, "df['sex_id'] should have 2 feature levels"
# assert len(df['outcome_id'].unique()) == 2, "df['outcome_id'] should have 2 feature levels"
# assert len(df['facility_id'].unique() == 2), "df['facility_id] should have 2 feature levels"

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

# If individual record: add one case for every diagnosis
# df['val'] = 1  # Qatar is tabulated

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

# Saving the file
write_path = root + r"FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
