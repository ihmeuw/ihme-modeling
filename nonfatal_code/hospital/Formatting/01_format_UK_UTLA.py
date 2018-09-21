# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017

Format the UK UTLA data, data has already been merged with regional data
and desuppressed with linear model weighting in the scripts
Formatting/001_pre_format_uk_cluster.py
and
Formatting/001_pre_format_UK_UTLA_fit_models.py
respectively.

"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

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
if platform.system() == "Linux":
    filepath = root + "FILEPATH"
    df = pd.read_csv(filepath)
else:
    filepath = root + "FILEPATH"
    df = pd.read_hdf(filepath, key="df")

# read in injuries data
inj = pd.read_csv(root + "FILEPATH")
# clean the column names of injuries
inj.rename(columns={'SEX': 'sex_id', 'CAUSE': 'cause_code'}, inplace=True)
# injuries data was dual E/N encoded and there are a lot of rows where E codes
# weren't provided
inj = inj[inj.cause_code.notnull()]

# create fiscal year for the merge
inj['FYEAR'] = inj['FYEAR'].astype(str)
inj['year_end'] = inj['FYEAR'].str[-2:].astype(np.int)
inj['year_end'] = inj['year_end'] + 2000
inj['year_start'] = inj['year_end'] - 1
inj['fiscal_year'] = (inj['year_start'] + inj['year_end']) / 2
# formate ages into age_start and age_end in utla
inj.loc[inj['AgeBand'] == '90+', 'AgeBand'] = '9099'
inj.loc[inj['AgeBand'] == '0000', 'AgeBand'] = '0001'

inj['age_start'] = inj['AgeBand'].str[0:2]
inj['age_end'] = inj['AgeBand'].str[2:]
inj['age_start'] = pd.to_numeric(inj['age_start'], errors='raise')
inj['age_end'] = pd.to_numeric(inj['age_end'], errors='raise')

# and there are a few missing ages
inj.loc[inj.age_start.isnull(), ['age_start', 'age_end']] = [0, 99]

inj.drop(['FYEAR', 'year_start', 'year_end', 'AgeBand', 'DiagCode3'], axis=1, inplace=True)
inj['location_id'] = 4749

# concat injuries and non injuries together
df = pd.concat([df, inj])

# If this assert fails uncomment this line:
df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# keep cols we want
df = df[['location_id', 'age_start', 'age_end', 'sex_id', 'fiscal_year', 'cause_code', 'value']]

val_sum = df.value.sum()

# CONVERT FROM FISCAL YEARS TO CALENDAR USING USING METHOD FROM LAST YEAR
# get half year values and regular calendar years
df['start_value'] = df['value'] / 2
df['end_value'] = df['value'] / 2
# make calendar years
df['year_start'] = df['fiscal_year'] - 0.5
df['year_end'] = df['fiscal_year'] + 0.5
df.drop(['fiscal_year', 'value'], axis=1, inplace=True)
# reshape long to duplicate half counts
df = df.set_index(['location_id', 'age_start', 'age_end', 'sex_id', 'cause_code', 'year_start', 'year_end']).\
                stack().reset_index()

# match year start and year end to the discharge data
df.loc[df['level_7'] == 'start_value', 'year_end'] =\
    df.loc[df['level_7'] == 'start_value', 'year_start']
df.loc[df['level_7'] == 'end_value', 'year_start'] =\
    df.loc[df['level_7'] == 'end_value', 'year_end']
df.rename(columns={0: 'value'}, inplace=True)
df.drop(['level_7'], axis=1, inplace=True)

# double min and max values (same method used last year)
year_min = df.year_start.min()
year_max = df.year_start.max()
df.loc[df['year_start'] == year_min, 'value'] =\
        df.loc[df['year_start'] == year_min, 'value'] * 2
df.loc[df['year_end'] == year_max, 'value'] =\
        df.loc[df['year_end'] == year_max, 'value'] * 2

# confirm cases weren't lost
val1 = df[(df.year_start != df.year_start.min()) & (df.year_end != df.year_end.max())].value.sum()
val2 = df[df.year_start == df.year_start.min()].value.sum() / 2
val3 = df[df.year_end == df.year_end.max()].value.sum() / 2
assert round((val1 + val2 + val3), 2) == round(val_sum, 2)
val_sum = df.value.sum()

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    # 'fiscal_year': 'year',
    # 'year_start': 'year_start',
    # 'year_end': 'year_end',
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
    'cause_code': 'dx_1',
    'value': 'val'}

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

df['representative_id'] = 1  #

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'UK_HOSPITAL_STATISTICS'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1
df['facility_id'] = "hospital"
df['outcome_id'] = "case"


# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2001: 134187, 2002: 121272 , 2003: 121273 , 2004: 121274,
                  2005: 121275 , 2006: 121276 , 2007: 121277 , 2008: 121278,
                  2009: 121279 , 2010: 121280 , 2011: 121281 , 2012: 265423,
                  2013: 265424, 2014: 265425, 2015: 265425}
df = fill_nid(df, nid_dictionary)

assert df['nid'].isnull().sum() == 0

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

# use this to infer data types
# df.apply(lambda x: pd.lib.infer_dtype(x.values))

if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}"
                  r" These nulls will be converted to the string nan".
                  format(str_cols), Warning)

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
for col in str_cols:
    df[col] = df[col].astype(str)

# assign unknown sex id to 3 for sex splitting
df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

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
# df['sex_id'].replace(['2 - MEN','1 - FEMALE'],[1,2], inplace = True)
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
# df['val'] = 1

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
# because we're duplicating min and max years, we have more years than NIDs
# assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
#    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
assert round(df_agg.val.sum(), 3) == round(val_sum, 3),\
    "some cases were lost"
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + "FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
