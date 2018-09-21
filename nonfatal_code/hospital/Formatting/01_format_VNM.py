# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.


"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings

# load our functions
hosp_path = "FILEPATH"
sys.path.append(hosp_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#%%
#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# Vietnam data without any age or sex info
fpath =root + "FILEPATH"
df = pd.read_excel(fpath, sheetname="Year 2013")


#rename some columns
df.columns = ['disease_code', 'disease_name', 'otp_total', 'otp_female',
              'otp-cases-0-14', 'otp-deaths-0-99', 'inp_total', 'inp_female',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under_15',
              'inp-cases-0-4', 'inp_under_15_deaths', 'inp-deaths-0-4']
df = df.iloc[3:,:]
# %%
inp_total_cases = df.inp_total.sum()
otp_total_cases = df.otp_total.sum()
total_cases = df.inp_total.sum() + df.otp_total.sum()
# %%
cols = list(df.columns)
cols.remove('disease_name')
for col in cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
    df[col] = df[col].fillna(0)
# %%
# create 5-15 and 16-99 columns
df['inp-cases-5-14'] = df['inp_under_15'] - df['inp-cases-0-4']
df['inp-cases-15-99'] = df['inp_total'] - df['inp_under_15']

df['inp-deaths-5-14'] = df['inp_under_15_deaths'] - df['inp-deaths-0-4']
df['inp-deaths-15-99'] = df['inp_total_deaths'] - df['inp_under_15_deaths']

df['otp-cases-15-99'] = df['otp_total'] - df['otp-cases-0-14']

# %%
# keep only the age delimited columns
df = df[['disease_code', 'disease_name',
         'otp-cases-15-99', 'otp-cases-0-14', 'otp-deaths-0-99',
         'inp-cases-0-4', 'inp-cases-5-14', 'inp-cases-15-99',
         'inp-deaths-0-4', 'inp-deaths-5-14', 'inp-deaths-15-99']].copy()

# %%
assert inp_total_cases == (df['inp-cases-0-4'] + df['inp-cases-5-14'] + df['inp-cases-15-99']).sum()
assert otp_total_cases == (df['otp-cases-0-14'] + df['otp-cases-15-99']).sum()
# %%
df = df.set_index(['disease_code', 'disease_name']).stack().\
    reset_index()
df.rename(columns={'level_2': 'facility_age', 0: 'val'}, inplace=True)
# %%
# split into facility type and age cols
df['facility_id'], df['outcome_id'], df['age_start'], df['age_end'] = df['facility_age'].str.split("-", 3).str
df.drop('facility_age', axis=1, inplace=True)

assert total_cases == df[df['outcome_id'] != 'deaths'].val.sum()

# %%
# If this assert fails uncomment this line:
#df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")
# %%

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'disease_code': 'dx_1'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
# %%
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

df['representative_id'] = 3  #
df['location_id'] = 20

df['sex_id'] = 3
df['year_start'], df['year_end'] = [2013, 2013]

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'VNM_MOH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
df['nid'] = 299375
# %%
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
str_cols = ['dx_1', 'source', 'facility_id', 'outcome_id']

# use this to infer data types
# df.apply(lambda x: pd.lib.infer_dtype(x.values))

if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}".format(str_cols),
                  " These nulls will be converted to the string 'nan'")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)
# %%
# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
#
#df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
## terminal age start will be lumped into the terminal age group.
#df = age_binning(df)
# %%
# drop unknown sex_id
#df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

# Create year range if the data covers multiple years
#df = year_range(df)

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
#for feat in diagnosis_feats:
#    df[feat] = sanitize_diagnoses(df[feat])
df.drop("disease_name", axis=1, inplace=True)

df['outcome_id'] = df.outcome_id.replace({"cases":"case", "deaths": "death"})

df['facility_id'] = df.facility_id.replace({"otp": "outpatient unknown",
                                            "inp": "inpatient unknown"})

# NOTE Deaths are already included in 'case'
df = df[df.outcome_id != 'death']

# %%
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
#df['val'] = 1
# %%
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
# %%
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
assert len(df_agg['sex_id'].unique()) <= 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
# %%
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = ("FILEPATH")
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5)
