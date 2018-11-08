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
path = r"FILEPATH"
sys.path.append(path)
sys.path.append(path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = ROOT
else:
    root = ROOT

warnings.warn("""

              THIS SOURCE DOES NOT HAVE ICD CODES!!! It has 312 proprietary
              codes. The column cause_code
              contains the disease codes.  In the mapping step we map them to
              baby sequelae.

              """)

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# Vietnam data without any age or sex info
fpath =root + r"{FILEPATH}VNM_3_Hosptial data year 2013 by diseases.xlsx"
df = pd.read_excel(fpath, sheetname="Year 2013")

#rename some columns
df.columns = ['disease_code', 'disease_name', 'otp_total', 'otp_female',
              'otp-cases-0-15', 'otp-deaths-0-125', 'inp_total', 'inp_female',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under_15',
              'inp-cases-0-5', 'inp_under_15_deaths', 'inp-deaths-0-5']
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

df['inp-cases-5-15'] = df['inp_under_15'] - df['inp-cases-0-5']
df['inp-cases-15-125'] = df['inp_total'] - df['inp_under_15']

df['inp-deaths-5-15'] = df['inp_under_15_deaths'] - df['inp-deaths-0-5']
df['inp-deaths-15-125'] = df['inp_total_deaths'] - df['inp_under_15_deaths']

df['otp-cases-15-125'] = df['otp_total'] - df['otp-cases-0-15']

# extract the rows where there are fewer total cases than child cases (which
# shouldn't be possible)
neg_list = []
constructed_cols = ['inp-cases-5-15', 'inp-cases-15-125', 'inp-deaths-5-15', 'inp-deaths-15-125', 'otp-cases-15-125']
for col in constructed_cols:
    if (df[col] < 0).sum() > 0:
        neg_list.append(df[df[col] < 0])
negs = pd.concat(neg_list)

df['otp_male'] = df['otp_total'] - df['otp_female']
df['inp_male'] = df['inp_total'] - df['inp_female']
df['inp_male_deaths'] = df['inp_total_deaths'] - df['inp_female_deaths']

df = df[['disease_code', 'disease_name',
         'otp_total',
         'inp_total',
         'inp_total_deaths']].copy()

df.rename(columns={'otp_total': 'otp_cases', 'inp_total': 'inp_cases',
                   'inp_total_deaths': 'inp_deaths'}, inplace=True)

df = df.set_index(['disease_code', 'disease_name']).stack().\
    reset_index()
df.rename(columns={'level_2': 'facility_outcome', 0: 'val'}, inplace=True)

df['facility_id'], df['outcome_id'] = df['facility_outcome'].str.split("_", 1).str
df.drop('facility_outcome', axis=1, inplace=True)
# There no age detail here at all, we're just using total counts
df['age_start'], df['age_end'] = 0, 125

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

df['representative_id'] = 3  # Do not take this as gospel, it's guesswork
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

str_cols = ['dx_1', 'source', 'facility_id', 'outcome_id']


if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}".format(str_cols),
                  " These nulls will be converted to the string 'nan'")

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

df.drop("disease_name", axis=1, inplace=True)

df['outcome_id'] = df.outcome_id.replace({"cases":"case", "deaths": "death"})

df['facility_id'] = df.facility_id.replace({"otp": "outpatient unknown",
                                            "inp": "inpatient unknown"})

# NOTE Deaths are already included in 'case'
df = df[df.outcome_id != 'death']

df = df[df.val > 0]

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
assert total_cases == df_agg.val.sum(),\
    "some cases were lost"
assert df[df.facility_id=='inpatient unknown'].val.sum() == inp_total_cases,\
    "some inpatient cases were lost"
# %%
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = (r"{}{FILEPATH}/formatted_VNM_MOH.H5".format(root))
write_hosp_file(df_agg, write_path, backup=True)
