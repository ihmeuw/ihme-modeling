# -*- coding: utf-8 -*-
"""
Formatting German data

GHDx link: ADDRESS

Containing folder: FILEPATH

Reporting hospitals include psychiatric facilites, rehab and prevention
facilities, and exclude those connected with penal institutions. These
statistics also include deaths, which are differentiated from
other cases.  That is, deaths are in a separate table.

I didn't see a separate table for deaths.

In the data, there is a sheet called "inhalt" (german for "content") that
is a table of contents.  It says what data are on which pages.  According to
this table of contents, it appears that many of the sheets contain essentially
the same information, but split up different ways.

"""
# %%
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
USERNAME_path = "FILEPATH"
sys.path.append(USERNAME_path)

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
df = pd.read_excel("FILEPATH",
                   sheetname="Geschlecht_Anzahl_09", header=1,
                   encoding="utf-8")

# keep all features

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
#    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'Geschlecht': 'sex_id',
#    'age': 'age',
#    'age_start': 'age_start',
#    'age_end': 'age_end',
#    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'ICD-10-4': 'dx_1',
#    'dx_2': 'dx_2',  # we only have primary
#    'dx_3': 'dx_3'

    # source specific
    "Insgesamt": "total_all_age",
    'unter 1 Jahr': "under 1 years",
    '95 und älter': "95 and older",
    "unbekannt": "unknown age"
    }

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you do the manual data processing, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# These are completely dependent on data source
df['representative_id'] = 3  # Not representative
df['location_id'] = 81

# group_unit 1 signifies age data is in years
# df['age_group_unit'] = 1  this column starts with "age_" so comment it out
df['source'] = 'DEU_HSRS'  # for the ISO code for Germany, and the source:
# Germany Hospital Statistics Reporting System

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2  # we have icd 10

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# fill NID
df['nid'] = 67132

# fill year_start and end
df['year_start'] = 2009
df['year_end'] = 2009

# fill outcome ID
# An assumption is being made here:  The source has a separate table for deaths,
# so for lack of more information we assume that this table that we're using
# does not include deaths. Ergo it is discharges.
df['outcome_id'] = 'discharge'

# fill facility_id
df['facility_id'] = "inpatient unknown"

#####################################################
# CLEAN VARIABLES
#####################################################

# replace string values in the sex_id with ints
df['sex_id'] = np.where(df['sex_id'] == "m", 1, 2)

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end',
            'sex_id', 'nid', 'representative_id',
            'metric_id', ]
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)



#####################################################
# WIDE TO LONG AGES
#####################################################

# TEST CODE to demonstrate
#test_wide = pd.DataFrame({"dx_1": ["A10", "C27", "D87", "Z81"],
#                     "age_0-1": [1,1,1,1], "age_1-4":[4,4,4,4],
#                     "age_5-9":[9,9,9,9]})
#test_wide['id'] = test_wide.index
#
#test_long = pd.wide_to_long(test_wide, ["age_"], i="id", j="age")
#test_long = test_long.reset_index()
#test_long.rename(columns={"age_": "count"}, inplace=True)
#test_long.drop('id', axis=1, inplace=True)
# %%
# rename columns to that wide_to_long function knows what to look for
df.rename(columns={"under 1 years": 'age_0-1',
                   '1-4': "age_1-4",
                   '5-9': "age_5-9",
                   '10-14': "age_10-14",
                   '15-19': "age_15-19",
                   '20-24': "age_20-24",
                   '25-29': "age_25-29",
                   '30-34': "age_30-34",
                   '35-39': "age_35-39",
                   '40-44': "age_40-44",
                   '45-49': "age_45-49",
                   '50-54': "age_50-54",
                   '55-59': "age_55-59",
                   '60-64': "age_60-64",
                   '65-69': "age_65-69",
                   '70-74': "age_70-74",
                   '75-79': "age_75-79",
                   '80-84': "age_80-84",
                   '85-89': "age_85-89",
                   '90-94': "age_90-94",
                   '95 and older': 'age_95+',
                   'unknown age': 'age_unknown'}, inplace=True)
# %%
# make id var
df['id'] = df.index

# go wide to long
df = pd.wide_to_long(df, ['age_'], i='id', j='age')

# this creates a multi index with our age labels on the lower level
df = df.reset_index()  # reset to a normal index DO NO SET PARAM drop = TRUE
# the age info we want is in the index

# wide_to_long made a new col named "age_" that contains the values that were
# in the age_0-1, age_1-4, ... columns
df.rename(columns={"age_": "val"}, inplace=True)  # rename

# drop id from columns
df.drop('id', axis=1, inplace=True)

# recover age_group_unit
df['age_group_unit'] = 1
# %%

# split into age groups
df['age'] = df['age'].replace({'unknown': np.nan, '95+': '95-99'})  # replace
# to make most common pattern
df.dropna(subset=['age'], inplace=True)  # lose null ages, need age splitting
df['age_start'], df['age_end'] = df['age'].str.split("-", 1).str  # split into
# two columns on the '-'
df.drop('age', axis=1, inplace=True)  # don't need age anymore

# convert age columns from strings to ints
df['age_start'] = pd.to_numeric(df['age_start'], errors='raise',
                                downcast='integer')
df['age_end'] = pd.to_numeric(df['age_end'], errors='raise',
                              downcast='integer')

# because of the original wide format, we have a lot of ages with null counts
df.dropna(subset=['val'], inplace=True)
# %%

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
# df['val'] = 1  # we have tabulated data

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.")
else:
    print(">> No.")

# %%

group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
columns_before = df.columns
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source', 'nid',
                   'facility_id',
                   'code_system_id', 'cause_code']
df = df[hosp_frmat_feat]
columns_after = df.columns
# %%
# check if all columns are there
assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])
# %%
# check data types
for i in df.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df['year_start'].unique()) == len(df['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df['age_start'].unique()) == len(df['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df['source'].unique()) == 1,\
    "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
#FILEPATH
write_path = "FILEPATH"
df.to_hdf(write_path, key='df', format='table', complib='blosc',
          complevel=5, mode='w')
