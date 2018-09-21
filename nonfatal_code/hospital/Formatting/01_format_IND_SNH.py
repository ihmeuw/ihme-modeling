# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
format IND data from Nazareth hospital in Shillong
@author: USERNAME
"""
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
USERNAME = "FILEPATH"
sys.path.append(USERNAME)

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

filepath = root + "FILEPATH"
df = pd.read_stata(filepath)

# review the date of admission and discharge variables
# some dates are missing but everything else occurs in 2014
pd.to_datetime(df.dateofdischarge, dayfirst=True, errors='ignore').sort_values()
pd.to_datetime(df.dateofadmission, dayfirst=True, errors='ignore').sort_values()

df['year_start'], df['year_end'] = 2014, 2014

# Select features from raw data to keep
keep = ['year_start', 'year_end', 'age_years', 'sex',
        'patientcode', 'opdinpatient',  'ruralurban',
        'icd10coding_1', 'icd10coding_2', 'icd10coding_3', 'icd10coding_4',
        'icd10coding_5', 'icd10coding_6']

df = df[keep]

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex': 'sex_id',
    'age_years': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    # diagnosis varibles
    'icd10coding_1': 'dx_1',
    'icd10coding_2': 'dx_2',
    'icd10coding_3': 'dx_3',
    'icd10coding_4': 'dx_4',
    'icd10coding_5': 'dx_5',
    'icd10coding_6': 'dx_6'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values())\
                                         - set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['representative_id'] = 3  # Not representative
df['location_id'] = 4862

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'IND_SNH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# facility_id can take one of multiple values
df['facility_id'] = 'hospital'

# discharge or death
df['outcome_id'] = 'discharge'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2014 : 281819}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

# remove missing age values
df = df[df['age'] != ""]
# convert age feature to int from str
df['age'] = df['age'].astype(int)
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

# Reshape diagnoses from wide to long
#   - review `hosp_prep.py` for additional function documentation
df = stack_merger(df)

# If individual record: add one case for every diagnosis
df['val'] = 1

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n" + str(not(df.isnull().sum()).all()==0))

# Group by all features we want to keep and sums 'val'
group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end','year_start',
           'year_end','location_id','nid','age_group_unit','source','facility_id',
          'code_system_id','outcome_id','representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val':'sum'}).reset_index()

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source','nid',
                   'facility_id',
                   'code_system_id', 'cause_code']

df_agg = df_agg[hosp_frmat_feat]

# check if all columns are there
assert len(hosp_frmat_feat) == len(df_agg.columns), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns, "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code','source','facility_id','outcome_id'], axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now) are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),    "number of feature levels of years should match number of feature levels of nid"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),    "number of feature levels age start should match number of feature levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2, "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2, "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1, "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table', complib='blosc',
              complevel=5, mode='w')
# df_agg.to_csv(write_path, index = False)
