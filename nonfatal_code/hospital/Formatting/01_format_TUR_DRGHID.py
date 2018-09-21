# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

Formatting Turkey data

The Turkish DRG system (TIG, or Tani Iliskili Gruplar in Turkish)
is administered by a branch of the MoH called the Department of Diagnosis
Related Groups (Teshis Iliskili Gruplar Daire Baskanligi, in Turkish -
http://www.tig.saglik.gov.tr/). Although this is a "DRG database" we don't
actually have the DRG codes for patients. The main reimbursement agency is
the Social Security Institute (SGK, or Sosyal Güvenlik Kurumu in Turkish -
http://www.sgk.gov.tr/).


Files are split up by sex. THERE IS NO SEX COLUMN IN THE DATA

"""

import pandas as pd
import platform
import numpy as np
import sys

# load our functions
hosp_path = "FILEPATH"
#sys.path.append(hosp_path)

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


df_male_2011 = pd.read_csv(filepath +
                           r"FILEPATH", sep=";",
                           names=['dx_1', 'age', 'val'])
df_female_2011 = pd.read_csv(filepath +
                             r"FILEPATH", sep=";",
                             names=['dx_1', 'age', 'val'])
df_male_2012 = pd.read_csv(filepath +
                           r"FILEPATH", sep=";",
                           names=['dx_1', 'age', 'val'])
df_female_2012 = pd.read_csv(filepath +
                             r"FILEPATH", sep=";",
                             names=['dx_1', 'age', 'val'])

df_male_2011['sex_id'] = 1
df_male_2011['year'] = 2011

df_female_2011['sex_id'] = 2
df_female_2011['year'] = 2011

df_male_2012['sex_id'] = 1
df_male_2012['year'] = 2012

df_female_2012['sex_id'] = 2
df_female_2012['year'] = 2012

df = pd.concat([df_male_2011, df_female_2011, df_male_2012, df_female_2012])
del df_male_2011
del df_female_2011
del df_male_2012
del df_female_2012

# Select features from raw data to keep
# keep = ['year', 'sex_id', 'age', 'dx_1', 'dx_2', 'dx_3',
#          'outcome_id', 'facility_id']
# df = df[keep]

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
#    'dx_1': 'dx_1',
#    'dx_2': 'dx_2',
#    'dx_3': 'dx_3'
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)


# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
del new_col_df

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['representative_id'] = 3
df['location_id'] = 155

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'TUR_DRGHID'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# fill nid
df['nid'] = np.nan
df.loc[df['year'] == 2011, 'nid'] = 130051
df.loc[df['year'] == 2012, 'nid'] = 130054

df['outcome_id'] = "discharge"

df['facility_id'] = "hospital"

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col

# drop unknown sex_ids
# df = df.query("sex_id == 1 | sex_id == 2")

# Create year range if the data covers multiple years
#df = year_range(df)

# create year_start and year_end
df['year_start'] = df['year']
df['year_end'] = df['year']
df.drop('year', axis=1, inplace=True)

# Replace feature levels manually, if any are needed

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
# %%
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

# check if all columns are there
assert len(hosp_frmat_feat) == len(df_agg.columns),\
    "The DataFrame has the wrong number of columns"
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
