"""
Format PHL health claims data as if it were hospital data
This script picks up the PHL data partway through the claims process
It's been extracted from access DBs and formatted to fit the claims process
This script gets it in line with our hospital process
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


df = pd.read_hdf(root + "FILEPATH", key='df')

# If this assert fails uncomment this line:
#df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# Select features from raw data to keep
#keep = ['year', 'sex_id', 'age', 'dx_1', 'dx_2',
#        'dx_3', 'outcome_id', 'facility_id']
#df = df[keep]
df.drop(['facility_id'], axis=1, inplace=True)

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    #'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'is_otp': 'facility_id'}
    # diagnosis varibles
    #'dx_1': 'dx_1',
    #'dx_2': 'dx_2',
    #'dx_3': 'dx_3'}

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
df['representative_id'] = 1

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'PHL_HICC'

# code 1 for ICD-9, code 2 for ICD-10
# df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

df.loc[df.facility_id == 'T', 'facility_id'] = 'outpatient unknown'
df.loc[df.facility_id == 'F', 'facility_id'] = 'inpatient unknown'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2013: 222560, 2014: 222563}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
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

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
# age pattern becomes very strange above age 112
df = df[df['age'] < 112]

# age_binning
df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)

# drop unknown sex_id
 
df = df.query("sex_id == 1 | sex_id == 2")

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
df = df[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id',
          'outcome_id', 'nid', 'facility_id', 'metric_id', 'code_system_id',
          'representative_id', 'source', 'age_group_unit'] + list(diagnosis_feats)]
split_df = np.array_split(df, 100)
del df
df = []
for each_df in split_df:
    each_df = each_df.set_index(['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id',
                                 'outcome_id', 'nid', 'facility_id', 'metric_id', 'code_system_id',
                                 'representative_id', 'source', 'age_group_unit']).stack().reset_index()
    # drop blank diagnoses
    each_df = each_df[each_df[0] != "None"]
    each_df = each_df[each_df[0] != ""]
    # rename cols
    each_df = each_df.rename(columns={'level_14': 'diagnosis_id', 0: 'cause_code'})
    # replace diagnosis_id with codes, 1 for primary, 2 for secondary
    # and beyond
    each_df['diagnosis_id'] = np.where(each_df['diagnosis_id'] ==
                                          'dx_1', 1, 2)
    # append to list of dataframes
    df.append(each_df)

df = pd.concat(df)
df.reset_index(inplace=True)

# If individual record: add one case for every diagnosis
df['val'] = 1

cases_sum = df.val.sum()

long_df = df.copy()

df.cause_code.value_counts()

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
assert set(columns_before) == set(columns_after),    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,        "%s is missing from the columns of the DataFrame"        % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),    "number of feature levels age start should match number of feature " +    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2,    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,    "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
