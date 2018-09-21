# -*- coding: utf-8 -*-
"""
Template for formatting ECU data.

THESE ARE NEW YEARS FOR A SOURCE WE ALREADY HAD DATA FOR
we already had data for 1997 - 2011.  We decided not to use the formatting
script that we already had because the variable names changed, so new code was
required anyways.  plus, running the old code and incorperating all the data
together takes forever.

containing folder: FILEPATH

GHDx link: ADDRESS

NOTE: there are nearly DOUBLE the amount of females in this data compared to
Males.

Ecuador's National Institute of Statistics and Censuses (INEC) collects
inpatient discharge information from all health facilities operating in the
country, both public and private. Hospitals report discharge information on a
monthly basis. Data on healthy newborns are not collected as part of the
discharge statistics. INEC also collects information on the number,
department, and use of hospital beds. Data are released on an annual basis,
and can be downloaded either in the form of patient-level data files
(discharges and beds are provided separately), or as a hospital statistics
yearbook containing tabulated data. This is an ad hoc series name rather
than a system name.

"""
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
USERNAME_path = "FILEPATH"
#USERNAME_path = "FILEPATH"
sys.path.append(USERNAME_path)
#sys.path.append(USERNAME_path)

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
df_2012 = pd.read_stata(root + "FILEPATH",
                        columns=['Cau_cie10',  'Con_egrpa', 'Cond_edad',
                                 'Edad_pac',  'Sexo_pac'])
df_2012['year'] = 2012
df_2012.columns = map(str.lower, df_2012.columns)

df_2013 = pd.read_stata("FILEPATH",
                        columns=['cau_cie10', 'con_egrpa', 'cod_edad',
                                 'edad', 'sexo'])
df_2013['year'] = 2013

df_2014 = pd.read_stata("FILEPATH",
                        columns=['cau_cie10', 'con_egrpa', 'cod_edad',
                                 'edad', 'sexo'])
df_2014['year'] = 2014

# rename columns to match each other and to be in english
df_2012.rename(columns={'cond_edad': 'cod_edad', 'edad_pac': 'edad',
                        'sexo_pac': 'sexo'}, inplace=True)

assert(set.difference(set(df_2012.columns), set(df_2013.columns),
                      set(df_2014.columns)) == set()), "column mismatch"

df = pd.concat([df_2012, df_2013, df_2014])
df.reset_index(inplace=True, drop=True)  # ALWAYS DO THIS AFTER CONCAT
#df['year_end'] = df['year_start']

del df_2012
del df_2013
del df_2014

assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows")

# keep all the columns

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year',
#    'year_start': 'year_start',
#    'year_end': 'year_end',
    'sexo': 'sex_id',
    'edad': 'age',
#    'age_start': 'age_start',
#    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'con_egrpa': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'cau_cie10': 'dx_1',
#    'dx_2': 'dx_2',
#    'dx_3': 'dx_3',

    # source specific; other
    'cod_edad': 'age_group_unit'
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

# make year start and year end
df['year_start'] = df['year']
df['year_end'] = df['year']
df.drop('year', axis=1, inplace=True)

# These are completely dependent on data source
df['representative_id'] = 1  # Nationally representative only, this source was labeled as such in GBD 2015
df['location_id'] = 122

# group_unit 1 signifies age data is in years
#df['age_group_unit'] = 1
df['source'] = 'ECU_INES_12_14'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2012: 114876 ,
                  2013: 160484 ,
                  2014: 237756}
df = fill_nid(df, nid_dictionary)

df['facility_id'] = 'inpatient unknown'

outcome_dict = {
    'Alta': 'discharge',
    'Fallecido 48H y más': 'death',
    'Fallecido < 48H': 'death',
    'Fallecido en menos de 48H': 'death'
}

df['outcome_id'].replace(outcome_dict, inplace=True)
#####################################################
# CLEAN VARIABLES
#####################################################
# %%

# FIX AGES
age_unit_translation_dict = {
    "Años (1 a 98 años de edad)": "Years",
    "Meses (1 a 11 meses de edad)": "Months",
    'Días (1 a 29 días de edad)': 'Days',
    'Ignorado': 'Ignored',
    'Anios (1 a 115 anios de edad)': "Years",
    'Dias (1 a 29 dias de edad)': 'Days',
    'Años (1 a 115 años de edad)': "Years",
    'Días (1 a 28 días de edad)': 'Days'
    }
df['age_group_unit'].replace(age_unit_translation_dict, inplace=True)

# check that everything labeled as days or months don't amount to a year
assert df.loc[df['age_group_unit'] == "Days", 'age'].max() < 365
assert df.loc[df['age_group_unit'] == "Months", 'age'].max() < 12

# convert days to years
df.loc[df['age_group_unit'] == "Days", 'age'] = 0
df.loc[df['age_group_unit'] == "Days", 'age_group_unit'] = "Years"

# convert months to years
df.loc[df['age_group_unit'] == "Months", 'age'] = 0
df.loc[df['age_group_unit'] == "Months", 'age_group_unit'] = "Years"

# now everything is in years, so we can change 'age_group_unit' to say that
df['age_group_unit'] = 1

# force age int
df['age'] = pd.to_numeric(df['age'], downcast='integer', errors='raise')

  
df = df[df['age'] < 125]  # get rid of ages that make no sense
df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col

# FIX SEXES
fix_sex_dict = {'Mujeres': 2,
                'Mujer': 2,
                'Hombres': 1,
                'Hombre': 1}
df['sex_id'].replace(fix_sex_dict, inplace=True)

# %%
# viz age dist by sex
#import matplotlib.pyplot as plt
#%matplotlib inline
#
#m = df.loc[df['sex_id'] == 1, 'age_start']
#f = df.loc[df['sex_id'] == 2, 'age_start']
#
#plt.hist((m,f), bins=20, label=("Males", "Females"), color=('b','g'), edgecolor='k', normed=False)
#plt.legend()
#plt.xlabel("Age")
#plt.ylabel("Count")
#plt.title("Age distribution for Ecuador 2012-2014")



# %%

# convert or downsize datatypes (str to int, or int64 to int8)
# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id', 'code_system_id']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
str_cols = ['source', 'facility_id', 'outcome_id', 'dx_1']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

# %%
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])
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
df['val'] = 1
# %%
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
df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

# %%
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

# check if all columns are there
assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])

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
write_path = root + "FILEPATH"
df.to_hdf(write_path + "FILEPATH", key='df',
          format='table', complib='blosc', complevel=5, mode='w')
# df.to_csv(write_path, index = False)
