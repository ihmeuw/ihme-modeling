# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017

Formatting Portugal

Data is located in FILEPATH

There are 7 files, and they seem to be an sample of a relational database
There are patient ID numbers, which persumably act as keys.  For example, the
file "Episodios" has sex, age, outcome, ect ..., but doesn't have any diagnoses
those are in different files.  In fact the file
FILEPATH acts as a code book and talks about
variables that are common and unique to all the files.

##############################################################################

file Episodios Codebook in English:

- [var name, our name for it, definition]
- ano (year): year
- sexo (sex_id): sex
- idade (age): Age of the user, in years, at the date of entry
- dsp (outcome_id): Code of destination of the user after the discharge of a
hospital
service:
    0 - Unknown
    1 - For the household
    2 - To another institution with hospitalization
    6 - Home service
    7 - Exit against medical opinion
    13 - Specialized aftercare (tertiary) (collected as of 2011)
    20 - Deceased
    51 - Palliative care - medical center (collected as of 2011)
    61 - Post-hospital care (CMS 19-22, AP21) (collected as of 2011)
    63 - Long-term hospital care (CMS 19-22, AP 21) (collected as of 2011)
- adm_tip (facility_id): Nature or mode of admission of a patient to a
health facility:
    1 - Scheduled
    2 - Urgent
    3 - Access
    4 - Peclec
    5 - Private Medicine
    6 - SIGIC
    7 - PACO
    12 - External SIGIC (since 2012)
    It can be grouped as Programmed (codes 1,3,4,6 and 12), Urgent (code 2),
    Private Medicine (code 5) and Access Plan for Ophthalmic Surgery (code 7)

##############################################################################

CODEBOOK for file "diagnoses"

-cod_diagnostico: ICD-9-CM Code ("International Classification of Diseases,
9th Revision, Clinical
- tipo_p_s:
    P- Main Diagnosis: defined as one who, after the study of the patient, is
    considered responsible for admission of the patient to the hospital for
    treatment.
    S- Additional diagnosis is any diagnosis attributed to a patient, in a
    particular episode of care, in addition to the main diagnosis.
- ordem: Coding order where:
    0 - main diagnosis
    Other orders (1,2,3 ...) - additional diagnostics
- pna: It identifies if the diagnosis was present when the patient was
admitted, being:
    S-Yes
    N - No
    D - Unknown
    I - Undetermined clinically
    NA - Exempt PNA registration

##############################################################################

CODEBOOK for the file "external causes"

- cod_diagnostico: ICD-9-CM Code ("International Classification of Diseases,
9th Revision, Clinical Modification"), which identifies the Diagnosis.
- cod_causa_ext: ICD-9-CM (International Classification of Diseases, 9th
Revision, Clinical Modification) Code of External Cause that led the patient
to the health institution. External cause codes allow you to codify the
circumstances in which a certain injury or intoxication occurred. See Section
3 for designations.
- tipo_p_s:
    P- Main Diagnosis: defined as one who, after the study of the patient, is
    considered responsible for admission of the patient to the hospital for
    treatment.
    S- Additional diagnosis is any diagnosis attributed to a patient, in a
    particular episode of care, in addition to the main diagnosis.
- ordem: Coding order where:
    0 - main diagnosis
    Other orders (1,2,3 ...) - additional diagnostics
- ordem_assoc: Order of association of the external cause to the diagnosis,
where:
    1 - First external cause associated to the diagnosis in reference;
    2 - Second external cause associated to the diagnosis in reference;
    3 - Third external cause associated to the diagnosis in reference;
    4 - ....
- pna: It identifies if the diagnosis was present when the patient was
admitted, being:
    S-Yes
    N - No
    D - Unknown
    I - Undetermined clinically
    NA - Exempt PNA registration

NOTES
- "ordem" variable appears to be zero indexed.
- "ordem_assoc" in external causes tells you which diagnoses the external cause
corresponds to

"""
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
hosp_path = r"FILEPATH"
sys.path.append(hosp_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"
# %%
#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# read in episodes information
df_episodes = pd.read_excel(root + r"FILEPATH")
df_episodes = df_episodes[['ano', 'seq_number', 'sexo', 'idade', 'dsp']]

# read in diagnosis information
df_diagnoses = pd.read_excel(root + r"FILEPATH")
df_diagnoses = df_diagnoses[['seq_episodio_int', 'cod_diagnostico',
                             'tipo_p_s']]

# read in external causes information
df_external_causes = pd.read_excel(root + r"FILEPATH")
df_external_causes = df_external_causes[['seq_episodio_int',
                                         'cod_diagnostico', 'cod_causa_ext',
                                         'tipo_p_s']]
# %%

# since episodes and diagnoses have the same number of unique ids, check if
# the ids are the same by asserting the disjoint set is the empty set
assert set.difference(set(df_episodes.seq_number.unique()),
                      set(df_diagnoses.seq_episodio_int.unique())) == set(),\
    "ids don't match between sets"

# since external causes have fewer ids than the episodes file, check that all
# the ids in the external causes files are contained within the episodes file.
# demonstartion: set([1]) - set([1,2,3,4]) = set().  We want the empty set:
assert set(df_external_causes.seq_episodio_int.unique()) -\
    set(df_episodes.seq_number.unique()) == set(), "result should be empty set"

# %%

# MERGING ALL INFO INTO ONE DATAFRAME
merged_ex_dx = df_diagnoses.merge(df_external_causes, how='left',
                                  on=['seq_episodio_int', 'cod_diagnostico',
                                      'tipo_p_s'], suffixes=['_diag', '_inj'])
merged_ex_dx.rename(columns={'seq_episodio_int': 'seq_number'}, inplace=True)
df = df_episodes.merge(merged_ex_dx, how='left', on='seq_number')

# lose the constituent parts now that we have the whole
# %%
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows")

# Select features from raw data to keep
df.drop('seq_number', axis=1, inplace=True)  # don't need it anymore

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'ano': 'year',
#    'year_start': 'year_start',
#    'year_end': 'year_end',
    'sexo': 'sex_id',
    'idade': 'age',
#    'age_start': 'age_start',
#    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'dsp': 'outcome_id',
    'adm_tip': 'facility_id',
    # diagnosis varibles
    # THIS DATA HAS MULT DX LONG ALREADY
#    'dx_1': 'dx_1',
#    'dx_2': 'dx_2',
#    'dx_3': 'dx_3'

    # source specific vars
    "tipo_p_s": "diagnosis_id"
    }

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
df['representative_id'] = 3  # not representative
df['location_id'] = 91

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'PRT_CAHS'  # Central Administration of the Health System

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 1  # it's ICD 9

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
#nid_dictionary = {'example_year': 'example_nid'}
#df = fill_nid(df, nid_dictionary)
df['nid'] = 285520

df['facility_id'] = 'inpatient unknown'
# %%
#####################################################
# CLEAN VARIABLES
#####################################################

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

# FIX AGES
df = df[df['age'] < 125]  # get rid of ages that make no sense
df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col

# FIX SEXES
# PRT already has 1=Males, 2=Females

#m = df.loc[df['sex_id'] == 1, 'age_start']
#f = df.loc[df['sex_id'] == 2, 'age_start']
# %%
#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# diagnosis id
df['diagnosis_id'].replace(["P", "S"], [1, 2], inplace=True)

# outcome_id
outcome_dict = {
    0: 'unknown',
    1: 'discharge',
    2: 'discharge',
    6: 'discharge',
    7: 'discharge',
    13: 'discharge',
    20: 'death',
    51: 'discharge',
    61: 'discharge',
    63: 'discharge'}
df['outcome_id'].replace(outcome_dict, inplace=True)

# %%

# switch external cause and nature of injury
# this line switches dx_1 and dx_2 where they meet both condtions
df.loc[df.cod_causa_ext.notnull(), ['cod_diagnostico', 'cod_causa_ext']] =\
    df.loc[df.cod_causa_ext.notnull(), ['cod_causa_ext', 'cod_diagnostico']].values


# %%

# Columns contain only 1 optimized data type
int_cols = ['year', 'sex_id', 'diagnosis_id', 'representative_id',
            'code_system_id', 'age_group_unit', 'age_group_unit', 'nid',
            'metric_id', 'age_start', 'age_end']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
str_cols = ['cod_diagnostico', 'source', 'facility_id', 'outcome_id']
# cast external causes, which have nulls, to string, while preserving nulls
df['cod_causa_ext'] = df.cod_causa_ext.loc[df.cod_causa_ext.notnull()].map(str)


for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)
# %%

# making external causes be in the same column as diagnoses
# want all the stuff that is in the "external cause" column to be a secondary
# diagnosis

# remember that we already swtiched the stuff that _was_ in external causes
# into the diagnosis column.

# TEST CODE
test = df[df.cod_causa_ext.notnull()].sample(5)
test.rename(columns={'cod_causa_ext': 'dx_2', 'cod_diagnostico': 'dx_1'},
            inplace=True)
diagnosis_feats = test.columns[test.columns.str.startswith('dx_')]
for feat in diagnosis_feats:
    test[feat] = sanitize_diagnoses(test[feat])
test.rename(columns={'diagnosis_id': 'old_diagnosis_id'}, inplace=True)
test_after = stack_merger(test)

# want the stuff that used to be labeled as a secondary diagnosis to continue
# to be labeled as a secondary diagnosis  (stack_merger was not made to be
# used like this)
test_after.loc[test_after.old_diagnosis_id == 2, 'diagnosis_id'] = 2

# %%

# make the the two columns that have diagnosis info stacked (long)
df.rename(columns={'cod_causa_ext': 'dx_2', 'cod_diagnostico': 'dx_1'},
            inplace=True)  # rename so we can use stack_merger

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])
df.rename(columns={'diagnosis_id': 'old_diagnosis_id'}, inplace=True)  # want
# to keep the old diagnosis column, and avoid a name collision inside
# stack_merger


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

# want the stuff that used to be labeled as a secondary diagnosis to continue
# to be labeled as a secondary diagnosis  (stack_merger was not made to be
# used like this)
df.loc[df.old_diagnosis_id == 2, 'diagnosis_id'] = 2
df.drop("old_diagnosis_id", axis=1, inplace=True)  # drop old diagnosis id
df.drop("patient_index", axis=1, inplace=True)  # by product we don't need
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

# still haven't made year_start and year_end
df['year_start'] = df['year']
df['year_end'] = df['year']
df.drop('year', axis=1, inplace=True)

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
write_path = root + "FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
# df_agg.to_csv(write_path, index = False)
