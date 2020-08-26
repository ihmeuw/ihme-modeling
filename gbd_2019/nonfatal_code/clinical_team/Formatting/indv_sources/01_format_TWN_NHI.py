
"""
Formatting Template Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

--potentially more years on the way--
URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

user = getpass.getuser()


prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)

import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





filepath = r"FILEPATH".format(root)

df = pd.read_stata(filepath)

start_cases = df.person_times.sum()



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex': 'sex_id',
    'age_value': 'age',
    
    
    'age_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome': 'outcome_id',
    'facility_type': 'facility_id',
    
    'diagnosis_1': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)


assert df.age[df.age_group_unit == 'days'].max() <= 365
df.loc[df.age_group_unit == 'days', 'age'] = 0



df['year'] = 2016























df['representative_id'] = 1  
df['location_id'] = 8


df['year'] = pd.to_numeric(df['year'], errors='raise')

df['age_group_unit'] = 1
df['source'] = 'TWN_NHI'


df['code_system_id'] = 2


df['outcome_id'] = df['outcome_id'].str.lower()

df['year_start'] = df['year']
df['year_end'] = df['year_start']

df['metric_id'] = 1


nid_dictionary = {2016: 336203}
df = hosp_prep.fill_nid(df, nid_dictionary)

















df['facility_id'].replace(['Inpatient','1','1_emg', '2', '2_emg', '3'],
                        ['inpatient unknown','outpatient unknown','emergency',
                        'dentistry', 'emergency dentistry', 'chinese medicine'], 
                        inplace=True)








df['val'] = df['person_times']

assert start_cases == df.val.sum(), "Some cases have been added or lost"






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)





df.loc[df['age'] > 95, 'age'] = 95  

df = hosp_prep.age_binning(df)


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3






    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")









print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()






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


assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])


for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
    
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)


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

assert start_cases == df_agg.val.sum(), "Some cases were added or lost"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")






write_path = r"FILEPATH"\
            "FILEPATH".format(root)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
