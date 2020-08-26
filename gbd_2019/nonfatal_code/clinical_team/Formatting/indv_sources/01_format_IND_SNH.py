
"""
Created on Mon Jan 23 17:32:28 2017
format IND data from Nazareth hospital in Shillong
@author: USERNAME
"""
import pandas as pd
import platform
import numpy as np
import sys
import getpass
import warnings

user = getpass.getuser()

hosp_path = r"FILEPATH".format(user)
sys.path.append(hosp_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    warnings.warn("This script cannot be run locally")
    exit
    






filepath = root + r"FILEPATH"
df = pd.read_stata(filepath)



pd.to_datetime(df.dateofadmission, dayfirst=True, errors='coerce').sort_values()


df.dateofdischarge = pd.to_datetime(df.dateofdischarge, dayfirst=True, errors='coerce')
df.dateofadmission = pd.to_datetime(df.dateofadmission, dayfirst=True, errors='coerce')

df['los'] = df['dateofdischarge'].subtract(df['dateofadmission'])

df = df[df['los'] != '0 days']

df['year_start'], df['year_end'] = 2014, 2014


keep = ['year_start', 'year_end', 'age_years', 'sex',
        'patientcode', 'opdinpatient',  'ruralurban',
        'icd10coding_1', 'icd10coding_2', 'icd10coding_3', 'icd10coding_4',
        'icd10coding_5', 'icd10coding_6']

df = df[keep]


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

    
    'outcome_id': 'outcome_id',
    
    'icd10coding_1': 'dx_1',
    'icd10coding_2': 'dx_2',
    'icd10coding_3': 'dx_3',
    'icd10coding_4': 'dx_4',
    'icd10coding_5': 'dx_5',
    'icd10coding_6': 'dx_6'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values())\
                                         - set(df.columns)))
df = df.join(new_col_df)






df['representative_id'] = 3  
df['location_id'] = 4862


df['age_group_unit'] = 1
df['source'] = 'IND_SNH'


df['code_system_id'] = 2


df['metric_id'] = 1


df['facility_id'] = 'hospital'


df['outcome_id'] = 'discharge'


nid_dictionary = {2014 : 281819}
df = fill_nid(df, nid_dictionary)











df.loc[df['age'] == ""] = np.nan

df['age'] = df['age'].astype(float)
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


df_wide = df.copy()
df_wide['metric_discharges'] = 1
df_wide = df_wide[df_wide.year_start.notnull()]
df_wide.to_stata("FILEPATH", write_index=False)
wide_path = "FILEPATH"
write_hosp_file(df_wide, wide_path, backup=True)


    



df = df[df.dx_1.notnull()]


df = stack_merger(df)

df['val'] = 1






print("Are there missing values in any row?\n" + str(not(df.isnull().sum()).all()==0))


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end','year_start',
           'year_end','location_id','nid','age_group_unit','source','facility_id',
          'code_system_id','outcome_id','representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val':'sum'}).reset_index()






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


assert len(hosp_frmat_feat) == len(df_agg.columns), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns, "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])


for i in df_agg.drop(['cause_code','source','facility_id','outcome_id'], axis=1, inplace=False).columns:
    
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)


assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),    "number of feature levels of years should match number of feature levels of nid"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),    "number of feature levels age start should match number of feature levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2, "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2, "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1, "source should only have one feature level"






write_path = root + r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)
