
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Formatting German data

GHDx link: URL

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

import pandas as pd
import platform
import numpy as np
import sys


USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
clust_path = r"FILENAME"
sys.path.append(USER_path)
sys.path.append(USER_path)
sys.path.append(clust_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





df = pd.read_excel(root + r"FILENAME"
                   r"DEU_HOSPITAL_DISCHARGE_2009_TABLES_BY_4_DIGIT_ICD10_"
                   r"Y2012M11D28.XLS",
                   sheetname="Geschlecht_Anzahl_09", header=1,
                   encoding="utf-8")






hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',

    'year_start': 'year_start',
    'year_end': 'year_end',
    'Geschlecht': 'sex_id',




    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'ICD-10-4': 'dx_1',



    
    "Insgesamt": "total_all_age",
    'unter 1 Jahr': "under 1 years",
    '95 und älter': "95 and older",
    "unbekannt": "unknown age"
    }


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)










df['representative_id'] = 3  
df['location_id'] = 81



df['source'] = 'DEU_HSRS'  



df['code_system_id'] = 2  


df['metric_id'] = 1


df['nid'] = 67132


df['year_start'] = 2009
df['year_end'] = 2009





df['outcome_id'] = 'discharge'


df['facility_id'] = "inpatient unknown"






df['sex_id'] = np.where(df['sex_id'] == "m", 1, 2)


int_cols = ['location_id', 'year_start', 'year_end',
            'sex_id', 'nid', 'representative_id',
            'metric_id', ]
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')  
for col in str_cols:
    df[col] = df[col].astype(str)



















df.rename(columns={"under 1 years": 'age_0-1',
                   '1-4': "age_1-5",
                   '5-9': "age_5-10",
                   '10-14': "age_10-15",
                   '15-19': "age_15-20",
                   '20-24': "age_20-25",
                   '25-29': "age_25-30",
                   '30-34': "age_30-35",
                   '35-39': "age_35-40",
                   '40-44': "age_40-45",
                   '45-49': "age_45-50",
                   '50-54': "age_50-55",
                   '55-59': "age_55-60",
                   '60-64': "age_60-65",
                   '65-69': "age_65-70",
                   '70-74': "age_70-75",
                   '75-79': "age_75-80",
                   '80-84': "age_80-85",
                   '85-89': "age_85-90",
                   '90-94': "age_90-95",
                   '95 and older': 'age_95-125',
                   'unknown age': 'age_0-125'}, inplace=True)


df['id'] = df.index


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


df = pd.wide_to_long(df, ['age_'], i='id', j='age')


df = df.reset_index()  




df.rename(columns={"age_": "val"}, inplace=True)  


df.drop('id', axis=1, inplace=True)


df['age_group_unit'] = 1





df.dropna(subset=['age'], inplace=True)  
df['age_start'], df['age_end'] = df['age'].str.split("-", 1).str  

df.drop('age', axis=1, inplace=True)  


df['age_start'] = pd.to_numeric(df['age_start'], errors='raise')
                                
df['age_end'] = pd.to_numeric(df['age_end'], errors='raise')
                             


df.dropna(subset=['val'], inplace=True)




    


if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")









print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.")
    print(df.isnull().sum())
else:
    print(">> No.")



group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()






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


assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])


for i in df.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
    
    assert df[i].dtype != object, "%s should not be of type object" % (i)


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







write_path = root + r"FILENAME" +\
    r"FILEPATH"
write_hosp_file(df, write_path, backup=True)
