
"""
Created on Mon Jan 23 17:32:28 2017

NOTE: we're calling this source MEX_SINAIS to match the existing source
in clinical data which is from the same reporting system

Format Mexico SAEH data from 2013 - 2015
Confirmed with data services that our SINAIS source is kind of an umbrella
reporting system and the more granular system providing inpatient data was
SAEH so we're good to add the add'l years
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
from getpass import getuser


prep_path = "FILEPATH".format(getuser())
sys.path.append(prep_path)

import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"






df_list = []

files = {2013: "FILEPATH",
         2014: "FILEPATH",
         2015: "FILEPATH"}

for year, file in files.items():
    tmp = pd.read_csv(file)
    tmp['year_start'] = year
    tmp['year_end'] = year
    
    if year in [2014, 2015]:
        tmp.drop("EDAD", axis=1, inplace=True)
        tmp.rename(columns={'EDAD1': 'EDAD'}, inplace=True)
    
    assert tmp.CEDOCVE.unique().size == 32, "Wrong number of locations"
    df_list.append(tmp)
    del tmp

df = pd.concat(df_list, sort=False, ignore_index=True)



mex_locs = pd.read_csv("FILEPATH")
mex_locs.rename(columns={'IDEDO': 'CEDOCVE'}, inplace=True)

mex_locs = mex_locs[mex_locs.location_id.notnull()]

pre = df.shape
df = df.merge(mex_locs[['CEDOCVE', 'location_id', 'DESCRIP']], how='left', on='CEDOCVE')
assert pre[0] == df.shape[0], "the merge changed the df shape"
assert df.location_id.isnull().sum() == 0, "missing loc IDs not expected"


hosp_wide_feat = {
    'nid': 'nid',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'SEXO': 'sex_id',
    'EDAD': 'age',
    'CVEEDAD': 'age_units',
    'DIAS_ESTA': 'los',
    
    
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    
    'code_system_id': 'code_system_id',
    'MOTEGRE': 'outcome_id',
    'CLUES': 'facility_id',
    
    'AFECPRIN4': 'dx_1'}  
    


df.rename(columns= hosp_wide_feat, inplace = True)



new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values())\
                                         - set(df.columns))) 
df = df.join(new_col_df)






df['representative_id'] = 3

"""  Mapping from MotEgreso to Spanish description (accents removed)
1   CURACIAN (Cured)
2   MEJORAA (Improved)
3   VOLUNTARIO (Voluntary discharge)
4   PASE A OTRO HOSPITAL (discharged to other hospital)
5   DEFUNCIAN  (Death)
6   OTRO MOTIVO  (Other Reason)
9   (N.E.) (NA)"""

df['outcome_id'] = np.where(df['outcome_id'] == 5, 'death', 'discharge')













df['age_group_unit'] = 1
df['source'] = 'MEX_SINAIS'



df['code_system_id'] = 2


df['metric_id'] = 1


nid_dict = {2004: 86953, 2005: 86954, 2006: 86955, 2007: 86956,
            2008: 86957, 2009: 86958, 2010: 94170, 2011: 94171,
            2012: 121282, 2013: 150449, 2014: 220205, 2015: 281773}
df = hosp_prep.fill_nid(df, nid_dict)






df.los.value_counts(dropna=False).head()  
df.los.isnull().sum()  


df = df[df['los'] > 0]
final_admits = len(df)


df['facility_id'] = 'hospital'




df = hosp_prep.age_binning(df, drop_age=True)



int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)


df.loc[~df['sex_id'].isin([1, 2]), 'sex_id'] = 3


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]



for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    
    df[feat] = df[feat].str.upper()



    


if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)
elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1
else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1






group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end','year_start',
           'year_end','location_id','nid','age_group_unit','source','facility_id',
          'code_system_id','outcome_id','representative_id', 'metric_id']


print("Are there missing values in any row?\n" + str(not(df[group_vars].isnull().sum().sum() == 0)))
df_agg = df.groupby(group_vars).agg({'val':'sum'}).reset_index()

del df  





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

assert len(df_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1, "source should only have one feature level"

assert df_agg[df_agg.diagnosis_id==1].val.sum() == final_admits, "The count of admits from the middle of formating should match primary admits in final data"





write_path = root + r"FILEPATH"
hosp_prep.write_hosp_file(df=df_agg, write_path=write_path, backup=True)
