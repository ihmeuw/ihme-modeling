





"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting Italy hospital data.  For the time being data is in
incoming data.  It doesn't have a GHDx entry or a NID yet.

Containing folder: FILEPATH

Codebook (in english): containing folder + RICOVERI_DIA_INT_legend.xlsx

Hospital data for the Friuli Venezia Giulia region received from collaborator, covering 2010 to 2015. Included in GBD 2016.

Italy - Friuli Venezia Giulia Hospital Inpatient Data.  We asked Jamie Hancock about
where the data came from, and he said:
"It's not 100% clear, but we got them from a collaborator
who works at this organization: Institute for Maternal and Child
Health – IRCCS “Burlo Garofolo"
So this institute is who provided these data, but not sure if it also collects
the data for that region of Italy, or it just got passed to them.
These are details not always provided by our collaborators, unfortunately"









GHDx entries:
URL
OR
URL

"""
import pandas as pd
import platform
import numpy as np
import sys
import glob
import re
















hosp_path = r"FILEPATH"
sys.path.append(hosp_path)


USER_path = r"FILENAME"
sys.path.append(USER_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"










file_dir =  glob.glob(root + r"FILENAME"
                      r"FILEPATH")

df_list = []
for f in file_dir:
    adf = pd.read_excel(f)
    adf['year_start'] = int(re.search(r"[0-9]+", f).group(0))
    df_list.append(adf)
df = pd.concat(df_list, ignore_index=True)
df['year_end'] = df['year_start']


keep = ['ANA_SESSO', 'DATA_DECESSO', 'ETA_DATA_INGRESSO',
        'DIA1', 'DIA2', 'DIA3', 'DIA4', 'DIA5', 'DIA6', 'year_start',
        'year_end', 'RICSDO_GIORNI_DEGENZA']
df = df[keep]














hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',

    'year_start': 'year_start',
    'year_end': 'year_end',
    'ANA_SESSO': 'sex_id',



    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',

    
    'DIA1': 'dx_1',
    'DIA2': 'dx_2',
    'DIA3': 'dx_3',
    'DIA4': 'dx_4',
    'DIA5': 'dx_5',
    'DIA6': 'dx_6',
    
    'ETA_DATA_INGRESSO': 'age',

    'DATA_DECESSO': 'DOD',
    'RICSDO_GIORNI_DEGENZA': 'bed_days'


}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)









df['representative_id'] = 3  
df['location_id'] = 86  


df['age_group_unit'] = 1
df['source'] = 'ITA_IMCH'  


df['code_system_id'] = 1  


df['metric_id'] = 1










nid_dictionary = {
2010: 285460,
2011: 285464,
2012: 285465,
2013: 285467,
2014: 285468,
2015: 285471}


df = fill_nid(df, nid_dictionary)

df['facility_id'] = 'inpatient unknown'








df['DOD'] = df['DOD'].astype(str)






df.loc[df['DOD'] == "9999-12-31 00:00:00", 'outcome_id'] = 'discharge'
df.loc[df['DOD'] != "9999-12-31 00:00:00", 'outcome_id'] = 'death'



tmp = df[df.bed_days == 0].copy()
tmp = tmp[tmp.outcome_id != "death"]
non_death_day_cases = tmp.shape[0]
del tmp



pre = df.shape[0]
df = df[(df.bed_days != 0) | (df.outcome_id != "discharge")]  
assert pre - df.shape[0] == non_death_day_cases,\
    "The wrong number of cases were dropped"
assert df[df.bed_days == 0].outcome_id.unique()[0] == 'death',\
    "There are day cases that didn't result in death present"
assert df[df.bed_days == 0].outcome_id.unique().size == 1,\
    "Too many unique outcome id values for day cases"
df.drop(['DOD', 'bed_days'], axis=1, inplace=True)








df['sex_id'] = np.where(df['sex_id'] == "M", 1, 2)


int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']


str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)




































df = df[df['age'] < 125]  
df.loc[df['age'] > 95, 'age'] = 95  

df = age_binning(df)
df['age_start'] = pd.to_numeric(df['age_start'], downcast='integer', errors='raise')
df['age_end'] = pd.to_numeric(df['age_end'], downcast='integer', errors='raise')
df.drop('age', axis=1, inplace=True)  



















df = df[['sex_id', 'dx_1', 'dx_2', 'dx_3', 'dx_4', 'dx_5', 'dx_6',
         'year_start', 'year_end', 'representative_id', 'nid',
         'code_system_id', 'facility_id', 'age_group_unit', 'outcome_id',
         'location_id', 'source', 'metric_id', 'age_start', 'age_end']]









diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])






    


if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1





df['cause_code'] = df['cause_code'].astype(str)









print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
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


assert set(columns_before) == set(columns_after),    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df.columns,        "%s is missing from the columns of the DataFrame"        % (hosp_frmat_feat[i])


for i in df.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
    
    assert df[i].dtype != object, "%s should not be of type object" % (i)


assert len(df['year_start'].unique()) == len(df['nid'].unique()),    "number of feature levels of years and nid should match number"
assert len(df['age_start'].unique()) == len(df['age_end'].unique()),    "number of feature levels age start should match number of feature " +    r"levels age end"
assert len(df['diagnosis_id'].unique()) <= 2,    "diagnosis_id should have 2 or less feature levels"
assert len(df['sex_id'].unique()) == 2,    "There should only be two feature levels to sex_id"
assert len(df['code_system_id'].unique()) <= 2,    "code_system_id should have 2 or less feature levels"
assert len(df['source'].unique()) == 1,    "source should only have one feature level"














write_path = root + r"FILEPATH"
write_hosp_file(df, write_path, backup=True)
