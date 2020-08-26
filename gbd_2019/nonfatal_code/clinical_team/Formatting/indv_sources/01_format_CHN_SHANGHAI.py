
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

PLEASE put link to GHDx entry for the source here
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import pyodbc


USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
sys.path.append(USER_path)
sys.path.append(USER_path)

import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"






assert 'Microsoft Access Driver (*.mdb, *.accdb)' in\
    [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')],\
    ("LOOK OUT, YOUR MACHINE DOESN'T HAVE THE DRIVER "
     "'Microsoft Access Driver (*.mdb, *.accdb)'" + "\n\n"
     "If you see an empty list then you are running 64-bit Python and "
     "you need to install the 64-bit version of the 'ACE' driver. If you "
     "only see ['Microsoft Access Driver (*.mdb)'] and you need to work with "
     "an .accdb file then you need to install the 32-bit version of "
     "the 'ACE' driver." + "\n\n"
     "See URL
     "Microsoft-Access"
     " For more information.")

DRV = "{Microsoft Access Driver (*.mdb, *.accdb)}"

ACCDB_2013 = (root + r"FILEPATH"
              r"DATA TO IHME.mdb")  
con = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, ACCDB_2013))

query = ('"DB QUERY"
df = pd.read_sql("DB QUERY")
con.close()

src_name = 'CHN_SHANGHAI'



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


keep = ['sex', 'age', 'icd_code', 'jzcnt', 'duration_mean']
df = df[keep]




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex': 'sex_id',
    
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'icd_code': 'dx_1',
    'jzcnt': 'val'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

start_cases = df.val.sum()























df['representative_id'] = 1  


df['location_id'] = 514
df['year_start'], df['year_end'] = [2016, 2016]


df['age_group_unit'] = 1
df['source'] = src_name


df['code_system_id'] = 2


df['outcome_id'] = "case"



df['facility_id'] = "hospital"


df['metric_id'] = 1


nid_dictionary = {2016: -999}
df = hosp_prep.fill_nid(df, nid_dictionary)






























int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
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

assert start_cases == df.val.sum(), "case total changed"


df = df[df.duration_mean > 1]
df.drop('duration_mean', axis=1, inplace=True)
minus_day_cases = df.val.sum()

start_icds = df.dx_1.unique()


    


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

start_icds = set(hosp_prep.sanitize_diagnoses(pd.Series(start_icds)))
assert start_icds.symmetric_difference(set(df.cause_code.unique())) == set()





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

assert minus_day_cases == df_agg.val.sum()





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

assert (df.val >= 0).all(), ("for some reason there are negative case counts")




assert not df.apply(lambda x: x == -999).any().any(),\
    "Looks like the -999 placeholder is still in the data. It's not ready to"\
    " write yet"

write_path = root + r"FILENAME"\
    "FILEPATH".format(src_name, src_name)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
