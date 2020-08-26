
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
from db_queries import get_location_metadata
import getpass

user = getpass.getuser()


if platform.system() == "Linux":
    root = "FILENAME"
    hosp_path = r"FILEPATH".format(user)
else:
    root = "FILEPATH"
    hosp_path = r"FILEPATH".format(user)

sys.path.append(hosp_path)
import hosp_prep






filepath = root + r"FILEPATH"\
                  r"FILENAME"\
                  r"FILEPATH"
try:
    df = pd.read_excel(filepath)
except:
    print("The data has moved from FILEPATH


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
    'SEX': 'sex_id',
    
    'year': 'age',
    
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'alive': 'outcome_id',
    'facility_id': 'facility_id',
    
    'ICD-10': 'dx_1',
    
    

    'Admiss date': 'adm_date',
    'Date discharge': 'dis_date'
    }


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 3  


locs = get_location_metadata(location_set_id=9, gbd_round_id=5)
loc_id = locs.loc[locs.location_name == "Jordan", "location_id"]
loc_id = loc_id.tolist()[0]
df['location_id'] = loc_id
assert (df.location_id == 144).all(),\
    "loc id check failed"  


df['age_group_unit'] = 1
df['source'] = 'JOR_ABHD'


df['code_system_id'] = 2

df['year_start'] = 2016  
df['year_end'] = 2016


df['metric_id'] = 1


nid_dictionary = {2016: 317423}
df = hosp_prep.fill_nid(df, nid_dictionary)










df['los'] = df['dis_date'] - df['adm_date']




df = df[df['los'] > "0 days"]

df = df[df['los'] <= "365 days"]

non_day_cases = df.shape[0]

df['facility_id'] = "hospital"


df.drop(['los', 'Length of stay', 'dis_date', 'adm_date', 'month', 'day'],
        axis=1, inplace=True)

df['outcome_id'].replace(['alive', 'dead'],
                         ['discharge', 'death'], inplace=True)





















int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id', 'dx_1']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

assert df.dx_1.isnull().sum() == 0





df.loc[df['age'] > 95, 'age'] = 95  

df = hosp_prep.age_binning(df)


df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3



    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])



df.loc[df.dx_1.isnull(), 'dx_1'] = "cc_code"

if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1






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

assert (df_agg.val >= 0).all(),\
    ("for some reason there are negative case counts")
assert non_day_cases == df_agg.val.sum(),\
    "case sum before and after groupby don't match"






write_path = root + r"FILENAME"\
                    r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
