
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

NOTE: there are e-codes here but they're not in the primary dx position. The data is stored
long by dx so it's kind of tricky. Could try using id_cols by uncommenting code below but
they don't seem to uniquely identify a person, ie thousands of rows with multiple ages/sexes

NOTE2: there's also outpatient data for this source which we could prep and review if we have
an acceptable outpatient envelope

Formatting Botswana inpatient and outpatient data

URL

A LOT MORE WORK TO BE DONE. ADDITIONAL YEARS TO ADD. REVIEW HOW PATIENTS ARE RECORDED WHEN STAYING LONGER THAN 1 MONTH
SOURCE FILES HERE: FILEPATH
THE STORAGE STRUCTURE ISN'T UNIFORM OR INTUITIVE, BASICALLY YOU NEED TO LOOK AROUND THROUGH EACH FOLDER
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

src_name = 'BWA_HMDS'
    
paths = {
    "IG_2007_MORB": "FILEPATH",
    "IG_2007_MORT": "FILEPATH",
    "IG_2008_MORB": "FILEPATH",
    "IG_2008_MORT": "FILEPATH",
    "IG_2009_MORB": "FILEPATH",
    "IG_2009_MORT": "FILEPATH",
}


data = {k: pd.read_csv(paths[k], dtype=str) for k in paths}

def rename_and_drop(df, key, proc_year):
    df = df[key.keys()]
    df['year'] = proc_year
    return df.rename(columns=key)




data["IG_2007_MORB"] = rename_and_drop(data["IG_2007_MORB"], {
    'AGE': 'age',
    'DISCHCON': 'outcome_id',
    'HFACTY': 'facility_id',
    'MORBDIAG': 'dx_1',
    'MORTYPE': 'diagnosis_id',
    'SEXOFPAT': 'sex_id',
    'YYOFADM': 'adm_year',
    'MMOFADM': 'adm_month',
    'DDOFADM': 'adm_day',
    'DISCHADD': 'dis_day',
    'DISCHAMM': 'dis_month',
    'DISCHAYY': 'dis_year'},
                                       proc_year=2007
)
data["IG_2007_MORB"]['nid'] = 126516




data["IG_2007_MORT"] = rename_and_drop(data["IG_2007_MORT"], {
    'AGE': 'age',
    'DISCHCON': 'outcome_id',
    'HFACTY': 'facility_id',
    'MORTDIAG': 'dx_1',
    'MORTTYPE': 'diagnosis_id',
    'SEXOFPAT': 'sex_id',
    'YYOFADM': 'adm_year',
    'MMOFADM': 'adm_month',
    'DDOFADM': 'adm_day',
    'DISCHADD': 'dis_day',
    'DISCHAMM': 'dis_month',
    'DISCHAYY': 'dis_year'},
                                       proc_year=2007
)
data["IG_2007_MORT"]['nid'] = 126516




data["IG_2008_MORB"] = rename_and_drop(data["IG_2008_MORB"], {
    'AGE': 'age',
    'DIS_CON': 'outcome_id',
    'H_FAC_TY': 'facility_id',
    'MORBDIAG': 'dx_1',
    'MORB_TY': 'diagnosis_id',
    'P_SEX': 'sex_id',
    'Y_ADM': 'adm_year',
    'M_ADM': 'adm_month',
    'D_ADM': 'adm_day',
    'D_DIS': 'dis_day',
    'M_DIS': 'dis_month',
    'Y_DIS': 'dis_year'},
                                       proc_year=2008
)
data["IG_2008_MORB"]['nid'] = 126517




data["IG_2008_MORT"] = rename_and_drop(data["IG_2008_MORT"], {
    'AGE': 'age',
    'DIS_CON': 'outcome_id',
    'H_FAC_TY': 'facility_id',
    'MORTDIAG': 'dx_1',
    'MORT_TY': 'diagnosis_id',
    'P_SEX': 'sex_id',
    'Y_ADM': 'adm_year',
    'M_ADM': 'adm_month',
    'D_ADM': 'adm_day',
    'D_DIS': 'dis_day',
    'M_DIS': 'dis_month',
    'Y_DIS': 'dis_year'},
                                       proc_year=2008
)
data["IG_2008_MORT"]['nid'] = 126517




data["IG_2009_MORB"] = rename_and_drop(data["IG_2009_MORB"], {
    'AGE': 'age',
    'DISCHCON': 'outcome_id',
    'FACI_TY': 'facility_id',
    'MORBDIAG': 'dx_1',
    'TYPE_MOR': 'diagnosis_id',
    'SEX': 'sex_id',
    'YYOFADM': 'adm_year',
    'MMOFADM': 'adm_month',
    'DDOFADM': 'adm_day',
    'DISCHADD': 'dis_day',
    'DISCHAMM': 'dis_month',
    'DISCHAYY': 'dis_year'},
                                       proc_year=2009
)
data["IG_2009_MORB"]['nid'] = 126518




data["IG_2009_MORT"] = rename_and_drop(data["IG_2009_MORT"], {
    'AGE': 'age',
    'DISCHCON': 'outcome_id',
    'FACI_TY': 'facility_id',
    'MORTDIAG': 'dx_1',
    'TYP_MORT': 'diagnosis_id',
    'SEX': 'sex_id',
    'YYOFADM': 'adm_year',
    'MMOFADM': 'adm_month',
    'DDOFADM': 'adm_day',
    'DISCHADD': 'dis_day',
    'DISCHAMM': 'dis_month',
    'DISCHAYY': 'dis_year'},
                                       proc_year=2009
)
data["IG_2009_MORT"]['nid'] = 126518

df = pd.concat(data.values(), sort=False)

begin_admits = df[df.diagnosis_id==1].shape[0]





df = df[df.dx_1.notnull()]  

warnings.warn("We're not swapping ecodes yet, waiting on inj team to see if it's useful")

df.loc[(df['diagnosis_id'].isin(['3', '9'])) | (df.diagnosis_id.isnull()), 'diagnosis_id'] = '2'

"""
Facility values ['hospital','hospital','emergency']
"""
df['facility_id'] = 'hospital'


df.loc[df.outcome_id.isnull(), 'outcome_id'] = '9'





df.outcome_id.replace({
    '1': 'discharge',
    '2': 'death',
    '3': 'discharge',
    '9': 'discharge'
}, inplace=True)





df.sex_id.replace("9", "3", inplace=True)
df.loc[df.sex_id.isnull(), 'sex_id'] = "3"


df.loc[df['dis_year'].isnull(), 'dis_year'] = df.loc[df['dis_year'].isnull(), 'year'].astype(str)

df['raw_dis'] = df['dis_year'] + '-' + df['dis_month'] + '-' + df['dis_day']
df['raw_admit'] = df['adm_year'] + '-' + df['adm_month'] + '-' + df['adm_day']



for col in ['raw_dis', 'raw_admit']:
    df.loc[df[col] == '2007-11-31', col] = '2007-11-30'
    df.loc[df[col] == '2007-4-31', col] = '2007-4-30'
    df.loc[df[col] == '2007-9-31', col] = '2007-9-30'
    df.loc[df[col] == '2007-6-31', col] = '2007-6-30'
    df.loc[(df[col] == '2007-2-30') | (df[col] == '2007-2-31') | (df[col] == '2007-2-29'), col] = '2007-2-28'
    df.loc[df[col] == '2006-2-31', col] = '2006-2-28'

pre = len(df)
df = df[df['raw_admit'].notnull()]
df = df[df['raw_dis'].notnull()]
print("{} rows lost".format(pre-len(df)))


df.loc[df.adm_year == '9999', 'raw_admit'] = '2008-1-1'

df['dis_date'] = pd.to_datetime(df['raw_dis'])
df['adm_date'] = pd.to_datetime(df['raw_admit'])
df['los'] = df['dis_date'] - df['adm_date']


df = df[df.los != '0 days']




n_rows = len(df)
warnings.warn("dropping data before 2004 and after 2009")
df['adm_year'] = df['adm_year'].astype(int)
df = df[df.adm_year >= 2004]  

print("Dropping {} rows of very long term care".format(n_rows - len(df)))

df['year_start'] = df.year
df['year_end']   = df.year

final_admits_check = len(df)


keep = ['sex_id', 'age', 'dx_1', 'year_start', 'year_end',
        'outcome_id', 'diagnosis_id', 'facility_id', 'nid']
df = df[keep]




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'dx_1': 'dx_1',
    
    
}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 0  
df['location_id'] = 193


df['age_group_unit'] = 1
df['source'] = src_name


df['code_system_id'] = 2





df['metric_id'] = 1








































df['age'] = pd.to_numeric(df['age'])
df.loc[df['age'] > 95, 'age'] = 95  

df = hosp_prep.age_binning(df, drop_age=True)


int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id', 'diagnosis_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3



    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1





assert df.val.sum() == final_admits_check, "There are {} missing admissions".format(final_admits_check - df.val.sum())


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']


print("Are there missing values in any row?\n")
null_condition = df[group_vars].isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

del df





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
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df_agg.val >= 0).all(), ("for some reason there are negative case counts")

assert df_agg.val.sum() == final_admits_check, "There are {} missing admissions".format(final_admits_check - df_agg.val.sum())






write_path = root + r"FILEPATH"\
    "FILEPATH".format(src_name, src_name)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)

























