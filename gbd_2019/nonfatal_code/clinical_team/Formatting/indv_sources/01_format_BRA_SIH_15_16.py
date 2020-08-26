


"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

URL
Just the years 2015 and 2016
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import time
import getpass


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)

from hosp_prep import *
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





start = time.time()
filepath = "FILEPATH"
df = pd.read_hdf(filepath, key='df')
print((time.time()-start)/60)



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


assert df['DIAG_SECUN'].unique().size == 1, "there are actualy diagnoses here"
df.drop('DIAG_SECUN', axis=1, inplace=True)




hosp_wide_feat = {
    'nid': 'nid',
    'MUNIC_RES': 'location_id',
    'representative_id': 'representative_id',
    'ANO_CMPT': 'year_start',
    'SEXO': 'sex_id',
    'IDADE': 'age',
    'COD_IDADE': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'MORTE': 'outcome_id',
    'facility_id': 'facility_id',

    
    'DT_SAIDA': 'dis_date',
    'DT_INTER': 'adm_date',
    
    'DIAG_PRINC': 'dx_1',
    'DIAGSEC1': 'dx_2',
    'DIAGSEC2': 'dx_3',
    'DIAGSEC3': 'dx_4',
    'DIAGSEC4': 'dx_5',
    'DIAGSEC5': 'dx_6',
    'DIAGSEC6': 'dx_7',
    'DIAGSEC7': 'dx_8'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 1  



loc_dict = {"12": 4750,  
            "27": 4751,  
            "13": 4752,  
            "16": 4753,  
            "29": 4754,  
            "23": 4755,  
            "53": 4756,  
            "32": 4757,  
            "52": 4758,  
            "21": 4759,  
            "31": 4760,  
            "50": 4761,  
            "51": 4762,  
            "15": 4763,  
            "25": 4764,  
            "41": 4765,  
            "26": 4766,  
            "20": 4766,  
            "22": 4767,  
            "33": 4768,  
            "24": 4769,  
            "11": 4770,  
            "14": 4771,  
            "43": 4772,  
            "42": 4773,  
            "28": 4774,  
            "35": 4775,  
            "17": 4776}  

df['to_map_id'] = df['location_id'].astype(str).str[0:2]


df['location_id'] = df['to_map_id'].map(loc_dict)

df.drop('to_map_id', axis=1, inplace=True)


df['year_end'] = df['year_start']



df['source'] = 'BRA_SIH'


df['code_system_id'] = 2


df['facility_id'] = 'inpatient unknown'


df['outcome_id'] = df['outcome_id'].replace([0, 1], ['discharge', 'death'])

df['sex_id'] = df['sex_id'].replace([3], [2])


df['metric_id'] = 1


nid_dictionary = {2015: 237832, 2016: 281543}
df = fill_nid(df, nid_dictionary)






















unit_dict = {'days': 2, 'months': 3, 'years': 4, 'centuries': 5}
df = stage_hosp_prep.convert_age_units(df, unit_dict)

def drop_day_cases(df, drop_date_cols=True):
    """
    drop individuals who we know stayed for less than 24 hours using the
    admission and discharge dates. This isn't perfect (it keeps someone who
    stays for 8 hours overnight) but it does remove the cases we know aren't
    full days
    """
    pre_drop = df.shape[0]

    df['dis_date'] = pd.to_datetime(df['dis_date'])
    df['adm_date'] = pd.to_datetime(df['adm_date'])

    
    df = df[df.dis_date != df.adm_date].copy()
    dropped = round(1 - (float(df.shape[0]) / pre_drop), 4) * 100
    print("{} percent of rows were dropped".format(dropped))

    
    los = df['dis_date'].subtract(df['adm_date'])
    assert los.min() == pd.Timedelta('1 days 00:00:00'),\
        "Minimum length of stay is less than a day, something wrong"
    if drop_date_cols:
        df.drop(['dis_date', 'adm_date'], axis=1, inplace=True)
    return df

df = drop_day_cases(df, drop_date_cols=True)






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
for col in str_cols:
    df[col] = df[col].astype(str)





df.loc[df['age'] > 95, 'age'] = 95  



df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)


df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3

start = time.time()


df.to_stata(r"FILEPATH")
write_path = r"FILEPATH"
write_hosp_file(df, write_path, backup=False)



    


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


print("This ran in {} min".format((time.time()-start) / 60))






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
    "%s is missing from the columns of the DataFrame"% (hosp_frmat_feat[i])


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






updated_locs = [4761, 4750, 4760]
test_df = df_agg[~df_agg.location_id.isin(updated_locs)]

compare_df = pd.read_hdf("FILEPATH")
compare_df = compare_df[~compare_df.location_id.isin(updated_locs)]

test_results = stage_hosp_prep.test_case_counts(test_df, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg






write_path = root + r"FILENAME"\
    "FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
