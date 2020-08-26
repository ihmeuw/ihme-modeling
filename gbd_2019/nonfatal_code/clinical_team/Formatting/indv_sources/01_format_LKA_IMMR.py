
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
import glob
import os
import re


USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
sys.path.append(USER_path)
sys.path.append(USER_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

pd.options.display.max_columns = 99


def fix_col_names(n):
    """
    clean up these column names
    """
    
    n = n.split("_")

    
    if "<" in "_".join(n):
        n[2] = "0_1"
    
    if "70+" in "_".join(n):
        n[2] = "70_124"
    
    if "FILENAME" in "_".join(n):
        n[2] = "0_124"
    
    if "-" in "_".join(n):
        n[2] = n[2].split("-")
        n[2] = "_".join(n[2])
        n[2] = n[2].replace(" ", "")

    
    n = "_".join(n)
    return n




data_dir = "FILEPATH"

files1 = glob.glob(data_dir + "FILEPATH")
files2 = glob.glob(data_dir + "*.xls")
files = files1 + files2
assert len(files) == 12

df = pd.read_excel(files[10], header=None)
for i in np.arange(0, 3, 1):
    
    df.loc[i] = df.loc[i].fillna(method='ffill')
    
    df.loc[i] = df.loc[i].fillna("^")


col_names = df.loc[0].str.cat(df.loc[1], sep='_').str.cat(df.loc[2], sep="_")
col_names[0:3] = ['number', 'immr', 'disease_name']

col_names = [fix_col_names(n) for n in col_names]


df_list = []  
exp_col_len = 35
for i in np.arange(0, len(files), 1):
    
    year = int(os.path.basename(files[i]).split("_")[1].split(".")[0])
    tmp = pd.read_excel(files[i], header=None)
    assert tmp.shape[1] == exp_col_len
    tmp.columns = col_names
    tmp['year_start'], tmp['year_end'] = [year, year]

    
    tmp = tmp.loc[3:]
    df_list.append(tmp)
    print(tmp.shape, files[i])


df = pd.concat(df_list, ignore_index=True)

df = df[df.number != "Grand Total"]


df = df[df.disease_name.notnull()]


df.drop(['number', 'immr'], inplace=True, axis=1)



run_sum = 0
count_cols = [x for x in col_names if "LIVE" in x or "DEATHS" in x]
for col in count_cols:
    df[col] = df[col].replace(".", "0")
    df[col] = pd.to_numeric(df[col], errors="raise")
    run_sum = run_sum + df[col].sum()



to_calc = [n for n in count_cols if "Total" not in n]
sum_cols = [n for n in count_cols if "Total" in n]

df['calc_total'] = round(df[to_calc].sum(axis=1))
df['data_total'] = round(df[sum_cols].sum(axis=1))

assert df[df.calc_total != df.data_total].shape[0] == 0


run_sum = run_sum - df[[n for n in df.columns if "Total" in n]].sum().sum()


to_drop = [n for n in df.columns if "Total" in n or "total" in n]
df.drop(to_drop, axis=1, inplace=True)

long_df = df.set_index(['disease_name', 'year_start', 'year_end']).\
                       stack().reset_index()
long_df.rename(columns={'level_3': 'out_sex_age',
                        0: 'val'}, inplace=True)


assert run_sum == long_df.val.sum()


long_df['outcome_id'], long_df['sex_id'],\
    long_df['age_start'], long_df['age_end'] =\
    long_df['out_sex_age'].str.split("_", 3).str

assert (long_df.outcome_id.unique() == ['LIVE DISCHARGES', 'DEATHS']).all()
long_df['outcome_id'] = long_df['outcome_id'].\
    replace(['DEATHS', 'LIVE DISCHARGES'], ['death', 'discharge'])

assert (long_df.sex_id.unique() == ['Male', 'Female']).all()
long_df['sex_id'] = long_df['sex_id'].replace(['Male', 'Female'], [1, 2])


find_unmappable_names_for_MAPMASTER = False
if find_unmappable_names_for_MAPMASTER:
    df['disease_name'] = df['disease_name'].astype(str)
    df['raw_disease_name'] = df['disease_name']

    df['disease_name'] = df['disease_name'].str.replace("\W", "")

    
    df['cc_list'] = df.disease_name.map(lambda x: re.split("([A-Z]\d+)", x))

    
    
    if sys.version[0] == '3':
        df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  
    if sys.version[0] == '2':
        df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  

    
    df['cc_list_len'] = df.cc_list.apply(len)

    to_m = df[(df.cc_list_len > 2) | (df.cc_list_len == 1)]
    to_m = to_m[['disease_name', 'raw_disease_name', 'cc_list']].\
        drop_duplicates('disease_name')
    to_m.sort_values('raw_disease_name', inplace=True)
    to_m.to_csv(r"FILEPATH", index=False)

    
    dat = df['cc_list'].apply(pd.Series)

    
    dat2 = dat[dat[2].isnull()][[0, 1]]
    dat2.columns = ['disease_name', 'cause_code']

    
    
    maps = pd.read_csv(r"FILEPATH")
    maps = maps[maps.code_system_id == 2]
    maps['cause_code'] = maps['cause_code'].str.upper()
    maps = maps[['cause_code', 'nonfatal_cause_name']].\
        drop_duplicates('cause_code')
    dat2 = dat2.merge(maps, how='left', on='cause_code')

    
    assert 0 == dat2[(dat2.nonfatal_cause_name.isnull()) &
                     (dat2.cause_code.notnull())].shape[0]



df = long_df.copy()
df['disease_name'] = df['disease_name'].astype(str)
df['raw_disease_name'] = df['disease_name']

df['disease_name'] = df['disease_name'].str.replace("\W", "")


df['cc_list'] = df.disease_name.map(lambda x: re.split("([A-Z]\d+)", x))



if sys.version[0] == '3':
    df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  
if sys.version[0] == '2':
    df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  


df['cc_list_len'] = df.cc_list.apply(len)

dat = df['cc_list'].apply(pd.Series)

df = pd.concat([df, dat[1]], axis=1)
df['dx_1'] = np.nan
df.loc[df.cc_list_len == 2, 'dx_1'] = df.loc[df.cc_list_len == 2, 1]

df.loc[df.cc_list_len != 2, 'dx_1'] =\
    df.loc[df.cc_list_len != 2, 'raw_disease_name']
assert df.dx_1.isnull().sum() == 0, "There are nulls in dx 1"
df.drop(['cc_list', 'raw_disease_name', 'disease_name', 'out_sex_age',
         'cc_list_len', 1],
        axis=1, inplace=True)







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
    'sex_id': 'sex_id',
    
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'dx_1': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 1  
df['location_id'] = 17


df['age_group_unit'] = 1
df['source'] = 'LKA_IMMR'


df['facility_id'] = 'hospital'


df['code_system_id'] = 2


df['metric_id'] = 1


nid_dictionary = {2015: 328684, 2014: 328682,
                  2013: 259989, 2012: 259990, 2011: 328685, 2010: 259991,
                  2009: 259992, 2008: 259993, 2007: 259994, 2006: 259995,
                  2005: 259996, 2004: 259997}
df = fill_nid(df, nid_dictionary)






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











df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3






    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]








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

assert (df.val >= 0).all(), ("for some reason there are negative case counts")






write_path = root + r"FILEPATH"
write_path = root + r"FILENAME"\
            r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
