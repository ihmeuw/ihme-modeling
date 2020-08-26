
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Format Indonesia Integrated Hospital Data 2013

URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass



user = getpass.getuser()
prep_path = "FILEPATH"  
sys.path.append(prep_path)

from hosp_prep import *
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





def check_sums(data, sexes=["male", "female"]):
    for sex in sexes:
        cols = data.columns[data.columns.str.contains("\d-" + sex)]
        data[sex + "_sums"] = dat[cols].sum(axis=1)
        data[sex + "_diff"] = data["total_" + sex] - data[sex + "_sums"]
        assert data[data[sex + "_diff"] > 0].shape[0] == 0,\
            "this one {} is bad {} column {}".format(sex, i, col)


def reshape_long(df):
    
    df = df.set_index(['cause_code', 'subnat']).stack().\
        reset_index()
    df.rename(columns={'level_2': 'age_sex', 0: 'val'}, inplace=True)

    
    df['age_start'], df['age_end'], df['sex_id'] = df['age_sex'].str.split("-", 2).str
    
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    
    df['age_start'], df['age_end'] = pd.to_numeric(df['age_start'], errors='raise'), pd.to_numeric(df['age_end'], errors='raise')
    
    df.drop(['age_sex'], axis=1, inplace=True)
    return(df)





filepath = root + r"FILEPATH"


no_inj = pd.read_excel(filepath, sheetname="Tables C.1.1-C.1.29")
inj = pd.read_excel(filepath, sheetname="Tables C.3.1-C.3.25")

df_list = [no_inj, inj]

res_list = []
for df in df_list:
    
    
    
    age_groups = ['0-6 hr', '7-28 hr', '29hr-<1 th', '1-5', '5-15', '15-25',
                  '25-45', '45-65', '65-125']
    age_sex_cols = []
    for age in age_groups:
        lk = age + "-male"
        pr = age + "-female"
        age_sex_cols.append(lk)
        age_sex_cols.append(pr)
    
    col_names = ['row_num', 'idn_disease_code', 'cause_code', 'disease_name'] +\
            age_sex_cols +\
            ['total_male', 'total_female', 'total_discharges', 'total_deaths']
    
    df.columns = col_names
    
    
    df['row_num'] = df['row_num'].astype(str)
    
    headers = df['row_num'][df['row_num'].str.startswith("TABEL")]
    
    
    
    provinces = []
    for i in np.arange(0, headers.size, 1):
        
        if i == headers.size-1:
            dat = df.iloc[headers.index[i]:df.shape[0], :].copy()
        else:
            dat = df.iloc[headers.index[i]:headers.index[i+1], :].copy()

        
        
        dat = dat.loc[dat.index[dat['row_num'] == "1"][0]:, :]
        
        dat = dat[dat['row_num'] != "TOTAL"]
        
        subnat = headers.iloc[i]
        
        subnat_start = subnat.find("PROVINSI") + 9  
        subnat_end = subnat.find("TAHUN") - 1 
        dat['subnat'] = subnat[subnat_start:subnat_end].lower()
        for col in age_sex_cols + ['total_male', 'total_female',
                                   'total_discharges', 'total_deaths']:
            
            
            

            
            dat['comma'] =\
                dat.loc[dat[col].astype(str).str.contains(","), col].str.len() -\
                dat.loc[dat[col].astype(str).str.contains(","), col].str.find(",")

            dat.loc[dat['comma'] == 3, col] = dat.loc[dat['comma'] == 3, col] + "0"
            dat.loc[dat['comma'] == 2, col] =\
                dat.loc[dat['comma'] == 2, col] + "00"
            dat.loc[dat['comma'] == 1, col] =\
                dat.loc[dat['comma'] == 1, col] + "000"

            
            dat[col] = dat[col].astype(str).str.replace("\W", "")
            dat[col] = pd.to_numeric(dat[col], errors='coerce')


        dat['0-1-male'] = dat['0-6 hr-male'].fillna(0) +\
            dat['7-28 hr-male'].fillna(0) +\
            dat['29hr-<1 th-male'].fillna(0)
        dat['0-1-female'] = dat['0-6 hr-female'].fillna(0) +\
            dat['7-28 hr-female'].fillna(0) +\
            dat['29hr-<1 th-female'].fillna(0)




        
        check_sums(dat)
        provinces.append(dat)

    df = pd.concat(provinces)
        
    name_dict = {'0-6 hr-male': '0-.01917808-male',  
                 '0-6 hr-female': '0-.01917808-female',
                 '7-28 hr-male': '.01917808-.07671233-male',  
                 '7-28 hr-female': '.01917808-.07671233-female',
                 '29hr-<1 th-male': '.07671233-1-male',
                 '29hr-<1 th-female': '.07671233-1-female'
                 }
    df.rename(columns=name_dict, inplace=True)
    df.drop(['0-1-female', '0-1-male'], axis=1, inplace=True)


    if df.shape[0] > 4500:
        
        df.loc[4518, 'total_discharges'] = df.loc[4518, 'total_male'] +\
            df.loc[4518, 'total_female']
    
    
    data = df.copy()
    data = data[data['total_discharges'].notnull()]
    assert (data["female_sums"] + data["male_sums"] ==
            data['total_discharges']).all()

    def get_case_sum(df):
        total = 0
        for col in df.filter(regex="^[0-9]|\.").columns:
            colsum = df[col].sum()
            total = total + colsum
        return(total)
    val_sum = get_case_sum(df)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    
    
    df.drop(['comma', 'male_sums', 'male_diff', 'female_sums', 'female_diff',
             'total_male', 'total_female', 'total_discharges', 'row_num',
             'idn_disease_code', 'disease_name', 'total_deaths'], axis=1,
             inplace=True)

    
    df = reshape_long(df)
    
    df = df[df['val'] != 0]

    
    assert val_sum == df.val.sum()

    
    df = df.reset_index(drop=True)
    assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
        "the index has a length of " + str(len(df.index.unique())) +
        " while the DataFrame has " + str(df.shape[0]) + " rows" +
        "try this: df = df.reset_index(drop=True)")

    
    
    
    hosp_wide_feat = {
        'nid': 'nid',
        
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
        
        'cause_code': 'dx_1'}

    
    df.rename(columns=hosp_wide_feat, inplace=True)

    
    
    new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                           set(df.columns)))
    df = df.join(new_col_df)

    
    
    
    
    

    

    
    
    
    
    
    
    
    
    
    
    
    
    
    

    df['representative_id'] = 1  
    df['location_parent_id'] = 11
    df['outcome_id'] = 'case'
    df['facility_id'] = 'hospital'
    df['year_start'], df['year_end'] = [2013, 2013]

    
    
    
    locs = pd.read_csv(root + r"FILEPATH")
    locs['location_name'] = locs['location_name'].str.lower()
    
    df.loc[df.subnat == "dki jakarta", 'subnat'] = "jakarta"
    df.loc[df.subnat == "di yogyakarta", 'subnat'] = "yogyakarta"
    df.loc[df.subnat == "kepulauan bangka belitung", 'subnat'] = "bangka belitung"
    df.rename(columns={'subnat': 'location_name'}, inplace=True)
    df = df.merge(locs, how='left', on='location_name')
    assert df.location_id.isnull().sum() == 0

    
    
    df['age_group_unit'] = 1
    df['source'] = 'IDN_SIRS'

    
    df['code_system_id'] = 4

    
    df['metric_id'] = 1
    df['nid'] = 206640

    
    
    

    
    
    

    
    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
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

    
    
    

    
    
    
    

    
    df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

    
    

    
    
    
    

    
    

    
    
    
    
    
    
    
    
    

    
    
    
    

    
    diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
    
    for feat in diagnosis_feats:
        df[feat] = sanitize_diagnoses(df[feat])

    pre_unique_codes = df['dx_1'].sort_values().unique()
    df['dx_1'] = df['dx_1'].astype(str)
    assert (pre_unique_codes == df['dx_1'].sort_values().unique()).all()
    
    
        
    

    if len(diagnosis_feats) > 1:
        
        
        df = stack_merger(df)

    elif len(diagnosis_feats) == 1:
        df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
        df['diagnosis_id'] = 1

    else:
        print("Something went wrong, there are no ICD code features")
    res_list.append(df)

df = pd.concat(res_list)



val_sum = df.val.sum()





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
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
assert round(val_sum, 3) == round(df_agg.val.sum(), 3),\
    "some cases were lost"





fpath = r"FILEPATH"
compare_df = pd.read_hdf(fpath)
test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg






write_path = root + r"FILENAME" +\
    r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)
