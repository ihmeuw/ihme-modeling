

"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Format Italy hospital discharge data
Note: this is a fairly large dataset (103 million rows, ~20 columns)
so you'll need to dedicate a large amount of memory to run this script

there are 12 entries not a series so here are the first and last years
URL
URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import os
import re
import getpass
import time

user = getpass.getuser()

prep_path = r"FILENAME"
sys.path.append(prep_path)

from hosp_prep import *
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"







def read_raw_data(write_data=False):
    """
    ITA data is stored by year in stata files that take awhile to read in.
    These were read in once, the desired columns were kept and data was
    written to the location above in /raw
    """
    years = np.arange(2005, 2017, 1)
    
    df_list = []
    sdict = {}
    for year in years:
        print("Starting on year {}".format(year))
        start = time.time()
        if year == 2007:
            print("2007 Stata is corrupted as of 2018-01-12")
            fpath = "FILENAME"\
                    "gbd 2005-2016 sas7bdat files/gbd_2007.sas7bdat"
            df = pd.read_sas(fpath)
        else:
            
            fpath = r"FILENAME"\
                    "FILENAME"\
                    "FILEPATH".format(year)
            df = pd.read_stata(fpath)
            read_time = (time.time()-start)/60
            print("It took {} min to read in year {}".format(read_time, year))
            
            sdict[year] = df.columns.tolist()

        try:
            
            ideal_keep = ['sesso', 'eta', 'eta_gg', 'reg_ric', 'regric', 'gg_deg',
                          'mod_dim', 'mot_dh', 'tiporic', 'dpr', 'dsec1',
                          'dsec2', 'dsec3', 'dsec4', 'dsec5', 'causa_ext',
                          'tip_ist2', 'data_ric', 'data_dim', 'data_ricA',
                          'data_dimA']
            to_keep = [n for n in ideal_keep if n in df.columns]
            print(("Missing {} for this year".
                  format([n for n in ideal_keep if n not in df.columns])))
            df = df[to_keep]
            df['year_start'] = year
            df_list.append(df)
        except:
            print("well that didn't work for {}".format(year))
        del df

    if write_data:
        df = pd.concat(df_list, ignore_index=True)

        df.loc[(df.data_ric.isnull()) & (df.data_ricA.notnull()), 'data_ric'] =\
            df.loc[(df.data_ric.isnull()) & (df.data_ricA.notnull()), 'data_ricA']
        df.loc[(df.data_dim.isnull()) & (df.data_dimA.notnull()), 'data_dim'] =\
            df.loc[(df.data_dim.isnull()) & (df.data_dimA.notnull()), 'data_dimA']

        df.drop(['data_ricA', 'data_dimA'], axis=1, inplace=True)

        df['gg_deg'] = pd.to_numeric(df.gg_deg, errors='raise')

        write_hosp_file(df, "FILEPATH")
    return df_list



















final_list = []

years = np.arange(2005, 2017, 1) 
for year in years:
    print("Starting on year {}".format(year))
    start = time.time()
    
    if year == 2007:
        print("2007 Stata is corrupted as of 2018-01-12")
        fpath = "FILENAME"\
                "gbd 2005-2016 sas7bdat files/gbd_2007.sas7bdat"
        df = pd.read_sas(fpath)
    else:
        
        fpath = r"FILENAME"\
                "FILENAME"\
                "FILEPATH".format(year)
        df = pd.read_stata(fpath)
        
        
        
        read_time = (time.time()-start)/60
        print("It took {} min to read in year {}".format(read_time, year))
        
        

    try:
        
        ideal_keep = ['sesso', 'eta', 'eta_gg', 'reg_ric', 'regric', 'gg_deg',
                      'mod_dim', 'mot_dh', 'tiporic', 'dpr', 'dsec1',
                      'dsec2', 'dsec3', 'dsec4', 'dsec5', 'causa_ext',
                      'tip_ist2', 'data_ric', 'data_dim', 'data_ricA',
                      'data_dimA', 'cod_reg']
        to_keep = [n for n in ideal_keep if n in df.columns]
        print(("Missing {} for this year".
              format([n for n in ideal_keep if n not in df.columns])))
        df = df[to_keep]
        df['year_start'] = year

    except:
        print("well that didn't work for {}".format(year))

    yr_start = time.time()
    print("Starting on year {}".format(year))

    if 'data_ricA' in df.columns:
        df.rename(columns={'data_ricA': 'data_ric'}, inplace=True)
    if 'data_dimA' in df.columns:
        df.rename(columns={'data_dimA': 'data_dim'}, inplace=True)
    

    df['gg_deg'] = pd.to_numeric(df.gg_deg, errors='raise')

    
    assert not df.data_dim.isnull().sum()
    assert not df.data_ric.isnull().sum()

    
    start_cases = df.shape[0]

    

    
    
    assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
        "the index has a length of " + str(len(df.index.unique())) +
        " while the DataFrame has " + str(df.shape[0]) + " rows" +
        "try this: df = df.reset_index(drop=True)")

    
    
    
    hosp_wide_feat = {
        'nid': 'nid',
        
        'representative_id': 'representative_id',
        'year_start': 'year_start',
        'year_end': 'year_end',
        'sesso': 'sex_id',
        'eta': 'age',
        'eta_gg' : 'age_days',
        'age_group_unit': 'age_group_unit',
        'code_system_id': 'code_system_id',

        'data_dim': 'dis_date',
        'data_ric': 'adm_date',
        'gg_deg': 'los',

        
        'mod_dim': 'outcome_id',
        'tip_ist2': 'facility_id',
        
        'dpr': 'dx_1',
        'dsec1': 'dx_2',
        'dsec2': 'dx_3',
        'dsec3': 'dx_4',
        'dsec4': 'dx_5',
        'dsec5': 'dx_6',
        'causa_ext': 'ecode_1',

        
        'tiporic': 'hosp_type',
        'reg_ric': 'hosp_scheme',
        'mot_dh': 'hosp_reason'}

    
    df.rename(columns=hosp_wide_feat, inplace=True)
    

    
    
    new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                           set(df.columns)))
    df = df.join(new_col_df)

    
    df.drop(['hosp_type', 'hosp_scheme', 'hosp_reason'], axis=1, inplace=True)

    assert start_cases == df.shape[0], "Some rows were lost or added"

    
    null_count = df.dx_1.isnull().sum()
    
    warnings.warn("Dropping rows with missing primary dx. {} rows will be dropped".\
        format(null_count))

    df = df[df.dx_1.notnull()]
    print("next up is empty string dx")

    blank_count = (df.dx_1 == "").sum()
    warnings.warn("Dropping rows with blank primary dx. {} rows will be dropped".\
        format(blank_count))
    df = df[df.dx_1 != ""]
    start_cases = df.shape[0]

    
    df.loc[df.age_days.isnull(), 'age_days'] = np.nan
    
    
    
    df.loc[df['ecode_1'] == "", 'ecode_1'] = np.nan

    if not df[df.year_start == year].ecode_1.isnull().all():
        df['dx_7'] = np.nan
        
        df.loc[df['ecode_1'].notnull(), 'dx_7'] = df.loc[df['ecode_1'].notnull(), 'dx_1']
        
        df.loc[df['ecode_1'].notnull(), 'dx_1'] = df.loc[df['ecode_1'].notnull(), 'ecode_1']
    
    df.drop('ecode_1', axis=1, inplace=True)

    
    
    
    
    

    

    
    
    
    
    
    
    
    
    
    
    
    
    
    

    df['representative_id'] = 1  

    
    
    df['age_group_unit'] = df.apply(lambda x: 1 if pd.isnull(x['age_days']) else 2, axis = 1)
    df['age'] = df.apply(lambda x: x['age'] if x['age'] != 0.0 else x['age_days'], axis = 1)    
    df.drop(columns=['age_days'], inplace=True)

    df['source'] = 'ITA_HID'

    
    cod_reg_dict = {'010': 'Piemonte', '020' : "Valle d'Aosta", '030' : 'Lombardia',
                    '041' : 'Provincia autonoma di Bolzano',
                    '042' : 'Provincia autonoma di Trento', '050' : 'Veneto', '060' : 'Friuli-Venezia Giulia',
                    '070' : 'Liguria', '080' : 'Emilia-Romagna', '090' : 'Toscana', '100' : 'Umbria',
                    '110' : 'Marche', '120' : 'Lazio', '130' : 'Abruzzo', '140' : 'Molise', '150' : 'Campania',
                    '160' : 'Puglia', '170' : 'Basilicata', '180' : 'Calabria', '190' : 'Sicilia', '200' : 'Sardegna'}

    loc_dict = {'Piemonte':	35494, "Valle d'Aosta": 35495, 'Liguria': 35496, 'Lombardia': 35497,
                'Provincia autonoma di Bolzano': 35498, 'Provincia autonoma di Trento': 35499,
                'Veneto': 35500, 'Friuli-Venezia Giulia': 35501, 'Emilia-Romagna': 35502, 'Toscana': 35503,
                'Umbria': 35504, 'Marche': 35505,'Lazio': 35506, 'Abruzzo': 35507,'Molise': 35508,
                'Campania': 35509, 'Puglia': 35510,'Basilicata': 35511, 'Calabria': 35512, 'Sicilia': 35513,
                'Sardegna': 35514}

    loc_df = pd.DataFrame({'cod_reg': list(cod_reg_dict.keys()), 'location_name': list(cod_reg_dict.values())}).merge(\
                pd.DataFrame({'location_name': list(loc_dict.keys()), 'location_id': list(loc_dict.values())}), how='outer', on='location_name')
    assert loc_df.shape[0] == 21, "Shape is off"
    assert loc_df.isnull().sum().sum() == 0, "Null should not be present"
    
    pre = df.shape[0]
    df = df.merge(loc_df, how='left', on='cod_reg')
    assert pre == df.shape[0]
    assert df.location_id.isnull().sum() == 0, "missing location IDs"
    df.drop(['cod_reg', 'location_name'], axis=1, inplace=True)

    
    df['code_system_id'] = 1

    
    
    
    df['outcome_id'].replace(["1"], ['death'], inplace=True)
    df.loc[df['outcome_id'] != 'death', 'outcome_id'] = 'discharge'

    
    df['metric_id'] = 1

    df['year_end'] = df['year_start']

    
    df.loc[df.sex_id == 'X', 'sex_id'] = 3

    
    
    

    
    df['facility_id'] = 'hospital'

    
    
    num_cols = ['sex_id']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')

    diag_cols = df.filter(regex="^dx_|^ecode_").columns

    
    
    
    

    
    for date_type in ['adm_date', 'dis_date']:
        df[date_type] = pd.to_datetime(df[date_type], errors='coerce')
        nulls = df[date_type].isnull().sum()
        if nulls > 0:
            warnings.warn("Dropping {} null date rows".format(nulls))
            df = df[df[date_type].notnull()]

    start_cases = df.shape[0]

    df['los2'] = df['dis_date'].subtract(df['adm_date'])
    df['los2'] = df['los2'].dt.days

    df[df.los != df.los2].shape

    assert start_cases == df.shape[0]
    
    
    print(("There are {} rows with los2 less than 0".
          format((df.los2 < 0).sum())))

    
    df = df[df.los2 > 0]
    end_cases = start_cases - df.shape[0]
    print(("{} cases were lost when dropping day cases. or {} ".
          format(end_cases, float(end_cases) / df.shape[0])))
    int_cases = df.shape[0]

    df.drop(['los', 'los2'], axis=1, inplace=True)

    
    nid_dictionary = {2005: 331137, 2006: 331138, 2007: 331139, 2008: 331140,
                      2009: 331141, 2010: 331142, 2011: 331143, 2012: 331144,
                      2013: 331145, 2014: 331146, 2015: 331147, 2016: 331148}
    df = fill_nid(df, nid_dictionary)

    
    
    
    

    
    

    
    
    
    
    
    
    
    
    

    
    
    
    

    
    
    

    
    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
                'sex_id', 'nid', 'representative_id', 'metric_id']

    
    
    
    
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
    
    
    
    unit_dict = {'days': 2, 'months': 3, 'years': 1}
    df = stage_hosp_prep.convert_age_units(df, unit_dict)
    df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)
    
    
    df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3


    
    for col in ['age', 'regric', 'dis_date', 'adm_date']:
        if col in df.columns:
            df.drop(col, axis=1, inplace=True)
    
    


    
    df_wide = df.copy()
    df_wide['metric_discharges'] = 1
    
    dxs = df_wide.filter(regex="^dx_").columns.tolist()
    for dxcol in dxs:
        df_wide[dxcol].fillna("", inplace=True)
        df_wide[dxcol] = sanitize_diagnoses(df_wide[dxcol])
    print("Missing values", df_wide.isnull().sum())
    assert (df_wide.isnull().sum() == 0).all()
    df_wide = df_wide.groupby(df_wide.columns.drop('metric_discharges').tolist()).agg({'metric_discharges': 'sum'}).reset_index()
    df_wide.to_stata(r"FILEPATH".format(year))
    write_path = r"FILEPATH".format(year)
    write_hosp_file(df_wide, write_path, backup=False)
    del df_wide

    
    
        
    
    pre_reshape_rows = df.shape[0]

    
    diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
    
    for feat in diagnosis_feats:
        df[feat] = sanitize_diagnoses(df[feat])

    cols = df.columns
    print(df.shape, "shape before reshape")
    if len(diagnosis_feats) > 1:
        
        stack_idx = [n for n in df.columns if "dx_" not in n]
        
        len_idx = len(stack_idx)

        df = df.set_index(stack_idx).stack().reset_index()

        

        
        pre_dx1 = df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
        df = df[df[0] != ""]
        diff = pre_dx1 - df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
        print("{} dx1 cases/rows were lost after dropping blanks".format(diff))

        df = df.rename(columns={"level_{}".format(len_idx): 'diagnosis_id', 0: 'cause_code'})

        df.loc[df['diagnosis_id'] != "dx_1", 'diagnosis_id'] = 2
        df.loc[df.diagnosis_id == "dx_1", 'diagnosis_id'] = 1

    elif len(diagnosis_feats) == 1:
        df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
        df['diagnosis_id'] = 1

    else:
        print("Something went wrong, there are no ICD code features")

    
    df['val'] = 1

    print(df.shape, "shape after reshape")

    assert abs(int_cases - df[df.diagnosis_id == 1].val.sum()) < 350
    chk = df.query("diagnosis_id == 1").groupby('source').agg({'diagnosis_id': 'sum'}).reset_index()
    assert (chk['diagnosis_id'] >= pre_reshape_rows * .999).all()
    
    
    

    
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
            "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

    
    for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                         axis=1, inplace=False).columns:
        
        
        assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

    
    assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
        "number of feature levels of years and nid should match number"
    assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
        "number of feature levels age start should match number of feature"\
        " levels age end"
    assert len(df_agg['diagnosis_id'].unique()) <= 2,\
        "diagnosis_id should have 2 or less feature levels"

    s = [1, 2, 3]
    check_sex = [n for n in df.sex_id.unique() if n not in s]
    assert len(check_sex) == 0, "There is an unexpected sex_id value"

    assert len(df_agg['code_system_id'].unique()) <= 2,\
        "code_system_id should have 2 or less feature levels"
    assert len(df_agg['source'].unique()) == 1,\
        "source should only have one feature level"

    assert (df.val >= 0).all(), ("for some reason there are negative case counts")

    assert abs(int_cases - df[df.diagnosis_id == 1].val.sum()) < 350
    chk = df.query("diagnosis_id == 1").groupby('source').agg({'diagnosis_id': 'sum'}).reset_index()
    assert (chk['diagnosis_id'] >= pre_reshape_rows * .999).all()

    final_list.append(df_agg)
    del df
    del df_agg
    yr_run = (time.time() - yr_start) / 60
    print("Done with {} in {} min".format(year, yr_run))




df_agg = pd.concat(final_list, ignore_index=True)

df_agg = df_agg[df_agg.cause_code != 'nan']
df_agg.info(memory_usage='deep')
print(df_agg.shape)




compare_df = pd.read_hdf(root + "FILENAME"
                         "FILENAME"
                         "FILEPATH")

test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg


write_path = root + r"FILENAME"\
    "FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
