
"""
Created on Wed Nov  23 11:13:00 2016

Compile all sources of Formatted data into one file.  This combines data from
GBD 2015 and 2016 into the same format.

@author: USERNAME and USERNAME
"""





import warnings
import pandas as pd
import datetime
import getpass
import sys
import time


if getpass.getuser() == 'USERNAME':
    USER_path = r"FILENAME"
    sys.path.append(USER_path)
if getpass.getuser() == 'USERNAME':
    USER_path = r"FILENAME"
    sys.path.append(USER_path)

import gbd_hosp_prep
import hosp_prep





def print_diff_cols(df, df2):
    print(("Set difference of columns: {}"
          .format(set(df2.columns).symmetric_difference(set(df.columns)))))


def format_hospital(write_hdf=False, downsize=False, verbose=True, head=False):
    """
    Function that reads all our formatted data and concatenates them together.

    Arguments:
        write_hdf: (bool) If true, writes an HDF H5 file of the aggregated data
        downsize: (bool) If true, numeric types will be cast down to the
            smallest size
        verbose: (bool) If true will print progress and information
        head: (bool) If true will only grab first 1000 rows of each source.
    """
    start = time.time()
    today = datetime.datetime.today().strftime("%Y_%m_%d")  
    
    
    
    
    
    print("reading gbd2015 hospital data...")

    
    
    
    
    

    
    us_filepath = (r"FILENAME"
                   r"FILEPATH")
    us = pd.read_stata(us_filepath)
    
    us.rename(columns={'dx_mapped_': 'cause_code', 'dx_ecode_id': 'diagnosis_id'}, inplace=True)

    
    
    
    
    
    

    
    
    
    
    
    

    loop_list = [us]
    stata_sources = ['NOR_NIPH_08_12', 'USA_HCUP_SID_03', 'USA_HCUP_SID_04',
                     'USA_HCUP_SID_05', 'USA_HCUP_SID_06', 'USA_HCUP_SID_07',
                     'USA_HCUP_SID_08', 'USA_HCUP_SID_09', 'USA_NHDS_79_10',
                     'BRA_SIH', 'MEX_SINAIS', 'NZL_NMDS', 'EUR_HMDB',
                     'SWE_PATIENT_REGISTRY_98_12']

    for source in stata_sources:
        if verbose:
            print(source)
        filepath = (r"FILEPATH"
                    r"FILEPATH").format(source, source)
        if head:
            new_df = hosp_prep.read_stata_chunks(filepath, chunksize=1000, chunks=1)
        else:
            new_df = pd.read_stata(filepath)
        if source == "EUR_HMDB":
            new_df.drop(['metric_bed_days', 'metric_day_cases'], axis=1, inplace=True)
        if verbose:
            print_diff_cols(new_df, us)

        
        
        
        
        loop_list.append(new_df)

    
    
    df = pd.concat(loop_list, ignore_index=True)

    
    df.loc[df.source.str.startswith("USA_HCUP_SID_"), "source"] = "USA_HCUP_SID"

    
    df.loc[df.age_end > 1, 'age_end'] = df.loc[df.age_end > 1, 'age_end'] + 1
    print("Concatinated all gbd 2015 data together in {} minutes".format((time.time()-start)/60))

    
    rename_dict = {
        'cases': 'val',
        'sex': 'sex_id',
        'NID': 'nid',
        'dx_ecode_id': "diagnosis_id",
        'dx_mapped_': "cause_code",
        'icd_vers': 'code_system_id',
        'platform': 'facility_id',
        'national': 'representative_id',
        'year': 'year_id'
    }
    df.rename(columns=rename_dict, inplace=True)

    
    
    df.drop(['subdiv', 'iso3', 'deaths'], axis=1, inplace=True)

    
    df.loc[df['representative_id'] == 0, 'representative_id'] = 3

    
    df['nid'] = df['nid'].astype(int)  
    assert len(df['code_system_id'].unique()) <= 2, ("We assume that there "
        "only 2 ICD formats present: ICD 9 and ICD 10")
    df['code_system_id'].replace(['ICD9_detail', 'ICD10'], [1, 2], inplace=True)
    
    df['facility_id'].replace([1, 2],
                              ['inpatient unknown',
                               'outpatient unknown'],
                              inplace=True)
    

    
    df['outcome_id'] = 'case'  
    df['metric_id'] = 1  
    df['age_group_unit'] = 1  
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    df.drop('year_id', axis=1, inplace=True)

    
    
    df = df[df['val'] > 0]

    
    df.loc[(df.source == 'NZL_NMDS') & (df.age_end == 1), 'age_end'] = 5

    print("Done formating GBD 2015 in {} minutes".format((time.time()-start)/60))

    
    
    

    print("Reading in new data")

    h5_sources = ['KGZ_MHIF', 'IND_SNH', 'CHN_NHSIRS', 'TUR_DRGHID', 'CHL_MOH',
                  'DEU_HSRS', 'PRT_CAHS', 'ITA_IMCH', 'PHL_HICC', 'NPL_HID',
                  'QAT_AIDA', 'KEN_IMMS', 'GEO_COL', 'UK_HOSPITAL_STATISTICS',
                  'IDN_SIRS', 'VNM_MOH', "JOR_ABHD",
                  
                  'AUT_HDD', 'ECU_INEC_97_14']

    loop_list = [df]
    for source in h5_sources:
        if verbose:
            print(source)
        filepath = (r"FILEPATH"
                    r"FILEPATH").format(source, source)
        if head:
            new_df = pd.read_hdf(filepath, key="df", start=0, stop=1000)
        else:
            new_df = pd.read_hdf(filepath, key="df")
        assert set(df.columns).symmetric_difference(set(new_df.columns)) == \
            {'deaths'} or set(df.columns).symmetric_difference(set(new_df.columns)) == set(), \
            print_diff_cols(new_df, df)
        
        
        loop_list.append(new_df)



    
    df = pd.concat(loop_list, ignore_index=True)
    del loop_list
    print("Done reading in new data in {} minutes".format((time.time()-start)/60))

    
    
    df = df.loc[(df.source != "GEO_COL")|(~df.year_start.isin([2012, 2013]))]

    
    
    df = df[df.location_id != 4749]  

    
    df = df[df['val'] > 0]

    
    
    

    
    df.loc[(df.sex_id != 1)&(df.sex_id != 2), 'sex_id'] = 3

    
    hosp_frmat_feat = ['location_id', 'year_start', 'year_end',
                       'age_group_unit', 'age_start', 'age_end', 'sex_id',
                       'source', 'nid', 'representative_id', 'facility_id',
                       'code_system_id', 'diagnosis_id', 'cause_code',
                       'outcome_id', 'metric_id', 'val']

    columns_before = df.columns
    df = df[hosp_frmat_feat]  
    columns_after = df.columns

    assert set(columns_before) == set(columns_after),\
        "You accidentally dropped a column while reordering"

    print("converting datatypes and downsizing...")
    
    if verbose:
        print(df.info(memory_usage='deep'))
    df['cause_code'] = df['cause_code'].astype('str')
    df['source'] = df['source'].astype('category')
    df['facility_id'] = df['facility_id'].astype('category')
    df['outcome_id'] = df['outcome_id'].astype('category')

    int_list = ['location_id', 'year_start', 'year_end',
                'age_group_unit', 'age_start', 'age_end', 'sex_id', 'nid',
                'representative_id', 'code_system_id', 'diagnosis_id',
                'metric_id']

    if downsize:
        
        for col in int_list:
            try:
                df[col] = pd.to_numeric(df[col], errors='raise')
            except:
                print(col, "<- this col didn't work")
    if verbose:
        print(df.info(memory_usage='deep'))

    
    
    bad_ends = df[(df.age_end==29)].source.unique()
    for asource in bad_ends:
        
        df.loc[(df.age_end > 1)  & (df.source == asource), 'age_end'] = df.loc[(df.age_end > 1)  & (df.source == asource), 'age_end'] + 1

    df.loc[df.age_start > 95, 'age_start'] = 95
    df.loc[df.age_start >= 95, 'age_end'] = 125
    df.loc[df.age_end == 100, 'age_end'] = 125

    
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    
    
    assert len(df['diagnosis_id'].unique()) <= 2,\
              "diagnosis_id should have 2 or fewer feature levels"
    assert len(df['code_system_id'].unique()) <= 2,\
              "code_system_id should have 2 or fewer feature levels"

    
    
    for asource in df.source.unique():
        outcomes = df[df['source'] == asource].outcome_id.unique()
        if "case" in outcomes:
            assert "death" not in outcomes
            assert "discharge" not in outcomes

    print("Done processing formatted files in {} minutes".format((time.time()-start)/60))
    if write_hdf == True:
        
        write_path = (r"FILENAME"
                      r"FILEPATH".format(today))
        category_cols = ['cause_code', 'source', 'facility_id', 'outcome_id']
        for col in category_cols:
            df[col] = df[col].astype(str)
        df.to_hdf(write_path, key='df', format='table',
                  complib='blosc', complevel=5, mode='w')

    return(df)
