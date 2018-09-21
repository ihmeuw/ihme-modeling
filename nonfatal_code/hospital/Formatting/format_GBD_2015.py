# -*- coding: utf-8 -*-
"""
Created on Wed Nov  23 11:13:00 2016

"""

# This script takes the hospital data used in gbd 2015 and reformats it to fit
# gbd 2016's requirements. Then it adds every new data source for 2016
# and saves the complied data to the J drive

import warnings
import pandas as pd
import datetime


# load hospital data from gbd 2015
def format_hospital(write_hdf=False):
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
    ###########################################
    # READ AND PREP 2015 GBD DATA
    ###########################################
    filepath = "FILEPATH"
    df = pd.read_stata(filepath)
    print("read all_hospital_epi")

    # add data for AZ and AR which was lost due to missing loc IDs
    recovered_filepath = (r"FILEPATH")
    recovered = pd.read_stata(recovered_filepath)

    # add data for USA NAMCS
    us_filepath = ("FILEPATH")

    us = pd.read_stata(us_filepath)

    # drop the USA NAMCS data from all_hospital_epi.dta because
    # it was double counted
    df = df[df['source'] != "USA_NAMCS"]

    # add data for NZL 2015
    nzl_filepath = ("FILEPATH")
    nzl = pd.read_stata(nzl_filepath)
    # drop the nzl data that counts day cases as inpatient cases
    df = df[df['source'] != "NZL_NMDS"]

    # append them together
    df = pd.concat([df, recovered, us, nzl])
    print("Concatinated all gbd 2015 data together")

    # Rename columns to match gbd 2016 requirements
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

    # Drop unneeded columns from the old data
    # df.drop(['year_gbd'], axis=1, inplace=True)
    df.drop(['subdiv', 'iso3', 'deaths'], axis=1, inplace=True)

    # change id from 0 to 3 for "not representative"
    df.loc[df['representative_id'] == 0, 'representative_id'] = 3

    # Clean the old data
    df['nid'] = df['nid'].astype(int)  # nid does not need to be a float
    assert len(df['code_system_id'].unique()) <= 2, ("We assume that there "
        "only 2 ICD formats present: ICD 9 and ICD 10")
    df['code_system_id'].replace(['ICD9_detail', 'ICD10'], [1, 2], inplace=True)
    # replace ICD9 with 1 and ICD10 with 2
    df['facility_id'].replace([1, 2],
                    ['inpatient unknown', 'outpatient unknown'], inplace=True)
    # replace 1 with 'inpatient unknown' and 2 with 'outpatient unknown

    # Add columns to the old data
    # h['diagnosis_id'] = 1  # all rows are primary diagnosis
    df['outcome_id'] = 'case'  # all rows are cases (discharges and deaths)
    df['metric_id'] = 1  # all rows are counts
    df['age_group_unit'] = 1  # all ages are in years
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    df.drop('year_id', axis=1, inplace=True)

    # drop where val is 0. in at lease a few cases, if not all
    # data was made to be square last year
    df = df[df['val'] > 0]

    # fix age group on nzl.  It says it's 0-1, but it's actually 0-4
    df.loc[(df.source == 'NZL_NMDS') & (df.age_end == 1), 'age_end'] = 4


    print("Done formating GBD 2015")

    ###########################################
    # READ NEW DATA SOURCES
    ###########################################
    def print_diff_cols(df2):
        print("These columns are different",
              set(df2.columns).symmetric_difference(set(df.columns)))
    print("Reading in new data")

    sources = ['KGZ_MHIF', 'IND_SNH', 'CHN_NHSIRS', 'TUR_DRGHID', 'CHL_MOH',
               'DEU_HSRS', 'ECU_INES_12_14', 'PRT_CAHS', 'ITA_IMCH',
               'PHL_HICC', 'NPL_HID', 'QAT_AIDA', 'KEN_IMMS', 'GEO_COL',
               'UK_HOSPITAL_STATISTICS', 'IDN_SIRS', 'VNM_MOH']

    loop_list = [df]
    for source in sources:
        filepath = (r"FILEPATH")
        new_df = pd.read_hdf(filepath, key="df")
        assert set(df.columns) == set(new_df.columns),\
            print_diff_cols(new_df)
        loop_list.append(new_df)
    df = pd.concat(loop_list)

    print("Done reading in new data")
    # reset index since we concated
    df.reset_index(drop=True, inplace=True)

    # England has only E codes and N codes, but they include day cases
    #  and we can't get rid of them.
    df = df[df.location_id != 4749]

    # in case a dataset has negative values
    df = df[df['val'] > 0]

    ###########################################
    # Format all data sets
    ###########################################

    # Reorder columns
    hosp_frmat_feat = ['location_id', 'year_start', 'year_end',
                       'age_group_unit', 'age_start', 'age_end', 'sex_id',
                       'source', 'nid', 'representative_id', 'facility_id',
                       'code_system_id', 'diagnosis_id', 'cause_code',
                       'outcome_id', 'metric_id', 'val']

    columns_before = df.columns
    df = df[hosp_frmat_feat]  # reorder
    columns_after = df.columns

    assert set(columns_before) == set(columns_after),\
        "You accidentally dropped a column while reordering"

    print("converting datatypes and downsizing...")
    # enforce some datatypes
    df['cause_code'] = df['cause_code'].astype(str)
    df['source'] = df['source'].astype(str)
    df['facility_id'] = df['facility_id'].astype(str)
    df['outcome_id'] = df['outcome_id'].astype(str)

    int_list = ['location_id', 'year_start', 'year_end',
    'age_group_unit', 'age_start', 'age_end', 'sex_id', 'nid',
    'representative_id', 'code_system_id', 'diagnosis_id',
    'metric_id']

    # downsize. this cuts size of data in half!
    for col in int_list:
        try:
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='raise')
        except:
            print(col, "<- this col didn't work")


    # Final check
    # check number of unique feature levels
    assert len(df['diagnosis_id'].unique()) <= 2,\
              "diagnosis_id should have 2 or fewer feature levels"
    assert len(df['code_system_id'].unique()) <= 2,\
              "code_system_id should have 2 or fewer feature levels"

    # assert that a single source doesn't have cases combined with
    # anything other than "unknown"
    for asource in df.source.unique():
        outcomes = df[df['source'] == asource].outcome_id.unique()
        if "case" in outcomes:
            assert "death" not in outcomes
            assert "discharge" not in outcomes


    if write_hdf == True:
        # Saving the file
        write_path = "FILEPATH"
        df.to_hdf(write_path, key='df', format='table',
                      complib='blosc', complevel=5, mode='w')

    return(df)

# if __name__ == '__main__':
#     df = format_hospital()
