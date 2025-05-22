# --------------------------------------------------------------------------------

'''
Description: Drops redundant data according to defined rules 
Arguments: KeepBest.py <acause> <staging_name> 
    acause (str) : cancer cause 
    staging_name (str) : string denoting staging pipeline 
            possible values: {'mi_ratio',
                              'cod_mortality',
                              'nonfatal_skin'}
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''

# import libraries 
import os 
import pandas as pd 
import numpy as np
import sys 
from cancer_estimation._database import cdb_utils as cdb
import db_queries
import getpass
import cancer_estimation.py_utils.common_utils as utils


# --------------------------------------------------------------------------------
 
class WriteDataState():
    mort_pipeline_output_dir = "FILEPATH"
    output_of_mort_kb_fp = f"{mort_pipeline_output_dir}FILEPATH"
    input_to_mort_kb_fp = f"{mort_pipeline_output_dir}FILEPATH"
    nid_cols = ['NID', 'nid', 'underlying_nid', 'registry_index', 'registry_id']

    def __init__(self):
        self.username = getpass.getuser()
        self.files_dir = f"FILEPATH"
        utils.ensure_dir(self.files_dir)
        self.counter = 1

    def write_data(self, df, filename = "KeepBest", tag = ""):
        num_as_str = "{:03d}".format(self.counter)
        tag = "_" + tag if tag != "" else tag
        df.to_csv(f"{self.files_dir}{filename}_{num_as_str}{tag}.csv")
        self.counter += 1

    def output_filepath(self):
        return self.files_dir

    def nid_report(self, df, tag = ""):
        report = f"Waypoint: {tag}\n"
        report += f"The dataframe has {len(df)} rows\n"
        for col in WriteDataState.nid_cols:
            if col in df.columns:
                count = len(df[df[col] == 0])
                report += f"{col} has {count} 0's\n"
                count = len(df[df[col].isna()])
                report += f"{col} has {count} NA's\n"
                count = len(df[df[col] == '.'])
                report += f"{col} has {count} .'s\n"
            else:
                report += f"{col} not found in dataframe's columns\n"
        return report




def define_uids(): 
    ''' uids to check for redundancies throughout script 
    '''
    uid_cols = ['registry_index','year_id','sex_id','age'] 
    return(uid_cols)


def define_metric(staging_name):
    ''' given a staging process, return the appropriate metric values 
    '''
    if (staging_name in ['mi_ratio']): 
        metric = ['cases','deaths']
    elif (staging_name in ['cod_mortality','nonfatal_skin','nonfatal']): 
        metric = ['cases','pop']
    return(metric)


def gen_keep_cols(staging_name, mi_mortality):
    ''' defines columns outside of uids that are important to keep
    '''
    uid_cols = define_uids() 
    metric_cols = define_metric(staging_name)
    other_cols = ['dataset_name','year_start','year_end','acause','registry_id','age_range_type_id']
    if (mi_mortality == "INC_MOR"): 
        other_cols += ['dataset_name_INC','dataset_name_MOR', 'NID', 'NID_inc','NID_mor','dataset_id']
    return(uid_cols + other_cols + metric_cols)


def merge_age_type(df, mi_mortality, data_type): 
    ''' Takes a dataframe and merge age demographic information (i.e all_ages vs. pediatric)
    '''
    can_db = cdb.db_api()
    ds_table = can_db.get_table('dataset')[['dataset_name','age_range_type_id']]
    if mi_mortality == "INC_MOR":
        if data_type == "CR":
            ds_table = ds_table.assign(dataset_name_INC=ds_table['dataset_name'], 
                            dataset_name_MOR=ds_table['dataset_name'])
            ds_table = ds_table.assign(age_range_inc=ds_table['age_range_type_id'], 
                            age_range_mor=ds_table['age_range_type_id'])
            final = df.merge(ds_table[['dataset_name_INC', 'age_range_inc']], 
                                how='left', on='dataset_name_INC', indicator=True)
            assert (final['_merge'].eq('both').all()), 'unmerged age ranges!!'
            del final['_merge']

            final = final.merge(ds_table[['dataset_name_MOR', 'age_range_mor']], 
                                how='left', on='dataset_name_MOR', indicator=True)
            # only keep matches between data of same age range type
            final['age_type_mor'] = np.where(final['age_range_mor'].isin([1, 2, 4]), 'adult', 'peds')
            final['age_type_inc'] = np.where(final['age_range_inc'].isin([1, 2, 4]), 'adult', 'peds')
            final = final.loc[final['age_type_mor'] == final['age_type_inc']]

            final['age_range_type_id'] = final['age_range_mor'].copy()
            final = final.drop(labels=['age_range_mor', 'age_range_inc'], axis = 1)
        elif data_type == "VR":
            # since all MOR data will be VR, just have to look at INC data col
            ds_table = ds_table.assign(dataset_name_INC=ds_table['dataset_name'], 
                            age_range_inc=ds_table['age_range_type_id'])
            final = df.merge(ds_table[['dataset_name_INC', 'age_range_inc']], 
                            how='left', on='dataset_name_INC', indicator=True)
            final['age_range_type_id'] = final['age_range_inc'].copy()
            final = final.drop(labels = ['age_range_inc'], axis = 1)

    else:
        final = df.merge(ds_table, how='left', on='dataset_name', indicator=True)
    # check merge result: 
    assert (final['_merge'].eq('both').all()), 'unmerged age ranges!!'
    del final['_merge'] 
    return(final)


def gen_preferred_ds_list(staging_name, age_type, mi_mortality): 
    ''' Creates a dataframe of preferred datasets with rankings 
    '''
    if ((staging_name in ['cod_mortality','nonfatal_skin']) & (age_type == 'all_ages')):
        d = {'preferred_dataset_name' : ['usa_seer_1973_2008_inc', 
                                'USA_SEER',
                                'USA_Q472_MIS',
                                'SWE_NCR_1990_2010', 
                                'IND_PBCR_2009_2011_inc', 
                                'BRA_SPCR_2011',
                                'JPN_NationalCI5_1958_2013',
                                'CHE_Q790'],   
             'priority_rank' : [1,2,3,4,5,6,7,8]
             }
        df = pd.DataFrame(data=d)
    elif ((staging_name in ['mi_ratio']) & (age_type == 'all_ages')): 
        d = {'preferred_dataset_name' : ['USA_Q791_M_I',
                                'usa_seer_1973_2013_inc',
                                'USA_SEER', 
                                'USA_Q472_MIS',
                                'SWE_NCR_1990_2010',
                                'CHE_Q790',
                                'NZL_Q554_I',
                                'NORDCAN_1980_2014',
                                'NORDCAN',
                                'aut_2007_2008_inc',
                                'EUREG_GBD2016',
                                'BRA_SPCR_2011',
                                'NZL_Q752_M_I',
                                'NZL_Q284_M_I',
                                'GBR_Scotland_1989_2014',
                                'GBR_Scotland',
                                'CoD_VR_ICD10'], 
             'priority_rank': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]
             }
        df = pd.DataFrame(data=d)
        if (mi_mortality == "INC_MOR"): 
            d_peds = {'preferred_dataset_name' : ['IICC_Q405_I',
                                    'IICC_Q406_I',
                                    'NAM_Q392_I',
                                    'COL_Q426_P',
                                    'TWN_Q350_I',
                                    'twn_1980_2007_inc',
                                    'EST_Q333_I'], 
            'priority_rank' : [1,2,3,4,5,6,7]}
            dpeds = pd.DataFrame(data=d_peds) 
            df = pd.concat([df, dpeds])
    elif (age_type == 'pediatric'): 
        d = {'preferred_dataset_name' : ['IICC_Q405_I',
                                    'IICC_Q406_I',
                                    'NAM_Q392_I',
                                    'COL_Q426_P',
                                    'TWN_Q350_I'], 
        'priority_rank' : [1,2,3,4,5]}
        df = pd.DataFrame(data=d)
    return(df) 


def count_datasets(input_df): 
    ''' counts the number of disinct datasets for a given uid, and attaches counts to 
        dataframe 
    '''
    uid_cols = define_uids() 
    # df_count = input_df.groupby(uid_cols, as_index=False).size().reset_index(names='num_datasets')
    df_count = input_df.groupby(uid_cols, as_index=False).size()
    current_column_names = df_count.columns.tolist()
    current_column_names[-1] = 'num_datasets'
    df_count.columns = current_column_names
    final_counts = pd.merge(input_df, df_count, on=uid_cols, how='left')

    assert (len(input_df) == len(final_counts)), 'data points dropped'
    return(final_counts)


def gen_gbd_round_map(): 
    ''' attaches information on when dataset was added 
    '''
    can_db = cdb.db_api()
    ds_info = can_db.get_table('dataset_info')[['dataset_id', 'dataset_name', 'gbd_round_added']]
    return(ds_info)


def gen_ci5_pref(staging_name, age_type): 
    ''' returns a dataframe ranking CI5 datasets 
    '''
    if ((staging_name == "mi_ratio") & 
        (age_type == 'all_ages')): 
        d = {'dataset_name' : ['CI5_Q320_I', # Volume V and VI (data re-extracted)
                               'CI5_Q321_I', # Volume VII and VIII (data re-extracted)
                               'CI5_Q318_I', # Volume IX (data re-extracted)
                               'CI5_Q319_I', # Volume X (data re-extracted)
                               'CI5_Q661_I', # Volume XI
                               'ci5_period_i_viii_with_skin_inc', # Volume V and VI (older version)
                               'ci5_period_ix_inc', # Volume IX (older version)
                               'CI5_X_2003_2007_inc', # Volume X (older version)
                               'CI5_X_2003_2007', # Volume X (older version)
                               'CI5_Q280_I', # Volume XI (older version)
                               ],
            'priority_rank': [1,2,3,4,5,6,7,8,9,10]}
        df = pd.DataFrame(data=d)
    elif ((staging_name in ['cod_mortality','nonfatal_skin','nonfatal']) & 
            (age_type == 'all_ages')):
        d = {'dataset_name' : ['CI5_Q320_I', # Volume V and VI (data re-extracted)
                               'CI5_Q321_I', # Volume VII and VIII (data re-extracted)
                               'CI5_Q318_I', # Volume IX (data re-extracted)
                               'CI5_Q319_I', # Volume X (data re-extracted)
                               'CI5_Q661_I', # Volume XI
                               'ci5_period_i_viii_with_skin_inc', # Volume V and VI (older version)
                               'ci5_period_ix_inc', # Volume IX (older version)
                               'CI5_X_2003_2007_inc', # Volume X (older version)
                               'CI5_X_2003_2007', # Volume X (older version)
                               'CI5_Q280_I', # Volume XI (older version)
                               ],
            'priority_rank': [1,2,3,4,5,6,7,8,9,10]}
        df = pd.DataFrame(data=d)
    elif (age_type == 'pediatric'): 
        df = pd.DataFrame(columns=['dataset_name','priority_rank'])
    return(df)


def keep_smaller_year_span(dataset, dropped_entries, staging_name, mi_mortality): 
    ''' Where redundancies exist, keeps smallest year span. 
    '''
    print('keeping datasets with a smaller year span...')
    uid_cols = define_uids() 
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    dataset['year_span'] = (dataset['year_end'] - dataset['year_start']) + 1

    # attach the smallest year_span per uid 
    dataset['span_min'] = dataset.groupby(uid_cols)['year_span'].transform(min)
    df = count_datasets(dataset)

    # keep data with the smallest year span where redundancies exist
    drop_these = ((df['num_datasets'] > 1) & (df['span_min'] != df['year_span']))
    drop_df = pd.concat([df.loc[drop_these, ], dropped_entries])
    drop_df.loc[drop_df['drop_reason'].isnull(), 'drop_reason'] = 'dropped due to favor of data with smaller year span'

    df = df.loc[~drop_these,]

    assert (len(df) + len(drop_df) == len(dataset) + len(dropped_entries)), 'some entries are missing after keeping smaller year span'
    return(df[keep_cols], drop_df[uid_cols + ['acause','dataset_name','drop_reason']]) 


def handle_ci5(dataset, dropped_entries, staging_name, age_type, mi_mortality): 
    ''' for MIRs, drop CI5 datasets where redundancies exist. And keep CI5 datasets 
        where redundancies exist for any other staging process
    '''
    print('handling CI5 datasets...')
    uid_cols = define_uids()
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    ci5_pref_list = gen_ci5_pref(staging_name, age_type)
    df = count_datasets(dataset)
    for i in ci5_pref_list['dataset_name'].unique().tolist(): 
        if (staging_name == 'mi_ratio'):    
            # drop if redundancies exist, and there's an entry from a CI5 dataset
            df.loc[(df['dataset_name'].str.slice(0,len(i)).str.upper() == i.upper()) & 
                    (df['num_datasets'] > 1), 'to_drop'] = 1
            dropreason = 'dataset has non-{} data for the same uids: {}'.format(i, uid_cols)
        elif (staging_name in ['cod_mortality','nonfatal_skin','nonfatal']): 
            # drop if redundancies exist and not coming from a CI5 dataset 
            # only drop if a ci5 dataset exists
            df.loc[(df['dataset_name'].str.slice(0, len(i)).str.upper() != i.upper()) & 
                        (df['num_datasets']>1) & 
                        (i.upper() in df['dataset_name'].str.slice(0,len(i)).str.upper().values), 'to_drop'] = 1 
            dropreason = 'dataset has {} data for the same uids: {}'.format(i, uid_cols)
        df['to_drop'].fillna(value=0, inplace=True) 
        drop_df = df.loc[df['to_drop'].eq(1),] 
        df = df.loc[df['to_drop'].eq(0), ]

        # compiling dropped entries 
        dropped_entries = pd.concat([dropped_entries, drop_df])
        dropped_entries.loc[dropped_entries['drop_reason'].isnull(), 'drop_reason'] = dropreason
        df.drop(labels=["num_datasets", 'to_drop'], axis=1, inplace=True)
        df= count_datasets(df) #recount 
    
    return(df[keep_cols], dropped_entries[uid_cols + ['acause','dataset_name','drop_reason']])


def keep_most_recent_dataset(dataset, dropped_entries, staging_name, mi_mortality): 
    ''' Keeps the most recently formatted dataset
    '''
    print('keeping most recently added dataset...')
    # merge gbd_round where datasets were added to processing 
    uid_cols = define_uids()
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    gbd_round_formatted = gen_gbd_round_map()
    if mi_mortality == "INC_MOR": 
        # when working with matched M/I data, merge in gbd_round_added information for both incidence 
        # and mortality datasets. Then take the sum of both integers and assign that to gbd_round_added 
        round_df = gbd_round_formatted.copy()
        round_df['dataset_name_INC'] = round_df['dataset_name']
        round_df['gbd_round_added_inc'] = round_df['gbd_round_added']
        ds_round_info = pd.merge(dataset, round_df, on='dataset_name_INC', how='left', indicator=True)
        assert ds_round_info['_merge'].eq('both').all(), 'oops'
        del ds_round_info['_merge']

        round_df.drop(labels=['dataset_name_INC', 'gbd_round_added_inc'],axis=1, inplace=True)
        round_df['dataset_name_MOR'] = round_df['dataset_name'] 
        round_df['gbd_round_added_mor'] = round_df['gbd_round_added']
        ds_round_info = pd.merge(ds_round_info, round_df, on='dataset_name_MOR', how='left', indicator=True)
        assert ds_round_info['_merge'].eq('both').all(), 'oops'
        del ds_round_info['_merge']

        ds_round_info['gbd_round_added'] = ds_round_info['gbd_round_added_inc'] + ds_round_info['gbd_round_added_mor']
        df = count_datasets(ds_round_info)
    else: 
        ds_round_info = pd.merge(dataset, gbd_round_formatted, on='dataset_name', how='left', indicator=True)
        df = count_datasets(ds_round_info)
        # ensure all entries merged 
        assert ds_round_info['_merge'].eq('both').all(), 'not all entries have been added to the dataset_info table!'
        del ds_round_info['_merge']

    # attach max gbd_round per uid 
    df['gbd_max'] = df.groupby(uid_cols)['gbd_round_added'].transform(max)

    # where redundancies exist, drop the entry with a smaller gbd_round 
    drop_these = ((df['num_datasets'] > 1) & (df['gbd_max'] != df['gbd_round_added']))
    drop_df = pd.concat([df.loc[drop_these, ], dropped_entries])
    drop_df.loc[drop_df['drop_reason'].isnull(), 'drop_reason'] = 'dataset has more recently formatted data for the same UIDS'
    df = df.loc[~drop_these, ]

    assert (len(df) + len(drop_df) == len(dataset) + len(dropped_entries)), 'some entries are missing after dropping most recent datset'
    return(df[keep_cols], drop_df[uid_cols + ['acause','dataset_name', 'drop_reason']])


def drop_deferred(dataset, dropped_entries, staging_name, mi_mortality): 
    ''' drops least favored datasets, only if there are redunancies 
    '''
    print('dropping deferred datasets...')
    uid_cols = define_uids() 
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    deferred_list = ['USA_SEER_threeYearGrouped_1973_2012']
    df = count_datasets(dataset) #sum of unique UIDs present in df and assings to num_datasets (will be >1 if duplicates exist)
    drop_df = dropped_entries.copy()
    # generate a deferred list and loop through each dataset 
    # dropping any of these datasets that may have redundant entries 
    for i in deferred_list:
        df.loc[df['dataset_name'].str.slice(0, len(i)).str.upper() == i.upper(), 'has_defer'] = 1 #generate has_defer columns and assign 1 when  dataset_name==i
        df['has_defer'].fillna(value=0, inplace=True) #if has_defer is NA, make it 0 
        if (df['has_defer'].eq(0).all()): #if all rows of has_defer are 0, skip else block
            continue
        else: 
            drop_entries = ((df['has_defer'].eq(1)) & (df['num_datasets'] > 1)) #if has_defer=1 and num_datasets >1 specify as true, otherwise make false
            additional_drop_df = df.loc[drop_entries, uid_cols + ['acause','dataset_name']] #for any row in df where drop_entries==True, assign to additional_drop_df and keep necessary cols
            additional_drop_df['drop_reason'] = 'dropping deferred' #add column for drop_reason
            drop_df = pd.concat([drop_df, additional_drop_df]) # concat additional_drop_df to drop_df
            df = df.loc[~drop_entries, ] #drop any rows from df where drop_entries=True
    assert ((len(df) + len(drop_df)) == (len(dataset) + len(dropped_entries))), 'some entries are missing after dropping deferred entries'
    return(df[keep_cols], drop_df[uid_cols + ['acause','drop_reason','dataset_name']]) 


def keep_preferred(dataset, staging_name, age_type, mi_mortality): 
    ''' edge case: redundancies across list of preferred datasets 
    '''
    print('keeping preferred datasets...')
    uid_cols = define_uids() 
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    pref_df = gen_preferred_ds_list(staging_name, age_type, mi_mortality)
    # merge dataframe with ranks of preferred datasets 
    # by doing this in a for_loop, this should handle scenarios if redunancies exist within priority datasets 
    df = count_datasets(dataset)
    drop_entries = pd.DataFrame()
    for i in pref_df['preferred_dataset_name'].unique().tolist():
        df.loc[df['dataset_name'].str.slice(0, len(i)).str.upper() == i.upper(), 'has_pref'] = 1 
        df['has_pref'].fillna(value=0, inplace=True)
        tmp = df.loc[df['has_pref'].eq(1), ].copy()
        tmp.rename(columns={'dataset_name':'pref_dataset_name'}, inplace=True)

        # issue with duplicates across uids when running on appended CR matched data + VR matched
        if (mi_mortality != "INC_MOR") & (tmp.duplicated(subset=uid_cols).any()):
            print('duplicates outside of VR matched data')
        else: 
            tmp.drop_duplicates(subset=uid_cols, inplace=True)

        # for those marked entries, keep uids from the preferred dataset 
        dfm = pd.merge(df[keep_cols], 
                            tmp[uid_cols + ['pref_dataset_name','has_pref','num_datasets']], 
                            on=uid_cols, 
                            how='left')
        drop = ((dfm['has_pref'].eq(1)) & (dfm['num_datasets'] > 1) & 
                    (dfm['dataset_name'] != dfm['pref_dataset_name']))
        
        
        drop_df = dfm.loc[drop,]
        df = dfm.loc[~(drop),] 
        drop_entries = pd.concat([drop_df, drop_entries])
        del df['num_datasets']
        del df['has_pref']
        del df['pref_dataset_name']
        #df.drop(labels=['num_datasets','has_pref','pref_dataset_name'], axis=1, inplace=True)  
        df = count_datasets(df) 
    assert (len(df) + len(drop_entries) == len(dataset)), 'some entries are missing from data points that were kept or dropped' 
    drop_entries['drop_reason'] = 'dropped due to preference in datasets'
    return(df[keep_cols], drop_entries[uid_cols + ['acause','drop_reason','dataset_name']]) 


def verify_keepbest_output(dataset, keepbest_df, drop_df):
    # Ensure that the length of the keepbest dataframe is not 0
    assert (len(keepbest_df) != 0), 'all data points dropped'

    # Ensure that we have at most 1 dataset representing a particular data point
    assert (len(keepbest_df.loc[keepbest_df['num_datasets'] > 1,]) == 0), 'There is more than one dataset representing a particular data point, \
        Please run KeepBest.py interactively for the particular acause and age type through the run_keep_best() function to see \
            at which of the preferential data point functions are not dropping the data points resulting in the num_datasets column to only be 1 \
                when passing the dataframe through count_datasets().'

    # Ensures total number of data points are the same
    assert (len(drop_df) + len(keepbest_df) == len(dataset)), 'total number of datapoints are not the same'
    return(True)


def replace_age_series(df, staging_name): 
    '''
    '''
    age_list = [1,2,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]
    existing_ages = df['age'].unique().tolist() 
    if (set(age_list) - set(existing_ages) == set()): 
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'dataset_name'] = df['peds_dataset_name']
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'age_range_type_id'] = df['peds_age_range_type_id']
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'registry_id'] = df['peds_registry_id']
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'year_start'] = df['peds_year_start']
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'year_end'] = df['peds_year_end']
        df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'cases'] = df['peds_cases']
        if staging_name == 'cod_mortality': 
            df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'pop'] = df['peds_pop']
        if staging_name == "mi_ratio": 
            df.loc[(df['_merge'].isin(['both','right_only'])) & (df['age'] <= 8), 'deaths'] = df['peds_deaths']
        return(df)
    else: 
        return(df)


def replace_overlaps_peds_data(adult_final, peds_final, staging_name, mi_mortality, drop_df): 
    ''' Replacing overlapping all-age data with peds only data for ages 0-14 yo
    '''
    uid_cols = define_uids()
    keep_cols = gen_keep_cols(staging_name, mi_mortality) 
    # rename columns that will be used to replace data in all_ages output 
    peds_final.rename(columns={'dataset_name':'peds_dataset_name',
                            'dataset_id' : 'peds_dataset_id', 
                            'age_range_type_id':'peds_age_range_type_id', 
                            'pop':'peds_pop', 
                            'cases' : 'peds_cases',
                            'deaths' : 'peds_deaths',
                            'year_start':'peds_year_start',
                            'year_end':'peds_year_end',
                            'registry_id' :'peds_registry_id'},
                            inplace=True)
    
    # to prevent naming issues when merging 
    if mi_mortality == "INC_MOR": 
        peds_final.rename(columns={'dataset_name_INC':'peds_dataset_name_INC', 
                                   'dataset_name_MOR': 'peds_dataset_name_MOR',
                                   'NID_inc': 'peds_NID_inc',
                                   'NID_mor': 'peds_NID_mor', 
                                   'NID' : 'peds_NID'},
                                    inplace=True)
    # note: outer merge is okay here since we only have a 1:1 relationship (i.e no duplicates exist)
    fin_df = pd.merge(adult_final, peds_final, how='outer', on=uid_cols+['acause'], indicator=True)

    # save version where all_ages is kept with no replacement operations 
    fin_all = fin_df[keep_cols]

    # keep all non-overlaps, replace peds data where there is overlap. 
    # making sure only to replace 0-14 age group 
    fin_peds = fin_df.copy()
    adult_cols_to_replace = ['dataset_name', 'age_range_type_id', 'registry_id', 
                            'year_start', 'year_end', 'cases']
    peds_cols_to_replace = ["peds_{}".format(col) for col in adult_cols_to_replace]

    # Conditions for replacement: Age is < 14 yo and overlaps with all-ages data 
    # Otherwise, also keep non overlapping peds data for 0-19 yo
    replace_cond = ((fin_peds['_merge'].isin(['both'])) & (fin_peds['age'] <= 8))
    keep_ped_cond = ((fin_peds['_merge'].isin(['right_only'])) & (fin_peds['age'] <= 9))

    # dropped overlapping entries
    dropped_data = fin_peds.loc[replace_cond, keep_cols]
    if len(dropped_data) > 0:
        dropped_data['drop_reason'] = "Overlapping with pediatric entries < 14 yo"
        if set(list(drop_df.columns)) <= set(list(dropped_data.columns)):
            drop_df = drop_df.append(dropped_data[drop_df.columns])
        else:
            drop_df = drop_df.append(dropped_data)

    # replace entries
    replace_with_this = fin_peds.loc[replace_cond|keep_ped_cond, peds_cols_to_replace]
    fin_peds.loc[replace_cond|keep_ped_cond, adult_cols_to_replace] = replace_with_this.values

    if staging_name == 'cod_mortality': 
        fin_peds.loc[replace_cond|keep_ped_cond, 'pop'] = fin_peds.loc[replace_cond|keep_ped_cond, 'peds_pop']
    if staging_name == "mi_ratio": 
        fin_peds.loc[replace_cond|keep_ped_cond, 'deaths'] = fin_peds.loc[replace_cond|keep_ped_cond, 'peds_deaths']
    fin_peds = fin_peds[keep_cols]
    
    print("Finished replacing overlapping areas with peds and all-age data")
    #assert (len(fin_peds) > 0) , \

    return(fin_all, fin_peds, drop_df)


def drop_peds_only_data(fin_peds, staging_name, mi_mortality, drop_df):
    ''' Dropping areas where we only have peds data and no
        adult data except for neo_liver_hbl and neo_eye_rb
    '''
    keep_cols = gen_keep_cols(staging_name, mi_mortality) 
    causes_keep = ['neo_liver_hbl', 'neo_eye_rb'] # causes with only peds age range

    fin_peds[['country_id', 'location_id', 'reg_uid']] = fin_peds['registry_index'].str.split(".",expand=True)

    fin_peds.loc[fin_peds['location_id'].eq('0'), 'location_id'] = fin_peds['country_id']
    peds_only = fin_peds.loc[fin_peds['age_range_type_id'].eq(3)]
    adults_only = fin_peds.loc[fin_peds['age_range_type_id'].isin([1,2,4])]

    cols_compare = ['location_id', 'acause', 'sex_id']
    national_locs = adults_only.loc[(~adults_only['acause'].isin(causes_keep)) &
                                    (adults_only['country_id'] != adults_only['location_id']),
                                    cols_compare].drop_duplicates()
    adult_areas = adults_only.loc[~adults_only['acause'].isin(causes_keep), cols_compare].drop_duplicates()
    adult_areas = (adult_areas.append(national_locs)).drop_duplicates()
    adult_areas['to_keep'] = 1

    # merge on loc, acause, sex_id pairs that are present in adult CR data
    peds_areas = peds_only.merge(adult_areas, how = "left", on = cols_compare)
    dropped_data = peds_areas.loc[(~peds_areas['to_keep'].eq(1)) & 
                                    (~peds_areas['acause'].isin(causes_keep))]
    if len(dropped_data) > 0:
        dropped_data['drop_reason'] = "Areas (cause, sex, location) with pediatric only data and no adult data"
        del dropped_data['to_keep']
        if set(list(drop_df.columns)) <= set(list(dropped_data.columns)):
            drop_df = pd.concat([drop_df, dropped_data[drop_df.columns]])
        else:
            drop_df = pd.concat([drop_df, dropped_data])

    # keep only common location, cause, sex and exception acauses
    peds_areas = peds_areas.loc[(peds_areas['to_keep'].eq(1))|
                                (peds_areas['acause'].isin(causes_keep))]
    del peds_areas['to_keep']
    final_data = pd.concat([adults_only, peds_areas])
    
    print("Finished dropping areas with only peds data")
    return(final_data[keep_cols], drop_df)


def run_keep_best(input_df, staging_name, acause, age_type, mi_mortality): 
    ''' Rules that are applied if redundancies exist: 
            1. Keep preferred datasets
            2. drop least-priority datasets
            3. keep most recently formatted datasets
            4. drop (MIRs) or keep ci5 datasets
            5. keep datasets with a smaller year_span 
            6. verify all redundances have been removed successfully 
    '''
    print('working on acause: {} - age_type: {}'.format(acause, age_type))

    input_df = input_df.loc[input_df['acause'].eq(acause), ]
    subset_df = input_df.copy() 
    if ((staging_name == 'mi_ratio') & (mi_mortality in ['MOR','INC_MOR'])):
        subset_df.loc[subset_df['dataset_name'].str.contains('&'), 'to_drop'] = 1 
        subset_df.fillna(value=0, inplace=True) 
        subset_df.loc[(subset_df['dataset_name_MOR'].eq('CoD_VR_ICD10')) | (subset_df['dataset_name_MOR'].eq('GBR_England_Wales_1981_2012_mor')), 'to_drop']  = 0
        mir_drop = subset_df.loc[subset_df['to_drop'].eq(1), ].copy()
        mir_drop['drop_reason'] = 'unmatched datasets excluded'
        subset_df = subset_df.loc[~subset_df['to_drop'].eq(1), ].copy()
    else: 
        mir_drop = pd.DataFrame() 

    (df, drop_df) = keep_preferred(subset_df, staging_name, age_type, mi_mortality)
    (df, drop_df) = drop_deferred(df, drop_df, staging_name, mi_mortality)
    (df, drop_df) = keep_most_recent_dataset(df, drop_df, staging_name, mi_mortality)
    (df, drop_df) = handle_ci5(df, drop_df, staging_name, age_type, mi_mortality)
    (df, drop_df) = keep_smaller_year_span(df, drop_df, staging_name, mi_mortality)

    # attach entries that were dropped if running for mirs 
    if (staging_name == 'mi_ratio'): 
        drop_df = pd.concat([drop_df, mir_drop])
    if (verify_keepbest_output(input_df, count_datasets(df), drop_df)):
        return(df, drop_df)
    else: 
        print('ERROR')
        sys.exit(-1)
    return(df, drop_df)


def add_dataset_id(df : pd.DataFrame) -> pd.DataFrame:
    dataset_id = 'dataset_id'
    dataset_name = 'dataset_name'
    table_name = 'dataset_info'
    if not (dataset_name in df.columns):
        raise KeyError(f"The column '{dataset_name}' isn't present in the dataframe that was passed in. We need that column in order to merge in {dataset_id}")
    can_db = cdb.db_api()
    dataset_info_table = can_db.get_table(table_name)
    dataset_info_table = dataset_info_table[[dataset_id, dataset_name]]
    df_with_id = df.merge(dataset_info_table, how='left', on=dataset_name)
    return(df_with_id)


def main(input_filepath, staging_name, mi_mortality, data_type): 
    ''' Takes an input and splits into two age groups. Then performs keepbest for each
        If any redundancies remain after the two results are appended together, 
        keep datasets from the pediatric datasets for ages 0-14. 
    ''' 
    # define uid_cols 
    uid_cols = define_uids()
    keep_cols = gen_keep_cols(staging_name, mi_mortality)
    
    # load dataset 
    input = pd.read_stata(input_filepath)
    cause_list = input.acause.unique().tolist()

    # merge age demographic information
    df = merge_age_type(input, mi_mortality, data_type)
    
    # check for data outside of peds and adult data
    assert not df['age_range_type_id'].eq(0).any(), \
        'Dataset has unknown age range, check dataset {}\n' \
            .format(df.loc[df['age_range_type_id'].eq(0), 'dataset_name'].unique().tolist())

    # keep only redundancies for adult-ages data 
    adult_df = df.loc[(df['age_range_type_id'].isin([1,2,4])), ]
    adult_dups = adult_df.loc[adult_df.duplicated(subset=uid_cols, keep=False), ] 
    non_adult_dups = adult_df.loc[~adult_df.duplicated(subset=uid_cols, keep=False), ]

    # for keepbest on peds data     
    # decision made: remove datasets that do not have population 
    drop_df = pd.DataFrame()
    if staging_name == 'cod_mortality': 
        peds_df = df.loc[(df['age_range_type_id'].isin([3])) & (df['pop'].notnull()), ]
        dropped_df = df.loc[(df['age_range_type_id'].isin([3])) & (df['pop'].isnull()), ]
        dropped_df.loc[:, 'drop_reason'] = "Has peds-only data and doesn't have population"
        drop_df = drop_df.append(dropped_df[define_uids() + ['acause', 'dataset_name', 'drop_reason']])

    else: 
        peds_df = df.loc[(df['age_range_type_id'].isin([3])), ]
    peds_dups = peds_df.loc[peds_df.duplicated(subset=uid_cols, keep=False),]
    non_peds_dups = peds_df.loc[~peds_df.duplicated(subset=uid_cols, keep=False), ]

    # apply KeepBest per cause 
    peds_unique_df = pd.DataFrame()
    adult_unique_df = pd.DataFrame()
    for i in cause_list: 
        # Pediatric Data
        if (len(peds_dups.loc[peds_dups['acause'].eq(i), ]) == 0): 
            # if no duplicates exist, move on to the next cause 
            pass
        else: 
            # run keepbest on pediatric data
            (peds_keepbest, drop_entries_peds) = run_keep_best(peds_dups, staging_name,i, 'pediatric', mi_mortality)
            peds_unique_df = pd.concat([peds_unique_df, peds_keepbest])
            drop_df = pd.concat([drop_df, drop_entries_peds])
            print("\n")

        # All Ages Data
        if (len(adult_dups.loc[adult_dups['acause'].eq(i), ]) == 0):
            # if no duplicates exist, move on to the next cause
            pass
        else:
            # run keepbest on all_ages datasets 
            (adult_keepbest, drop_entries_adult) = run_keep_best(adult_dups, staging_name, i, 'all_ages', mi_mortality)
            adult_unique_df = pd.concat([adult_unique_df, adult_keepbest])
            drop_df = pd.concat([drop_df, drop_entries_adult])
            print("\n")

    adult_unique_df = add_dataset_id(adult_unique_df)

    peds_final = pd.concat([peds_unique_df, non_peds_dups])
    adult_final = pd.concat([adult_unique_df, non_adult_dups])
    post_keepbest_df = pd.concat([peds_final, adult_final])
    post_keepbest_df = post_keepbest_df[keep_cols]

  if staging_name == 'mi_ratio': 
        (final_df_all_ages, final_df_peds, drop_df) = replace_overlaps_peds_data(adult_final, peds_final, staging_name, mi_mortality, drop_df)
    
    tmp_dir = os.path.dirname(input_filepath)

    if staging_name == 'cod_mortality':
        adult_final.to_csv('{}/keepbest_output_selected_inc_{}.csv'.format(tmp_dir, data_type))
        drop_df.to_csv('{}/keepbest_dropped_data_{}.csv'.format(tmp_dir, data_type))
    elif staging_name == 'mi_ratio':
        if (mi_mortality in ['MOR', 'INC_MOR']): 
            final_df_peds.drop(labels=['year_start','year_end','registry_id','age_range_type_id'], axis=1, inplace=True)
            final_df_peds.to_csv('{}/keepbest_output_mirs_{}_{}.csv'.format(tmp_dir, mi_mortality, data_type))
            drop_df.to_csv('{}/keepbest_dropped_data_{}_{}.csv'.format(tmp_dir, mi_mortality, data_type))
        else: 
            final_df_peds.to_csv('{}/keepbest_output_mirs_{}.csv'.format(tmp_dir, data_type))
            drop_df.to_csv('{}/keepbest_dropped_data_{}.csv'.format(tmp_dir, data_type))

    # return()

if __name__ == "__main__":
    import sys
    filepath = str(sys.argv[1])
    staging_name = str(sys.argv[2])
    mi_mortality = str(sys.argv[3])
    data_type = str(sys.argv[4])

    main(filepath, staging_name, mi_mortality, data_type)
