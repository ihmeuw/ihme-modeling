'''

Purpose: Based on the geographies that each point was assigned to in the 
  polygon overlay, determine which geography is the most likely match. 
  Returns a dataframe containing each of the UIDs

'''

# Imports (general)
import argparse
import numpy as np
import os
import sys
import pandas as pd
from os.path import join
from time import sleep
import getpass
import subprocess as sp
# Imports (this folder)
from multi_loc_splitting import multi_loc_split, split_to_most_detailed
# Imports (top-level)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities.create_tracker import create_tracker


def process_single_match_file(folder, filename, loc_col='location_id_matched',encoding='latin1'):
    loc_match_method = filename[:-4]
    data = pd.read_csv(join(folder, filename),encoding=encoding)
    data = data.loc[:,['uid',loc_col]]
    data.loc[:,'loc_match_method'] = loc_match_method
    data.loc[data[loc_col].isnull(),'loc_match_method']=''
    return data


def compile_matches(folder, encoding='latin1'):
    all_matches = [f for f in os.listdir(folder)
                     if os.path.isfile(join(folder,f))]
    compiled = pd.concat([process_single_match_file(folder, f, encoding=encoding)
                            for f in all_matches])
    compiled = compiled.drop_duplicates(subset=['uid','location_id_matched'], keep='first')
    compiled = compiled.drop_duplicates(subset=['uid'], keep='first')
    return compiled


def add_new_matched_data(raw_data, compiled_data, fill_at_end):
    merged = pd.merge(left=raw_data,
                      right=compiled_data,
                      on=['uid'],
                      how='left')
    fill_at_end = (fill_at_end.loc[:,['uid','location_id_matched']]
                               .drop_duplicates(subset=['uid'],keep='first')
                               .rename(columns={'location_id_matched':'__not_detailed'}))
    merged = pd.merge(left=merged,
                      right=fill_at_end,
                      on=['uid'],
                      how='left')
    merged.loc[merged['location_id_matched'].isnull(),
               'location_id_matched'] = merged.loc[merged['location_id_matched'].isnull(),
                                                   '__not_detailed']
    merged = merged.drop(labels=['location_id','__not_detailed'], axis=1)
    merged = merged.rename(columns={'location_id_matched':'location_id'})
    return merged


def get_records_with_multiple_locs(compiled_data, multi_locs_file,
                                   encoding='latin1'):
    # Get all UIDs that apparently refer to multiple locations
    multi_locs_df = pd.read_csv(multi_locs_file, encoding=encoding)
    multi_locs_uids = multi_locs_df['uid'].unique().tolist()
    # Set the location match method for these to 'multiple_locations'
    compiled_data.loc[compiled_data['uid'].isin(multi_locs_uids) &
                      compiled_data['location_id'].isnull(),
                      'loc_match_method'] = 'multi_loc_not_matched'
    return compiled_data


def split_out_missing(db):
    '''
    Fill possible missing fields and then split out all rows missing an event type
    or location ID
    '''
    missing_location_id = db['location_id'].isnull()
    missing_event_type = db['event_type'].isnull()
    missing_locs_df = db.loc[missing_location_id,:]
    missing_events_df = db.loc[missing_event_type,:]
    complete_db = db.loc[~missing_location_id & ~missing_event_type,:]
    return (missing_locs_df, missing_events_df, complete_db)

#launches in python 2. saves over the old model_ready
def run_side_overrides(in_filepath, out_filepath, encoding):
    GBD_PYTHON_PATH = ("FILEPATH")
    side_splitting_code = join(os.path.dirname(os.path.abspath(__file__)),
                          'FILEPATH')
    args = ['--infile',in_filepath,'--outfile',out_filepath,'--encoding',encoding]
    sp.check_call([GBD_PYTHON_PATH,side_splitting_code]+args)
    side_split_data = pd.read_csv(out_filepath,encoding=encoding)


if __name__ == "__main__":
    # Get input filepaths as command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--storage_dir",type=str,
                        help="The directory where all data from this run is being"
                             "stored.")
    parser.add_argument("-o","--outfile",type=str,
                        help="The filepath for the compiled data")
    parser.add_argument("-u","--outfile_split",type=str,
                        help="The filepath where location-split data will be saved.")
    parser.add_argument("-e","--encoding",type=str,default='latin1',
                        help="The encoding to read and write all CSVs with")
    cmd_args = parser.parse_args()

    # DEFINE PATHS
    # The path to the file before location standardization
    raw_file_no_match = join(cmd_args.storage_dir,"FILEPATH")
    # The path where all matched files have been stored
    matched_folder = join(cmd_args.storage_dir,"FILEPATH")
    # The path to the file containing multiple locations
    multi_locs_file = join(cmd_args.storage_dir,
                           "FILEPATH")
    # The path to the file containing data that is not detailed
    not_detailed = join(cmd_args.storage_dir,"FILEPATH")    

    # READ FILES
    raw_data = pd.read_csv(raw_file_no_match, encoding=cmd_args.encoding)
    fill_at_end = pd.read_csv(not_detailed,encoding=cmd_args.encoding)
    all_matches = compile_matches(matched_folder, encoding=cmd_args.encoding)
    # PROCESS
    standardized = add_new_matched_data(raw_data=raw_data, compiled_data=all_matches,
                                        fill_at_end=fill_at_end)
    # Determine when a field has multiple locations
    #   (For cases that did not match)
    standardized = get_records_with_multiple_locs(compiled_data=standardized,
                                             multi_locs_file=multi_locs_file,
                                             encoding=cmd_args.encoding)
    #   (For cases that did match)
    if 'multi_loc_split' in standardized.columns:
        standardized.loc[(standardized['multi_loc_split']==1) & 
                         (standardized['location_id'].notnull()),
                         'loc_match_method'] = 'multi_loc_matched'    
    # Any remaining events without a location ID must be manually matched
    standardized.loc[standardized['location_id'].isnull(),
                     'loc_match_method'] = 'needs_manual_match'
    # Any other fields that contained location IDs but were not tagged were
    #  probably matched from the beginning
    standardized.loc[standardized['loc_match_method'].isnull(),
                     'loc_match_method'] = 'already_positioned'
    # Save output
    standardized.to_csv(cmd_args.outfile, index=False,
                        encoding=cmd_args.encoding)
    # *** Location split ***
    split_df = multi_loc_split(standardized)
    split_df = split_to_most_detailed(split_df,
                                      location_id_col='location_id',
                                      location_set_id=35)
    # Save the split location dataframe
    split_df.to_csv(cmd_args.outfile_split, index=False,
                    encoding=cmd_args.encoding)
    # Split out any rows missing rows or event type mappings
    (missing_locs, missing_events, model_ready) = split_out_missing(split_df)
    if missing_locs.shape[0] > 0:
        missing_locs.to_csv(join(cmd_args.storage_dir,'FILEPATH'),
                            encoding='utf8',index=False)
    if missing_events.shape[0] > 0:
        missing_events.to_csv(join(cmd_args.storage_dir,'FILEPATH'),
                              encoding='utf8',index=False)
    model_ready_filepath = join(cmd_args.storage_dir,'FILEPATH')

    model_ready.to_csv(model_ready_filepath,encoding='utf8',index=False)

    run_side_overrides(model_ready_filepath, model_ready_filepath, 'utf8')

    # Test 5 times to make sure that the file system recognizes the new file
    num_tries = 0
    while (num_tries < 5 and not os.path.exists(model_ready_filepath)):
        num_tries = num_tries + 1
        sleep(2)