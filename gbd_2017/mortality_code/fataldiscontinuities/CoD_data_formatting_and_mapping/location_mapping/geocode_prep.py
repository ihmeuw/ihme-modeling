'''
GEOCODE SHOCKS DATA

Purpose: This is a managing script that prepares data for geocoding
  and then passes it to the geocoding functions in the batch_geocode repo.

'''

# Imports (general)
import numpy as np
import os
import pandas as pd
from os.path import join
from sys import platform

# Imports (Shared functions)
from db_queries import get_location_metadata

# Define your J directory
if "win" in platform:
    j_head = "J:/"
else:
    j_head = "/home/j/"


## Functions for preparing the dataframe columns for geocoding
def construct_iso2_table(iso_match_table_file,
                         location_set_id=21):
    '''
    This function constructs a dataframe that contains all known mappings
    of GBD 'location_id' to ISO2 code (stored as the 'iso2' field).

    Inputs:
      iso_match_table_file (str): The full filepath to a CSV file containing
        mappings of ISO3 codes to ISO2 codes (stored in the fields 'iso3' 
        and 'iso2', respectively)
      location_set_id (int): The location set ID to pull GBD location metadata
        for. This should be consistent with the analysis set being used for the
        shocks data

    Output:
      loc_id_to_iso (pandas DataFrame)
    '''
    # Get location metadata for the given location set
    meta = get_location_metadata(location_set_id=location_set_id)
    # Keep only national and subnational data for assigning ISO codes
    meta = meta.loc[meta['level']>=3,:]
    # Create a new column that represents the ISO3 code
    meta.loc[:,'iso3'] = meta['ihme_loc_id'].apply(lambda x: str(x)[:3].upper())
    # Read the ISO2 to ISO3 table to get all ISO2 columns
    iso_match_table = pd.read_csv(iso_match_table_file,
                                  encoding='utf8')
    # Merge ISO2 to location ID using ISO3 as a match field
    loc_id_to_iso = pd.merge(left=meta,
                             right=iso_match_table,
                             on=['iso3'],
                             how='inner')
    # Keep only location ID and ISO2 code for matching
    loc_id_to_iso = (loc_id_to_iso.loc[:,['location_id','iso2']]
                                  .drop_duplicates())
    return loc_id_to_iso

    
def match_to_iso2(shocks_data,iso2_matching_df):
    # Merge on the location_id field to get the ISO2 field
    # Keep all rows in the shocks table
    matched = pd.merge(left=shocks_data,
                       right=iso2_matching_df,
                       on='location_id',
                       how='left')
    return matched


def reshape_for_geocode(shocks_df,
                        cols_to_reshape,
                        reshaped_col_name='location_name',
                        preserve_cols=['iso2','uid']):
    # Reshape long using all cols_to_reshape
    reshaped = pd.melt(shocks_df,
                       id_vars=preserve_cols,
                       value_vars=cols_to_reshape,
                       var_name='location_level',
                       value_name=reshaped_col_name)
    # Drop missing values
    reshaped[reshaped_col_name] = reshaped[reshaped_col_name].replace(
                                                                to_replace='',
                                                                value=np.nan)
    reshaped = reshaped.dropna(subset=[reshaped_col_name])
    # Assert that no events were totally dropped
    assert shocks_df['uid'].unique().size == reshaped['uid'].unique().size
    return reshaped


def clear_folder(folder_path):
    '''
    Removes all files in a given directory.
    '''
    for f in os.listdir(folder_path):
        if os.path.isfile(join(folder_path,f)):
            os.remove(join(folder_path,f))
    return None


def split_save_for_geocoding(full_data,
                             save_folder,
                             max_row_count=2450,
                             max_num_files=10):
    # Check that there are enough geocoding keys
    if full_data.shape[0] > max_num_files * max_row_count:
        raise ValueError("Too many rows to geocode, need additional geocoding keys")
    # Get the number of dataframes to split into
    num_files_to_save = max_num_files or np.max([1,int(np.ceil(1.0*full_data.shape[0]
                                                               / max_row_count))])
    # Split and save to a folder
    split_dfs = np.array_split(full_data, num_files_to_save)
    for i in range(num_files_to_save):
        split_dfs[i].to_excel(join(save_folder,"geocode_{}.xlsx".format(i)),
                              index=False)
    return None

## Main function
def geocode_shocks(shocks_for_geocoding_file,
                   iso3_to_iso2_match_file,
                   cols_to_geocode,
                   save_folder,
                   gbd_location_set_id=21,
                   csv_encoding='latin1'):
    '''
    This function prepares a file for geocoding by matching to an ISO2 code
    wherever possible, melting the data frame so that all location names are 
    stored within a single location column, and then saving this reshaped data
    in manageable chunks to a given folder.

    Inputs:
      shocks_for_geocoding_file (str): The filepath to the shocks location
        data that needs geocoding
      iso3_to_iso2_match_file (str): The path to the CSV file containing
        matches between ISO3 codes (which can be derived from GBD location
        metadata) and ISO2 codes (which are needed for geocoding)
      cols_to_geocode (list): All fields in the shocks location data that
        may contain data that could be used for geocoding
      save_folder (str): Full path to the directory where this geocoded data
        will be stored. THIS FOLDER WILL BE EMPTIED BY THE SCRIPT so the
        folder should only be used for storing temporary data for geocoding
      gbd_location_set_id (int, optional): The GBD location set ID that will
        be used to pull location metadata. Defaults to 21 (mortality computation
        hierarchy)
      csv_envoding (str, optional): The encoding to use to read the shocks CSV.
        Defaults to 'latin1'

    Outputs:
      None; formatted shocks data will be saved to the save_folder.
    '''
    ## Read in all shocks locations that need geocoding
    shocks_for_geocoding = pd.read_csv(shocks_for_geocoding_file,
                                       encoding=csv_encoding)
    ## Associate each shocks event with an ISO-2 code, wherever possible
    map_iso_codes = construct_iso2_table(location_set_id=gbd_location_set_id,
                                iso_match_table_file=iso3_to_iso2_match_file)
    shocks_with_iso2 = match_to_iso2(shocks_data=shocks_for_geocoding,
                                     iso2_matching_df=map_iso_codes)
    ## Create a new dataframe that has three fields: UID, location, and ISO-2
    reshaped = reshape_for_geocode(shocks_df=shocks_with_iso2,
                                   cols_to_reshape=cols_to_geocode)
    ## Save all files for geocoding to the given folder
    ## Empty the folder beforehand to ensure that no extra files are sent to
    ##  geocoding
    clear_folder(save_folder)
    split_save_for_geocoding(full_data=reshaped,
                             save_folder=save_folder)
    return None


if __name__ == "__main__":
    # DEFINE PATHS
    shocks_folder = 'FILEPATH'
    code_folder = join(shocks_folder,'FILEPATH')
    iso_match_file = join(code_folder,"FILEPATH")
    data_to_geocode = join(shocks_folder,"FILEPATH")
    cols_to_geocode = ["admin1","admin2","admin3","location"]
    save_folder = join(shocks_folder,"FILEPATH")
    location_set = 21
    encoding='latin1'

    geocode_shocks(shocks_for_geocoding_file=data_to_geocode,
                   iso3_to_iso2_match_file=iso_match_file,
                   cols_to_geocode=cols_to_geocode,
                   save_folder=save_folder)
