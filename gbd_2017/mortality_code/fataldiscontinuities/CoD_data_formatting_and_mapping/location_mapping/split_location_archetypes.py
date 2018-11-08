
# Imports (general)
import argparse
import numpy as np
import os
import pandas as pd
from os.path import join
from sys import platform

from db_queries import get_location_metadata

# Define your J directory
if "win" in platform:
    j_head = "J:/"
else:
    j_head = "/home/j/"

################################################################################
# EXPLORATORY FUNCTIONS
################################################################################
def split_on_missing(in_df, cols, how='any'):
    '''
    Split a dataframe into two parts, one where the specified columns are missing (NA),
    and another where they are not
    
    Input:
    in_df = Full dataframe
    cols = List of fields to check for completeness
    how = One of "any" or "all" - drop rows with ANY missing data, or only
      rows where ALL selected fields are missing data
    
    Output: 
    missing, not_missing = Two dataframes where the columns are either missing or not
    '''
    # Validate input arguments
    assert type(in_df) is pd.core.frame.DataFrame, "in_df must be a DataFrame"
    cols = list(cols)
    assert np.all([col in in_df.columns for col in cols]), "Not all columns in the dataframe field"
    assert how in ['any','all'], "how must be one of ['any','all']"
    # Subset down to non-missing rows and missing rows only
    not_missing = in_df.dropna(how=how, subset=cols)
    if how=='any':
        missing = in_df.loc[in_df.loc[:,cols].isnull().any(axis=1),:]
    else:
        missing = in_df.loc[in_df.loc[:,cols].isnull().all(axis=1),:]
    # Make sure that the missing and non-missing row counts add up to the total
    #   number of rows in the original dataframe
    assert in_df.shape[0] == not_missing.shape[0] + missing.shape[0], "Subsetting didn't work..."
    return (not_missing, missing)


def check_missing(in_df, cols, how='any'):
    '''
    Same arguments as "split_on_missing", but rather than returning two DFs
    this function tells you how many rows are missing any/all of the specified
    columns
    '''
    # Get total number of rows
    old_row_count = in_df.shape[0]
    # Keep only the rows of interest and drop NA data
    missing_rows = (split_on_missing(in_df=in_df,cols=cols,how=how)[1]
                          .shape[0])
    print("Out of {} rows total, {} rows were missing data "
          "for any of the following fields: {}".format(
                           old_row_count, missing_rows,cols))
    return None


def check_if_most_detailed(in_df, location_set_id=21):
    # Get location metadata for joining
    meta = get_location_metadata(location_set_id=location_set_id)
    meta_sub = meta.loc[:,['location_id','most_detailed']].drop_duplicates()
    # Create a new field, 'already_located', which identifies whether a row
    #  has already been assigned a most detailed GBD location ID
    in_df = pd.merge(left=in_df, right=meta_sub, on=['location_id'],
                     how='left')
    in_df['already_located'] = (in_df['most_detailed']
                                    .fillna(0)
                                    .astype(np.bool_))
    in_df = in_df.drop('most_detailed',axis=1)
    return in_df


def check_multiple_locs(in_df,test_commas_fields,
                        test_dashes_fields):
    '''
    This function splits the input dataframe based on whether or not the 
    location data in that dataframe apparently refers to multiple locations and
    will need to be split. It does this by testing for commas and dashes in
    certain location fields

    Inputs:
      in_df (pandas DataFrame): the input data to be split
      test_commas_fields (list): The fields where commas would indicate multiple
        locations
      test_dashes_fields (list): The fields where dashes would indicate multiple
        locations

    Outputs:
      has_multiple_locations (pandas DataFrame): All rows that apparently refer
        to multiple locations
      one_location_only (pandas DataFrame): All rows that apparently refer to 
        a single location
    '''
    # Create a field that indicates whether or not a given row apparently
    #  refers to multiple locations
    in_df['__multi_locs'] = False
    # Iteratively check for commas in each test_commas_field
    for field in test_commas_fields:
        in_df['__multi_locs'] = (in_df['__multi_locs'] |
                                 in_df[field]
                                        .apply(str)
                                        .str.contains(',')
                                        .fillna(False))
    # Iteratively check for dashes in each test_dashes_field
    for field in test_dashes_fields:
        in_df['__multi_locs'] = (in_df['__multi_locs'] |
                                 in_df[field]
                                        .apply(str)
                                        .str.contains('-')
                                        .fillna(False))
    # Split the dataframe based on the multiple location columns, dropping
    #  this column at the same time
    has_multiple_locations = (in_df.loc[in_df['__multi_locs'],:]
                                   .drop(labels=['__multi_locs'],axis=1))
    one_location_only = (in_df.loc[~in_df['__multi_locs'],:]
                              .drop(labels=['__multi_locs'],axis=1))
    return (has_multiple_locations, one_location_only)


def define_data_archetypes(in_df):
    '''
    Determine the best strategy for geolocating each shock.

    Input: 
        in_df = the shocks dataframe with all location columns still attached
    
    Output: Six dataframes - in order:
        already_positioned = Already matched to a most-detailed GBD location
        has_coords = Has lat/longs that should match it to a GBD location
          using a spatial join
        multiple_locs = The data does not have coordinates and apparently
          refers to multiple locations. It will need to be split before it can
          be used
        can_geocode = No lat/longs, but has location data that might allow
          us to determine a lat/long which can then be matched to a GBD 
          location via a spatial join
        not_detailed = The data has been matched to a GBD location, but not
          the most detailed location. There is no data at present that can be
          used to narrow down the location unless someone adds it manually
        manual_add = Insufficient location information at present, a DA needs
          to add more location data manually
    '''
    # Add on two new columns, 'location_id_matched' (GBD location ID based on existing fields)
    # and 'already_geolocated' (whether or not we have already found the most detailed geography)
    in_df = in_df.copy()
    in_df['location_id_matched'] = in_df['location_id'].copy()
    in_df = check_if_most_detailed(in_df)

    # Split into shocks that have already been geopositioned to a most detailed
    # GBD location, and those that should ideally be geocoded further
    already_positioned = (in_df.loc[in_df['already_located'],:]
                               .drop(labels='already_located', axis=1))
    needs_positioning = (in_df.loc[~in_df['already_located'],:]
                              .drop(labels='already_located', axis=1))
    all_matched_fill_at_end = in_df.loc[~in_df['location_id_matched'].isnull(),:]


    # Split into shocks with and without lat-longs
    (has_coords, no_coords) = split_on_missing(needs_positioning,
                                                 cols=['latitude','longitude'],
                                                 how='any')
    # Ensure that all of the data with latitude-longitude already has country information
    # The following should show that 0 rows are missing all country/location information
    print("\nChecking that all data with lat/longs also has some GBD-location identifying information:")
    check_missing(has_coords, cols=['location_id_matched'])

    # Split based on whether or not the location data will apparently need to 
    #  be split before geocoding
    (multiple_locs, one_loc) = check_multiple_locs(no_coords,
                                     test_commas_fields=['admin1',
                                                 'admin2','admin3','country'],
                                     test_dashes_fields=['country'])

    # Split based on locations that have enough data to try geocoding
    (can_geocode, cant_geocode) = split_on_missing(one_loc,
                                  cols=['location','admin1','admin2','admin3'],
                                  how='all')

    # Split based on whether or not shocks without enough data for geocoding
    #   have already been at matched to any GBD geography, even though it is 
    #   not the most detailed geography:
    (not_detailed, manual_add) = split_on_missing(cant_geocode,
                                                 cols=['location_id_matched']) 


    if manual_add.shape[0] > 0:
        print("*** {} data rows need manual location vetting: ***"
                                                .format(manual_add.shape[0]))

    # Make sure that all of the sub-groupings add up to the total number of rows
    assert (in_df.shape[0] == already_positioned.shape[0] + 
                              has_coords.shape[0] + 
                              multiple_locs.shape[0] +
                              can_geocode.shape[0] + 
                              not_detailed.shape[0] +
                              manual_add.shape[0]),(
               "The sub-groups didn't add up to the "
               "total number of rows for some reason...")

    # Print number of rows in each grouping
    print("\nGEOPOSITIONING ARCHETYPES:")
    print("'already_positioned':{} rows".format(already_positioned.shape[0]))
    print("'has_coords':        {} rows".format(has_coords.shape[0]))
    print("'can_geocode':       {} rows".format(can_geocode.shape[0]))
    print("'multiple_locs':     {} rows".format(multiple_locs.shape[0]))
    print("'not_detailed':      {} rows".format(not_detailed.shape[0]))
    print("'manual_add':        {} rows".format(manual_add.shape[0]))
    print(" TOTAL:              {} rows".format(in_df.shape[0]))
    # Return the four dataframes in order
    return (already_positioned,has_coords,multiple_locs,
            can_geocode,not_detailed,manual_add,all_matched_fill_at_end)


def define_and_save_all(in_filepath, save_dir, csv_encoding='latin1'):
    shocks_raw = pd.read_csv(in_filepath, encoding=csv_encoding)
    # Create a unique identifier to reference back to the original data
    if 'uid' not in shocks_raw.columns:
        shocks_raw['uid'] = shocks_raw.index
        shocks_raw.to_csv(in_filepath, encoding=csv_encoding, index=False)

    keep_cols = ['uid', # Unique identifier
       # The following are location data taken directly from the surveys
       'admin1','admin2','admin3','latitude','longitude','location','country',
       'location_precision',
       # Following is for points that didn't fall within any GBD geography
       'location_id',
       'iso',
       'urban_rural']

    shocks_geo = shocks_raw.loc[:,keep_cols].copy()

    # Create a dictionary for all of the shock archetypes and then determine
    #  those archetypes
    archetypes = dict()
    (archetypes["already_positioned"],
    archetypes["has_coords"],
    archetypes["multiple_locs"],
    archetypes["can_geocode"],
    archetypes["not_detailed"],
    archetypes["manual_add"],
    archetypes["all_matched_fill_at_end"]) = define_data_archetypes(shocks_geo)

    # Clear the existing files from this folder to avoid overlap
    for f in os.listdir(save_dir):
        os.remove(join(save_dir,f))
    # Iteratively save these shock archetypes to the output folder
    for arch_name, arch_data in archetypes.items():
        arch_data.to_csv(join(save_dir, "{}.csv".format(arch_name)),
                         encoding=csv_encoding, index=False)


if __name__ == "__main__":
    # Get filepaths and encoding
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--infile",type=str,
                        help="The filepath of the input data csv")
    parser.add_argument("-o","--outdir",type=str,
                        help="The folder where all data archetypes will be stored")
    parser.add_argument("-e","--encoding",type=str,default='latin1',
                        help="The encoding to read and write the points data CSV")
    cmd_args = parser.parse_args()

    # Run the main function
    define_and_save_all(in_filepath=cmd_args.infile,
                        save_dir=cmd_args.outdir,
                        csv_encoding=cmd_args.encoding)