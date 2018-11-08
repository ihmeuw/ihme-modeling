'''
COMPILE GEOCODING RESULTS

Description: Compile all results from geocoding into a single CSV file,
  then melt results so that each non-NaN result is on a single line.


'''

# Imports (general)
import argparse
import numpy as np
import os
import pandas as pd
from os.path import join
# Imports (from this library)
from overlay_with_polygon import create_gdf_overlay_with_shp

################################################################################
# COMPILATION FUNCTIONS
################################################################################
def compile_data(in_directory):
    # Get list of all files in the input directory
    # Drop any files that don't end with '.xls' or '.xlsx'
    read_files = [i for i in os.listdir(in_directory)
                    if i.lower().endswith('.xls')
                    or i.lower().endswith('.xlsx')]
    # Concatentate all files into a single pandas dataframe
    full_df = pd.concat([pd.read_excel(join(in_directory,f)) for f in read_files])
    return full_df


def melt_drop_nan(geo_wide):
    # Add a few columns that will be kept
    for i in ['ns','ew']:
        for j in [1,2]:
            geo_wide.loc[:,'gn_r{}_buffer_{}'.format(j,i)] = np.nan
    # Rename the buffer columns to remove an underscore
    geo_wide.columns = [i.replace("buffer_","buffer") for i in geo_wide.columns]
    # Get the list of all columns to keep
    id_cols = ['uid','iso2','location_name','location_level']
    reshape_cols = list()
    for source in ['gm','osm','gn']:
        for result in [1,2]:
            for info in ['lat','long','bufferns','bufferew']:
                reshape_cols.append("{}_r{}_{}".format(source, result, info))
    geo_wide = geo_wide.loc[:,id_cols + reshape_cols]
    # Melt
    geo_long = geo_wide.melt(id_vars=id_cols,
                             value_vars=reshape_cols,
                             var_name='single_result',
                             value_name='value')
    # Split headers in 'single_result' into two separate columns
    geo_long['source'] = geo_long['single_result'].apply(
                                    lambda x: "_".join(x.split("_")[:2]))
    geo_long['result_type'] = geo_long['single_result'].apply(
                                    lambda x: x.split("_")[-1])
    geo_long = geo_long.drop(labels=['single_result'],axis=1)
    # Cast wise using the variables in result_type
    reshape_wide_index = id_cols+['source','result_type']
    reshaped_final = (geo_long.set_index(reshape_wide_index)
                              .unstack()
                              .reset_index(drop=False))
    reshaped_final.columns = reshaped_final.columns.get_level_values(-1)
    reshaped_final.columns = (reshape_wide_index[:-1] + 
                              list(reshaped_final.columns[len(reshape_wide_index)-1:]))
    reshaped_final = reshaped_final.dropna(subset=['lat','long','bufferns','bufferew'])
    return reshaped_final


################################################################################
# VOTING FUNCTIONS
################################################################################
def vote_on_group(df, in_vote_col='location_id_matched',
                  voting_result_col='loc_id_group'):
    '''
    This function is applied to sub-dataframes returned within a groupby() statement.
    The function takes a simple plurality vote on the values in the 'in_vote_col'
    and returns a new sub-dataframe with the most common result listed under a new
    column name, defined by the 'voting_result_col'. If multiple values have the same
    number of occurrences, then the voted value is returned as a NaN.
    '''
    top_two = df[in_vote_col].value_counts().head(2)
    if top_two.shape[0] == 1:
        voted_val = top_two.index[0]
    elif top_two.shape[0] == 0:
        voted_val = np.nan
    elif top_two.iloc[0] == top_two.iloc[1]:
        voted_val = np.nan
    else:
        voted_val = top_two.index[0]
    return pd.DataFrame({voting_result_col:[voted_val]})


def vote_between_levels(df, in_loc_col='loc_id_group', out_df_col='location_id_matched'):
    '''
    This function is applied to sub-dataframes returned within a groupby() statement.
    The function takes a simple plurality vote on the values in the 'in_vote_col'
    and returns a new sub-dataframe with the most common result listed under a new
    column name, defined by the 'voting_result_col'. If multiple values have the
    same number of occurrences, then the voted value is determined based on the known
    trustworthiness of various location levels in a predefined "location_level" column.
    '''
    top_two = df[in_loc_col].value_counts().head(2)
    if top_two.shape[0]==1:
        voted_val = top_two.index[0]
    elif top_two.shape[0] == 0:
        voted_val = np.nan
    elif top_two.iloc[0] == top_two.iloc[1]:
        # Vote according to location type reliability
        location_levels = df['location_level'].unique().tolist()
        if 'admin1' in location_levels:
            voted_val = df.loc[df['location_level']=='admin1',in_loc_col].iloc[0]
        elif 'admin2' in location_levels:
            voted_val = df.loc[df['location_level']=='admin2',in_loc_col].iloc[0]
        elif 'admin3' in location_levels:
            voted_val = df.loc[df['location_level']=='admin3',in_loc_col].iloc[0]
        else:
            voted_val=np.nan
    else:
        voted_val = top_two.index[0]
    return pd.DataFrame({out_df_col:[voted_val]})


def vote_on_all_data(in_data):
    '''
    This is a wrapper function for the previous two functions. Takes a vote for 
    a predefined 'overlay_loc_id' by location level and UID, then chooses
    the most likely candidate for the UID across all location levels.
    '''
    # Get the number of unique UIDs before and after voting
    in_uid_count = in_data['uid'].unique().size
    # Drop any points where the overlay did not return a location ID
    in_data = in_data.dropna(subset=['location_id_matched'])
    # Choose within each location level
    grouped = (in_data.groupby(by=['uid','location_level'])
                      .apply(lambda x: vote_on_group(x))
                      .reset_index(drop=False)
                      .dropna(subset=['loc_id_group']))
    # Choose across all levels
    grouped_2 = (grouped.groupby(by=['uid'])
                        .apply(lambda x: vote_between_levels(x))
                        .reset_index(drop=False)
                        .dropna(subset=['location_id_matched']))
    grouped_2.drop(labels=[i for i in grouped_2.columns if i.startswith('level_')],
                   axis=1, inplace=True)
    out_uid_count = grouped_2['uid'].unique().size
    print("{} out of {} uids were matched in the voting process.".format(out_uid_count,in_uid_count))
    return grouped_2


################################################################################
# MAIN FUNCTION
################################################################################
def process_all_geocoding_results(in_directory):
    '''
    This function runs the full geocoding post-processing workflow. It compiles
    all Excel files in the given input directory into a single Pandas dataframe,
    then melts that dataframe into a long format by result number and UID. It
    sends the melted dataframe for overlay with the GBD master shapefile,
    then takes the overlaid data and votes on the most likely candidate location
    for each UID.

    Input:
      in_directory: The absolute path to the folder where all geocoding outputs 
        were saved.

    Output:
      geocoding_voted_result: Dataframe containing UID and 'loc_id_matched'
        column representing the most likely GBD location for each UID as selected
        by a vote of geocoding results
    '''
    # Compile and melt data
    geocoding_raw_results = compile_data(in_directory)
    original_uid_count = geocoding_raw_results['uid'].unique().size
    melted = melt_drop_nan(geocoding_raw_results)
    # Overlay with the GBD master shapefile
    # This creates a new field, 'location_id_matched'
    overlaid = create_gdf_overlay_with_shp(melted, lat_col='lat', lon_col='long',
                                drop_lat_lon=True, snap_points=False)
    # Vote on the most likely GBD location ID for each shock
    geocoding_voted_result = vote_on_all_data(overlaid)
    final_uid_count = geocoding_voted_result['uid'].unique().size
    print("{} out of {} UIDs were successfully matched to locations in the "
          "geociding process.".format(final_uid_count,original_uid_count))
    return geocoding_voted_result


if __name__=="__main__":
    # Set up argument parsing for input and output files
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--indir",type=str,
                        help="The directory where all geocoded results are stored")
    parser.add_argument("-o","--outfile",type=str,
                        help="The filepath of the processed geocoding CSV file")
    parser.add_argument("-e","--encoding",type=str, default='latin1',
                        help="The encoding of the output CSV file")
    # Parse arguments
    cmd_args = parser.parse_args()
    assert cmd_args.indir is not None, "Program requires an input directory"
    assert cmd_args.outfile is not None, "Program requires an output filepath"
    # Compile all the data in the folder into a single dataframe
    compiled_voted_results = process_all_geocoding_results(cmd_args.indir)
    # Save to the output csv file    
    compiled_voted_results.to_csv(cmd_args.outfile,index=False,
                                  encoding=cmd_args.encoding)