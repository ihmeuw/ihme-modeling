'''
MAP LOCATION NAMES

This program matches all possible input location names to GBD locations using
direct matching, fuzzy matching, and pre-defined maps.

'''
# Imports (general)
import argparse
import numpy as np
import os
import pandas as pd
import sys
from fuzzywuzzy import fuzz
from os.path import join
# Imports (from top-level)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities.save_tools import read_pandas, save_pandas
# Imports (Shared functions)
from db_queries import get_location_metadata


################################################################################
# Match to GBD admin1 units and location IDs directly using a location dictionary
################################################################################
def location_dictionary_map(in_df,
                   map_df_path="FILEPATH",
                   encoding=None):
    '''
    This function applies a dictionary that links current country and location values
    to the correct GBD country names, admin1 names, and location IDs. This
    dictionary also deletes erroneous admin1 names where they add no additional
    information (that is, they do not add information that could be used to
    link a subnational or geocode).

    Inputs:
      in_df (pandas DataFrame): The input shocks data. Ideally this data is
        coming in immediately after compilation but before location 
        standardization has taken place.
      map_df_path: The filepath to the mapping dataframe. This dataframe contains
        six relevant columns: columns for joining to the dataframe ('country' and
        'location'), columns for replacing place names ('country_replace' and 
        'admin1_replace'), a corrected location id field ('location_id'), and
        a field for deleting erroneous admin1 fields ('drop_admin1')

    Outputs:
      transformed_df (pandas DataFrame): The data after all specified location
        changes have been applied
    '''
    # Read in the map dataframe
    map_df = read_pandas(map_df_path, encoding=encoding)
    # Subset only to merge columns
    map_df = (map_df.loc[:,['country','location','country_replace','admin1_replace',
                           'drop_admin1','location_id']]
                    .rename(columns={'location_id':'__add_location_id'}))
    # Confirm that there are no duplicate map fields
    map_df = map_df.loc[~map_df.duplicated(subset=['country','location']),:]
    # JOIN THE MAP DATAFRAME TO THE INPUT DATA:
    #  Apply a transformation for NaN fields in the joining columns of both
    #  datasets
    map_df['location'] = map_df['location'].fillna("__NULL_MERGE")
    in_df['location'] = in_df['location'].fillna("__NULL_MERGE") 
    #  Join
    in_df = pd.merge(left=in_df,
                     right=map_df,
                     on=['country','location'],
                     how='left')
    #  Transform back to NaNs
    in_df.loc[in_df['location']=='__NULL_MERGE','location'] = np.nan
    # APPLY TRANSFORMATIONS:
    #  Delete erroneous admin1 columns
    in_df.loc[in_df['drop_admin1']==1,'admin1'] = np.nan
    #  Correct all relevant countries
    in_df.loc[in_df['country_replace'].notnull(),
              'country'] = in_df.loc[in_df['country_replace'].notnull(),
                                     'country_replace']
    #  Correct all relevant admin1 units
    in_df.loc[in_df['admin1_replace'].notnull(),
              'admin1'] = in_df.loc[in_df['admin1_replace'].notnull(),
                                     'admin1_replace']
    #  Add all location ids
    in_df.loc[in_df['__add_location_id'].notnull(),
              'location_id'] = in_df.loc[in_df['__add_location_id'].notnull(),
                                         '__add_location_id']
    # Clean up transformed data
    in_df = in_df.drop(labels=['drop_admin1','country_replace','admin1_replace',
                               '__add_location_id'], axis=1)

    in_df.loc[(in_df['country'].str.contains("Yemen") & 
              np.invert(in_df['country'].apply(str).str.contains(","))),
              "country"] = "Yemen"
    in_df.loc[in_df['country']=="Yemen","location_id"] = 157
    # The same transformation goes for Cote d'Ivoire
    in_df.loc[(in_df['country'].str.contains("Ivoire") & 
              np.invert(in_df['country'].apply(str).str.contains(","))),
              "country"] = "Cote d'Ivoire"
    in_df.loc[in_df['country']=="Cote d'Ivoire","location_id"] = 205

    # Return transformed data
    transformed_df = in_df.copy()
    return transformed_df


################################################################################
# Directly match variants of each column to GBD location IDs
################################################################################
def match_nan_only(in_df, match_df,
                   in_df_key,
                   match_df_key,
                   match_column_to_add,
                   assigned_column_name):
    '''
    Add values from a single column in a second dataframe
    based on matching values in the original and joining
    dataframe. Both columns will be set to lower-case where possible
    before joining.
    '''
    # Standardize and strip whitespace in data df
    def standardize_if_string(x):
        if type(x) is not str:
            return x
        else:
            return x.lower().strip()
    in_df['__join_col'] = in_df[in_df_key].apply(standardize_if_string)
    # Standardize and strip whitespace in matching df
    match_df = (match_df.rename(columns={match_column_to_add:'__new_values',
                            match_df_key:'__join_col'})
                        .drop_duplicates(subset='__join_col'))
    match_df['__join_col'] = match_df['__join_col'].apply(standardize_if_string)
    combined_df = pd.merge(left=in_df,
                           right=match_df.loc[:,['__new_values','__join_col']],
                           on=['__join_col'],
                           how='left')
    # Confirm that no keys were duplicated (no more )
    assert (in_df.shape[0]==combined_df.shape[0]),("ERROR: There were duplicated"
            " values for {} in the matching dataframe".format(match_df_key))
    # Assign new values from the matching dataframe only in cases where
    #  those values were NaNs in the main dataframe
    combined_df.loc[np.isnan(combined_df[assigned_column_name]),
                    assigned_column_name] = combined_df.loc[
                    np.isnan(combined_df[assigned_column_name]),'__new_values']
    combined_df.drop(labels={'__new_values','__join_col'},axis=1,inplace=True)
    return combined_df


def remove_suffixes(x):
    if type(x) is not str:
        return x
    # Remove all suffixes
    for suffix in ["province","district","county",
                     "state","isl.","island","country"]:
        if x.lower().endswith(suffix):
            x = x[:(-len(suffix))-1]
    return x


def unified_location_column(in_df,
                            match_df,
                            location_columns,
                            match_columns,
                            match_column_to_add,
                            new_column_name="location_id_matched"):
    '''
    Create a unified location column based on a number of rows in the input
    dataframe (in_df) matched to columns in the reference dataframe (match_df).
    Rows for matching should be listed in order of precedence -- that is,
    the first column listed will be used for matching first, and only 
    un-matched rows will be matched using the later columns
    Other inputs:
      match_column_to_add = The column whose values in the match_df will be 
                            assigned to the in_df
      new_column_name = The name of the new column that will be returned for
                        the in_df
    Outputs:
      A single dataframe containing a new column with values from
      match_column_to_add wherever the in_df aligned with the match_df
    '''
    location_columns=list(location_columns)
    match_columns=list(match_columns)
    # Input validation: same number of columns, all columns listed must
    #  be in their respective dataframes
    assert len(location_columns)==len(match_columns), ("location_columns and "
                                      "match_columns must be the same length")
    assert np.all([i in in_df.columns for i in location_columns])
    assert np.all([i in match_df.columns for i in match_columns])
    # Now, iteratively match the columns
    if new_column_name not in in_df.columns:
        in_df[new_column_name] = np.nan
    for i in range(0,len(location_columns)):
        in_df = match_nan_only(in_df=in_df,
                   match_df=match_df,
                   in_df_key=location_columns[i],
                   match_df_key=match_columns[i],
                   match_column_to_add=match_column_to_add,
                   assigned_column_name=new_column_name)
    return in_df


################################################################################
# FUZZY MATCHING FUNCTIONS
################################################################################
def match_single_string(in_string, match_df,
                        match_df_string_col,
                        match_df_return_col,
                        top_score_cutoff=90,
                        dist_to_second_score_cutoff=20):
    '''Define the fuzzy matching rules for a single string being matched to all
    values in a dataframe
    '''
    # Get the score for each potential match
    match_df['__fuzzy_scores'] = match_df[match_df_string_col].apply(str).apply(
                                   lambda x: fuzz.token_sort_ratio(in_string,x))
    # Get the top two scores
    two_largest = match_df.nlargest(n=2, columns='__fuzzy_scores')
    top_score = two_largest['__fuzzy_scores'].iloc[0]
    second_score = two_largest['__fuzzy_scores'].iloc[1]
    if ((top_score >= top_score_cutoff) and
         dist_to_second_score_cutoff >= (top_score - second_score)):
        return two_largest[match_df_return_col].iloc[0]
    else:
        return np.nan


def fuzzy_match_subnationals(in_df, loc_metadata, top_score_cutoff=90,
                             dist_to_second_score_cutoff=20):
    '''
    For all rows that have been matched to a country name but NOT to a subnational,
    where subnationals exist, try to fuzzy string match the location name to a
    subnational row.

    Inputs:
      in_df (pandas DataFrame): The input dataframe, already matched directly to
        GBD locations where possible
      location_set_id (int): The location set ID that will be used to pull GBD
        location metadata
      top_score_cutoff (int): Each string match receives a score from 0 to 100 
        based on the similarity between the two strings. This determines the 
        lowest possible score that the 'best' match can receive to be considered
        as a valid match
      dist_to_second_score_cutoff (int): For a clear match, the fuzzy string 
        matching program should (1) assign a high score to a single match and (2)
        NOT assign high scores to any other matches. This input determines the
        minimum acceptable 'distance' between the best and second-best matches
        offered by the fuzzy string matching program.
    '''
    # Get location IDs of countries that have subnationals
    need_more_detail = (loc_metadata.loc[(loc_metadata['level'] == 3) &
                               (loc_metadata['most_detailed']==0),'location_id']
                               .unique()
                               .tolist())
    # Drop any location IDs where no shocks data was matched to the ID
    all_matched_locs = in_df['location_id_matched'].unique().tolist()
    need_more_detail = [i for i in need_more_detail
                          if i in all_matched_locs]
    # Create a dataframe of all possible from-to matches in the data
    from_all = pd.DataFrame({'from':['admin1','admin2','location'],
                             '__merge_col':1})
    to_all = pd.DataFrame({'to':['location_name','location_name_short',
                                 'location_ascii_name'],
                           '__merge_col':1})
    from_to = (pd.merge(left=from_all, right=to_all, on=['__merge_col'])
                 .drop('__merge_col',axis=1))
    # Iterate through possible fuzzy matches within each country
    for natl_id in need_more_detail:
        # Subset down the location metadata dataframe to include only children of
        #  the given location id
        subnat_df = (loc_metadata
                        .loc[(loc_metadata['path_to_top_parent']
                                  .apply(lambda x: str(natl_id) in x)),:])
        # Iterate through possible location columns
        for mapping_index, mapping_row in from_to.iterrows():
            from_col = mapping_row['from']
            to_col = mapping_row['to']
            # Only iterate through rows where the location name is filled and 
            #  the matched location ID field is empty
            for index, row in (in_df.loc[(in_df[from_col].notnull()) & 
                                         (in_df['location_id_matched'] == natl_id),
                                         :].iterrows()):
                match_val = match_single_string(
                              in_string=str(row[from_col]),
                              match_df=subnat_df,
                              match_df_string_col=to_col,
                              match_df_return_col='location_id',
                              top_score_cutoff=top_score_cutoff,
                              dist_to_second_score_cutoff=
                                    dist_to_second_score_cutoff)
                if not(np.isnan(match_val)):
                    in_df.loc[index,'location_id_matched'] = match_val
    return in_df


################################################################################
# Run both direct and fuzzy string matching
################################################################################
def match_to_gbd_locations(in_df, location_set_id=21, fuzzy_match=True,
                           fm_top_cutoff=90, fm_dist_cutoff=20):
    print("Beginning direct string matching to GBD locations...")
    # Get location metadata for matching
    gbd_meta = get_location_metadata(location_set_id=21)
    ## Add some columns to the input dataframe without suffixes
    for col in ['admin1','admin2','admin3','location']:
        in_df["{}__short".format(col)] = in_df[col].apply(remove_suffixes)
    for col in ['location_name','location_name_short','location_ascii_name']:
        gbd_meta["{}__short".format(col)] = gbd_meta[col].apply(remove_suffixes)
    # Build the list of columns that will be matched, in order
    in_df_cols = list()
    meta_cols = list()
    for in_named_col in ['admin1','admin2','admin3','location']:
        for meta_named_col in ['location_name','location_name_short','location_ascii_name']:
            for suffix1 in ["","__short"]:
                for suffix2 in ["","__short"]:
                    in_df_cols.append("{}{}".format(in_named_col,suffix1))
                    meta_cols.append("{}{}".format(meta_named_col,suffix2))
    in_df_cols = in_df_cols + ["location_id","iso","country","country","country"]
    meta_cols = meta_cols + ["location_id","ihme_loc_id","location_name",
                             "location_name_short","location_ascii_name"]
    ## Iteratively merge, adding only NaN columns on each merge
    # Subset to most detailed for a first run
    meta_most_detailed = gbd_meta.loc[gbd_meta['most_detailed']==1,:]
    # FIRST, run only the most detailed locations
    in_df['location_id_matched'] = np.nan
    joined_df = unified_location_column(in_df,
                        match_df=meta_most_detailed,
                        location_columns=in_df_cols,
                        match_columns=meta_cols,
                        match_column_to_add='location_id',
                        new_column_name="location_id_matched")
    # NEXT, run on all locations to catch any not-most-detailed location matches
    joined_df = unified_location_column(in_df,
                        match_df=gbd_meta,
                        location_columns=in_df_cols,
                        match_columns=meta_cols,
                        match_column_to_add='location_id',
                        new_column_name="location_id_matched")
    # Use the new location data to join on the 'most-detailed' column
    joined_df = pd.merge(left=joined_df,
                         right=(gbd_meta.loc[:,['location_id','most_detailed']]
                                   .rename(columns={'location_id':'location_id_matched',
                                                    'most_detailed':'already_located'})),
                         on='location_id_matched',
                         how='left')
    # If fuzzy_match is true, try using fuzzy string matching to match countries
    #  to their subnational locations
    if fuzzy_match:
        print("Beginning fuzzy matching to GBD locations...")
        joined_df = fuzzy_match_subnationals(in_df=joined_df,
                        loc_metadata=gbd_meta, top_score_cutoff=fm_top_cutoff,
                        dist_to_second_score_cutoff=fm_dist_cutoff)
    # Cleanup
    joined_df.loc[joined_df['location_id'].isnull(),
                  'location_id'] = joined_df.loc[joined_df['location_id'].isnull(),
                                             'location_id_matched']
    joined_df = joined_df.drop(['admin1__short','admin2__short',
                                'admin3__short','location_id_matched'], axis=1)
    return joined_df



if __name__ == "__main__":
    # SET FILEPATHS AND SETTINGS
    shocks_dir = 'FILEPATH'
    in_data_file = join(shocks_dir,'FILEPATH')
    map_file = join(shocks_dir,'FILEPATH')
    out_file = join(shocks_dir,'FILEPATH')
    csv_encoding = 'latin1'

    # Read input data
    in_data = read_pandas(in_data_file, encoding=csv_encoding)
    # Create a location 

    # Map locations using a dictionary
    mapped = location_dictionary_map(in_df=in_data, map_df_path=map_file)
    # Map variants of location names to official GBD location names, then try
    #  using fuzzy matching to get at simple misspellings
    gbd_standard = match_to_gbd_locations(in_df=mapped.copy(),
                        location_set_id=21,
                        fuzzy_match=False)
    # Save to the output file
    save_pandas(gbd_standard, filepath=out_file, encoding=csv_encoding)