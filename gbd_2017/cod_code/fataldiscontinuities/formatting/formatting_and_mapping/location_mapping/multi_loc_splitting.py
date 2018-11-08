# Imports (general)
import numpy as np
import pandas as pd

from db_queries import get_population, get_location_metadata


def split_by_side(in_df, sides_to_countries_filepath):

    side_to_country = pd.read_excel(sides_to_countries_filepath)
    side_to_country = side_to_country.loc[side_to_country['location_id'].notnull(),:]
    side_to_location_map = dict(zip(side_to_country['this_side'],side_to_country['location_id']))
    all_sides_list = side_to_country['this_side'].unique().tolist()
    # Split out countries that have the appropriate amount of data to split by side
    has_sides = ((in_df['side_a'].notnull()) & 
                 (in_df['side_b'].notnull()) & 
                 (in_df['deaths_a'].notnull()) & 
                 (in_df['deaths_b'].notnull()) &
                 (in_df['side_a'].isin(all_sides_list)) &
                 (in_df['side_b'].isin(all_sides_list)))
    # Subset to get only the rows that need splitting by side
    # These are added to the dataframe afterwards
    no_sides_df = in_df.loc[~has_sides,:]
    has_sides_df = in_df.loc[has_sides,:]
    has_sides_df['deaths_civilians'] = has_sides_df['deaths_civilians'].fillna(0)
    has_sides_df['deaths_unknown'] = has_sides_df['deaths_unknown'].fillna(0)
    # Split all rows where sides are listed
    rows_split = list()
    for index, row in has_sides_df.iterrows():
        side_a_name = row['side_a']
        side_a_location_id = side_to_location_map[side_a_name]
        side_a_deaths = row['deaths_a']
        side_b_name = row['side_b']
        side_b_location_id = side_to_location_map[side_b_name]
        side_b_deaths = row['deaths_b']
        local_deaths = row['deaths_civilians'] + row['deaths_unknown']
        row = row.drop(labels=['side_a','side_b','deaths_a','deaths_b',
                               'deaths_civilians','deaths_unknown','lower','upper'],
                       errors='ignore')
        row['split_from_sides'] = 1
        # Add deaths from side A
        row_a = row.copy(deep=True)
        row_a = row_a.drop(labels=['country','admin1','admin2','admin3'],errors='ignore')
        row_a['country'] = side_a_name
        row_a['location_id'] = side_a_location_id
        row_a['best'] = side_a_deaths
        # Add deaths from side B
        row_b = row.copy(deep=True)
        row_b = row_b.drop(labels=['country','admin1','admin2','admin3'],errors='ignore')
        row_b['country'] = side_b_name
        row_b['location_id'] = side_b_location_id
        row_b['best'] = side_b_deaths
        # Add civilian and unknown deaths
        if local_deaths > 0:
            row['best'] = local_deaths
        rows_split = rows_split + [row_a, row_b, row]
    # Concatenate into a new dataframe
    split_by_side = pd.concat(rows_split,axis=1).T
    overall = pd.concat([no_sides_df,split_by_side])
    overall['split_from_sides'] = overall['split_from_sides'].fillna(0)
    # Return the joined dataframe
    return overall



def assemble_most_detailed_map(not_detailed_locs, loc_meta):
    '''
    For each of the locations that is not listed as most detailed, create a 
    dataframe of all the most deatiled locations that fall within that 
    non-detailed location.
    Inputs:
      not_detailed_locs: A list of all locations to create the dataframe for
      loc_meta: Dataframe of location metadata for a given location set
    Outputs:
      detailed_map: A dataframe with three fields: "map_from_loc", the non-detailed
        location_id; "map_to_loc", the most detailed location_ids, and 
        "split_fraction" the proportion of the total population in the
        non-detailed loc that can be found within the most detailed loc. 
    '''
    # Keep only most detailed locations
    detailed = loc_meta.loc[loc_meta['most_detailed']==1,:]
    # Get population data for all most detailed locations
    pops = (get_population(location_id=detailed['location_id'].unique().tolist())
                    .loc[:,['location_id','population']])
    detailed = pd.merge(left=detailed,
                        right=pops,
                        on=['location_id'],
                        how='inner')
    # Create an empty list that will store subsets of the final dataframe
    sub_map_dfs = list()
    # Iterate through all of the non-detailed locations
    not_detailed_locs = list(set(not_detailed_locs))
    for parent_loc in not_detailed_locs:
        # Subset to all most-detailed locations that fall under this location
        sub_df = (detailed.loc[detailed['path_to_top_parent'].apply(
                                   lambda x: ",{},".format(int(parent_loc)) in x),:])
        # Get the fraction of the total population in each most detailed location
        sub_df['split_fraction'] = sub_df['population'] / sub_df['population'].sum()
        # Keep only needed columns
        sub_df = sub_df.loc[:,['location_id','split_fraction']]
        sub_df['map_from_loc'] = parent_loc
        # Append to the list of sub-dataframes
        sub_map_dfs.append(sub_df)
    # Concatenate all sub-dataframes
    detailed_map = (pd.concat(sub_map_dfs)
                      .rename(columns={"location_id":"map_to_loc"}))
    return detailed_map


def split_to_most_detailed(in_df, location_id_col,
                           death_cols=['best','low','high','deaths_a','deaths_b',
                                'deaths_civilians','deaths_unknown'],
                           location_set_id=21):
    '''
    Split any GBD locations that are not the most detailed in a location
    hierarchy into the most detailed locations in that hierarchy. Split all the 
    death columns proportional to the population of each most detailed location
    within a given non-detailed location.
    Inputs:
      in_df (pandas dataframe): The full input dataset, with locations already
        standardized to GBD locations where possible
      location_id_col (string): The location ID column name in the input data
      death_cols (list): All input data columns containing deaths that must
        be split
      location_set_is (int): The location set that will be used to determine 
        the most detailed locations in a hierarchy.
    Output:
      split_df (pandas dataframe): The data after it has been split into most
        detailed locations. Includes a new column, "loc_not_detailed", that will
        contain the original GBD location IDs that were assigned to records that
        were split using this method.
    '''
    # Read in location metadata
    meta = get_location_metadata(location_set_id=location_set_id)
    # Determine which locations need to be split
    not_detailed_df = pd.merge(left=(in_df.loc[:,[location_id_col]]
                                        .drop_duplicates()
                                        .rename(columns={location_id_col:"location_id"})),
                               right=meta.loc[:,['location_id','most_detailed']],
                               on=['location_id'],
                               how='left')
    not_detailed_list = (not_detailed_df.loc[not_detailed_df['most_detailed']==0,
                                             'location_id']
                                        .unique().tolist())
    # Create a dictionary for mapping these locations to the most detailed
    #  locations in the hierarchy
    parent_map = (assemble_most_detailed_map(not_detailed_locs=not_detailed_list,
                                            loc_meta=meta)
                        .rename(columns={'map_from_loc':location_id_col}))
    # Merge on the dictionary
    split_df = pd.merge(left=in_df,
                     right=parent_map,
                     on=[location_id_col],
                     how='left')
    split_df['loc_not_detailed'] = np.nan
    # Determine which records have been split
    has_split = split_df['map_to_loc'].notnull()
    # For any records that have a split: 
    # - assign the old, non-detailed location ID to the 'loc_not_detailed' field
    split_df.loc[has_split,'loc_not_detailed'] = split_df.loc[has_split,location_id_col]
    # - assign the most detailed location IDs to the main location ID field
    split_df.loc[has_split,location_id_col] = split_df.loc[has_split,'map_to_loc']
    # - multiply all death fields by the split proportion in each most detailed loc
    for col in death_cols:
        split_df.loc[has_split,col] = (split_df.loc[has_split,col] * 
                                       split_df.loc[has_split,'split_fraction'])   
    # Drop unnecessary columns
    split_df = split_df.drop(labels=['map_to_loc','split_fraction'],
                             axis=1, errors='ignore')
    # Return the split dataframe
    return split_df



def create_multi_loc_dict(multi_loc_map_path):
    '''
    Reads in a dataframe that links records with multiple locations to individual
    GBD locations (across multiple rows). Assigns the proportion of the deaths that
    should be assigned to each of these GBD locations based on the proportional
    population sizes of each location.
    '''
    multi_loc_map = pd.read_excel(multi_loc_map_path)
    # Iterate through each individual splitting pattern
    # Apply the following function to create a new field, 'split_fraction',
    #  dictating how deaths will be assigned to each country in the split
    def get_pop_fractions(split_countries, pops):
        # For each country to be split out from the group, get the country's
        #  (current) population
        split_countries = pd.merge(left=split_countries,
                                   right=pops,
                                   on=['split_to_id'],
                                   how='left')
        # Get the fractional proportion of the total grouped population in each
        #  country
        total_pop = split_countries['population'].sum()
        split_countries['split_fraction'] = split_countries['population']/total_pop
        # Drop the raw population column, as it is no longer needed
        split_countries = split_countries.drop(labels=['population'], axis=1)
        # Return the new dataframe
        return split_countries
    # Apply the function
    all_loc_ids = (multi_loc_map['split_to_id'].astype(np.int32)
                                               .unique().tolist())
    pop_df = (get_population(location_id=all_loc_ids)
                  .loc[:,['location_id','population']]
                  .rename(columns={'location_id':'split_to_id'}))
    grouped_fractions = (multi_loc_map
                            .groupby(by=['split_from_name'])
                            .apply(lambda x: get_pop_fractions(x,pops=pop_df))
                            .reset_index(drop=True))
    return grouped_fractions


def multi_loc_split(in_df,
                    death_cols=['best','low','high','deaths_a','deaths_b',
                                'deaths_civilians','deaths_unknown'],
                    multi_loc_map_path='FILEPATH'):
    '''
    For any records that actually refer to multiple countries/regions/admin units
    and are NOT equal to known GBD units, split the data into individual GBD
    locations (which can then be split into most-detailed regions if necessary)
    '''
    multi_loc_map = create_multi_loc_dict(multi_loc_map_path=multi_loc_map_path)
    # In preparation for location splitting, make sure that ALL death columns
    #  have been converted to numeric
    for col in death_cols:
        in_df[col] = pd.to_numeric(in_df[col],errors='coerce')
    # Create a column indicating whether or not the location was split
    in_df['multi_loc_split'] = 0
    # Iterate through each of the possible matching columns (country, admin1, 
    #  location, etc)
    match_cols = multi_loc_map['split_level'].unique().tolist()
    assert np.all([i in in_df.columns for i in match_cols])
    for match_col in match_cols:
        # Keep only the matching rows that are assigned to this level
        match_df = (multi_loc_map
                      .loc[multi_loc_map['split_level']==match_col,:]
                      .drop(labels=['split_level','split_to_name'],
                            axis=1, errors='ignore')
                      .rename(columns={'split_from_name':match_col}))
        # Left join on the data
        in_df = pd.merge(left=in_df,
                         right=match_df,
                         on=[match_col],
                         how='left')
        is_split = in_df['split_to_id'].notnull()
        # Add location IDs, where they exist
        in_df.loc[is_split,'location_id'] = in_df.loc[is_split,'split_to_id']
        # Update deaths where needed
        for col in death_cols:
            in_df.loc[is_split,col] = (in_df.loc[is_split,col]
                                       * in_df.loc[is_split,'split_fraction'])
        # Show that the new rows are based on a multi-location split
        in_df.loc[is_split,'multi_loc_split'] = 1
        # Drop join columns
        in_df = in_df.drop(labels=['split_to_id','split_fraction'],
                           axis=1, errors='ignore')
    # Location splitting has been completed
    # Return the dataframe
    return in_df


if __name__ == "__main__":
    # This file should only have functions
    pass