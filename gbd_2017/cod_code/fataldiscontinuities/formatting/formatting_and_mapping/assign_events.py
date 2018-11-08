import numpy as np
import pandas as pd
import types


def manual_map_merge(db, path_to_merge_file, encoding='utf8'):
    '''
    Outputs:
      db_mapped: Merged database containing new mappings
    '''
    # Read in the mappings file
    m_maps = (pd.read_csv(path_to_merge_file,encoding=encoding)
                .rename(columns={'event_type':'cause_mapping'}))


    # Make unique keys in both the db and mapping file
    def drop_trailing_digits(num):
        num = str(num)
        if '.' in num:
            return num[:num.index('.')]
        else:
            return num
    def add_merge_key_field(in_df):
        merge_key = (in_df.apply(lambda x: "{}_{}_{}_{}_{}_{}".format(
                                             x['dataset'],
                                             x['dataset_event_type'],
                                             drop_trailing_digits(x['nid']),
                                             x['country'],
                                             drop_trailing_digits(x['year']),
                                             x['dataset_notes']
                                             ), axis=1))
        return merge_key
    db['__manual_merge_key'] = add_merge_key_field(db)
    m_maps['__manual_merge_key'] = add_merge_key_field(m_maps)
    m_maps = m_maps.loc[:,['__manual_merge_key','cause_mapping']]
    # Merge the new mappings onto the WHOLE dataframe using the unique key
    db_mapped = pd.merge(left=db,
                         right=m_maps,
                         on=['__manual_merge_key'],
                         how='left')
    # Add the merged mappings onto the dataframe ONLY where the match
    #  was good and where a notes field exists (showing that mapped that
    #  subset of the data)
    db_mapped.loc[(db_mapped['dataset_notes'].notnull()) & 
                  (db_mapped['cause_mapping'].notnull()),
                  'event_type'] = db_mapped.loc[
                                      (db_mapped['dataset_notes'].notnull()) & 
                                      (db_mapped['cause_mapping'].notnull()),
                                      'cause_mapping']
    # Drop the mapping key and mapping column
    db_mapped = db_mapped.drop(labels=['cause_mapping','__manual_merge_key'],
                               axis=1,errors='ignore')
    # Return the mapped dataframe
    return db_mapped


def no_note_event_merge(in_df, path_to_merge_file, encoding='utf8'):
    
    # Read in the merge file
    map_df = pd.read_csv(path_to_merge_file, encoding=encoding)
    map_df['add_mapping'] = map_df['add_mapping'].str.strip().str.upper()
    # Create a key linking the data file and the merge file
    def create_match_key(df):
        match_key = (df['dataset']
                        .apply(str).str.strip().str.lower()
                        .str.cat(others=(df['dataset_event_type']
                                           .apply(str).str.strip().str.lower()),
                                 sep="_"))
        return match_key
    in_df['__match_on'] = create_match_key(in_df)
    map_df['__match_on'] = create_match_key(map_df)
    map_df = map_df.loc[:,["__match_on","add_mapping"]]
    # Merge the two files
    mapped_df = pd.merge(left=in_df,
                         right=map_df,
                         on=["__match_on"],
                         how='left')
    # Add new event types, where available
    new_locs = mapped_df['event_type'].isnull()
    mapped_df.loc[new_locs,'event_type'] = (mapped_df.loc[new_locs,'add_mapping'])
    # Drop extraneous columns
    mapped_df = mapped_df.drop(labels=['add_mapping','__match_on'],
                               axis=1, errors='ignore')
    return mapped_df


def standardize_event_types(in_df):
    '''
    This is a helper function used to standardize event type codes in the data
    '''
    in_df['event_type'] = (in_df['event_type']
                             .apply(str)
                             .str.strip()
                             .str.upper()
                             .str.replace('NAN',"??")
                             .str.replace(r'^\s*$',"??")
                             .fillna("??"))
    return in_df


def override_event_assignment(in_df):

    in_df.loc[
      (in_df['country'] == "Iceland") &
      (in_df['year'] == 1995) &
      (in_df['dataset'] == "EMDAT") &
      (in_df['event_type'] == "A4"),
      'event_type'
    ] = "A2"

    in_df.loc[
      (in_df['country'] == "China") &
      (in_df['admin1'] == "Beijing") &
      (in_df['year'] == 1989) &
      (in_df['dataset'] == "GED") &
      (in_df['event_type'] == "B1"),
      'event_type'
    ] = "D7"

    in_df.loc[
      (in_df['iso'] == "LBY") &
      (in_df['year'] == 2011) &
      (in_df['dataset'] == "war_supplement_2014a") &
      (in_df['event_type'] == "D7"),
      'event_type'
    ] = "B1"

    in_df.loc[
      (in_df.country == 'Indonesia') &
      (in_df.year.isin(range(1975, 1989))) &
      (in_df.dataset == 'PRIO_BDD') &
      (in_df.event_type == 'D7'),
      'event_type'
    ] = 'B1'

    in_df.loc[
      (in_df.country == 'South Sudan') &
      (in_df.year.isin([2012, 2013])) &
      (in_df.dataset == 'ACLED_Africa') &
      (in_df.event_type == 'D7'),
      'event_type'
    ] = 'B1'

    in_df.loc[
      (in_df.country == 'Iraq') &
      (in_df.year.isin(range(1979, 1988))) &
      (in_df.dataset == 'PRIO_BDD') &
      (in_df.event_type == 'D7'),
      'event_type'
    ] = 'B1'

    return in_df

def assign_events(shock_db):
    # Add manual changes for GBD 2017\
    path_to_changes = ("")
    shock_db = manual_map_merge(shock_db,
                                       path_to_merge_file=path_to_changes,
                                       encoding='latin1')
    print("transform: manual cause mappings applied")
    # Assign events for records without a "dataset_notes" field
    no_notes_match_file = ("")
    shock_db = no_note_event_merge(in_df=shock_db,
                                   path_to_merge_file=no_notes_match_file,
                                   encoding='latin1')
    # Standardize the event_type field
    shock_db = standardize_event_types(shock_db)

    # Apply manual event_type overrides:
    shock_db = override_event_assignment(shock_db)
    
    try:
        assert (shock_db.event_type == '').any(
        ) == False, 'Missing event_types!'
    except AssertionError:
        missing_df = shock_db[shock_db.event_type == '']
        missing_df = missing_df.groupby('dataset')['dataset'].count()
        print("These datasets have missing event_types:")
        print(missing_df)
    print('transform: events assigned')

    return shock_db