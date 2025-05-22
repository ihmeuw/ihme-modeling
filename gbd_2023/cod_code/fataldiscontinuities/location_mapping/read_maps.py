from pathlib import Path
from enum import Enum

import pandas as pd

SOURCES_DIR = Path('FILEPATH')


class LocationMaps(Enum):
    COLLABORATOR = 'collaborator'
    MANUAL = 'manual'
    STRING = 'string'
    GROUP = 'group'
    GPS = 'gps'
    OSM = 'open_street_maps'

def remap_gbd_2022_loc_changes(row, location_column, locations_ids_2022, remap_dict):
    locations = str(row[location_column])
    new_locs = []
    if str(locations) == "":
        new_locs = "nan"
    elif str(locations) == "nan":
        new_locs = "nan"
    else:
        for location in locations.split(","):
            location = int(float(location))
            if location in locations_ids_2022:
                new_locs.append(location)
            else:
                new_loc = remap_dict[location]
                if new_loc not in new_locs: 
                    new_locs.append(new_loc)
                else:
                    pass        
        new_locs = ", ".join(str(x) for x in new_locs)
    return new_locs

def adjust_mapping_for_gbd_transition(df):
    locs_22 = get_location_metadata(release_id=16, location_set_id=21)
    locs_21 = get_location_metadata(release_id=9, location_set_id=21)
    locations_ids_2022 = list(locs_22['location_id'].unique())
    locs_to_parent = locs_21[~locs_21['location_id'].isin(locs_22['location_id'])]
    
    locs_to_parent = locs_to_parent[['location_id','parent_id']]
    remap_dict = pd.Series(locs_to_parent.parent_id.values,index=locs_to_parent.location_id).to_dict()
    location_columns = ['location_id','side_a','side_b']
    for location_column in location_columns:
        if location_column in df.columns:
            df[location_column] = df.apply(lambda x: remap_gbd_2022_loc_changes(x, location_column, locations_ids_2022, remap_dict), axis=1)

    return df

def read_loc_mapping(source, map_type):
    p = "FILEPATH"
    if p.exists():
        try:
            df = pd.read_csv(p)
        except:
            df = pd.read_csv(p, encoding='cp1252')
        assert 'source_event_id' in df
        df['source_event_id'] = df['source_event_id'].astype(str)
        return df
    else:
        return None


def create_location_mapping_dataframe(source):
    all_locs = []

    for map_type in LocationMaps:
        df = read_loc_mapping(source, map_type)
        if df is None:
            continue

        df = (df
              .loc[:, ['source_event_id', 'location_id', 'side_a', 'side_b']]
              .set_index('source_event_id', verify_integrity=True)
              )

        for col in df:
            for sei, loc_ids in df[col].dropna().astype(str).str.split(',').iteritems():
                locs = {int(float(loc_id)) for loc_id in loc_ids if loc_id != 'nan'}
                if locs:
                    all_locs.extend([[sei, loc, col, map_type.value] for loc in locs])
    df = pd.DataFrame(all_locs, columns=['source_event_id',
                                         'location_id', 'source_col', 'map_type'])
    loc_map_dtype = pd.CategoricalDtype(
        categories=[v.value for v in LocationMaps], ordered=True)
    df['map_type'] = df['map_type'].astype(loc_map_dtype)
    return df
