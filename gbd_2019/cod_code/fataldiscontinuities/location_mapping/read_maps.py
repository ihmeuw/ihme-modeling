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


def read_loc_mapping(source, map_type):
    p = SOURCES_DIR.joinpath(source, 'location_mapping', 'inputs',
                             f"{source}_{map_type.value}_location_map.csv")
    if p.exists():
        df = pd.read_csv(p)
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
