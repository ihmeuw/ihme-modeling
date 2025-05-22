import numpy as np
import pandas as pd

from covid_gbd_model.data.gbd_db import load_cod_data
from covid_gbd_model.data.provisional import (
    map_most_detailed_age_groups,
    load_extraction_metadata,
    EXTRACTION_FUNCTION_MAP,
)
from covid_gbd_model.data.surveillance import load_surveillance_data


def get_location_years(data: pd.Series, location_metadata: pd.DataFrame):
    location_years = (
        data.reset_index().loc[:, ['location_id', 'year_id']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    expanded_locations = {
        location_id: [
            int(i) for i in np.unique(
                np.hstack(
                    location_metadata
                    .loc[
                        location_metadata['path_to_top_parent'].apply(lambda x: str(location_id) in x.split(',')), 'path_to_top_parent'
                    ].str.split(',').values
                )
            )
        ]
        for location_id in location_years['location_id'].unique()
    }
    location_years = (
        location_years
        .drop('location_id', axis=1)
        .join(location_years['location_id'].map(expanded_locations).explode().rename('location_id'))
        .drop_duplicates()
        .set_index(['location_id', 'year_id'])
        .index
    )

    return location_years


def load_obs(location_metadata: pd.DataFrame, age_metadata: pd.DataFrame):
    extraction_metadata = load_extraction_metadata()

    cod_data = load_cod_data(location_metadata, age_metadata)

    provisional_data = []
    for country, extraction_function in EXTRACTION_FUNCTION_MAP.items():
        inputs = extraction_metadata.loc[country]
        provisional_data.append(
            extraction_function(country, location_metadata, age_metadata, *inputs)
        )
    provisional_data = pd.concat(provisional_data).rename('deaths').to_frame()

    surveillance_data = load_surveillance_data(location_metadata)

    cod_data_location_years = get_location_years(cod_data, location_metadata)
    provisional_data_location_years = get_location_years(provisional_data, location_metadata)
    vr_data_location_years = cod_data_location_years.union(provisional_data_location_years)

    surveillance_data = surveillance_data.rename('deaths').to_frame()
    model_surveillance_data = surveillance_data.drop(vr_data_location_years, errors='ignore')
    overlap_surveillance_data = (
        surveillance_data
        .drop(model_surveillance_data.index)
        .drop(provisional_data_location_years, errors='ignore')
    )

    age_map = map_most_detailed_age_groups(provisional_data, age_metadata)

    obs_dict = {
        'cod_data': cod_data,
        'provisional_data': provisional_data,
        'surveillance_data': surveillance_data,
        'model_surveillance_data': model_surveillance_data,
        'overlap_surveillance_data': overlap_surveillance_data,
        'age_map': age_map,
    }

    return obs_dict
