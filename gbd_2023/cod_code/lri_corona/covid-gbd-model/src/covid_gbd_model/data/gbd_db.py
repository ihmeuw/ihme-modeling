from typing import List

import pandas as pd
import numpy as np

import db_queries

from covid_gbd_model.variables import (
    MODEL_YEARS,
    RELEASE_ID,
    PREVALENCE_RELEASE_ID,
)


def load_gbd_covariate(
    covariate: str, covariate_id: int, demog_spec: str, age_metadata: pd.DataFrame, release_id: int = RELEASE_ID, model_years: List[int] = MODEL_YEARS
) -> pd.Series:
    if demog_spec == 'detailed':
        demog_dict = dict(
            age_group_id=age_metadata['age_group_id'].to_list(),
            sex_id=[1, 2],
        )
    elif demog_spec == 'aggregate':
        demog_dict = dict()
    data = db_queries.get_covariate_estimates(
        release_id=release_id,
        covariate_id=covariate_id,
        location_id='all',
        year_id=model_years,
        **demog_dict
    )

    id_vars = []
    for id_var in ['location_id', 'year_id', 'age_group_id', 'sex_id']:
        if data[id_var].unique().size > 1:
            id_vars.append(id_var)
    data = data.set_index(id_vars).loc[:, 'mean_value'].rename(covariate)

    return data


def load_gbd_prevalence(cause: str, cause_id: int, age_metadata: pd.DataFrame, release_id: int = PREVALENCE_RELEASE_ID, model_years: List[int] = MODEL_YEARS) -> pd.Series:
    # using GBD 2021
    data = db_queries.get_outputs(
        'cause',
        release_id=release_id,
        measure_id=5,
        metric_id=3,
        cause_id=cause_id,
        location_id='all',
        year_id=model_years,
        age_group_id=age_metadata['age_group_id'].to_list(),
        sex_id=[1, 2],
    )

    data = (
        data
        .set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'])
        .loc[:, 'val']
        .sort_index()
        .rename(cause)
    )
    if release_id == 9:
        data = (
            data
            .groupby(['location_id', 'age_group_id', 'sex_id']).ffill()
            .sort_index()
        )

    return data


def load_gbd_metadata(release_id: int = RELEASE_ID):
    location_metadata = db_queries.get_location_metadata(location_set_id=35, release_id=release_id)
    model_locations = (
        location_metadata
        .loc[
            (location_metadata['level'] == 3)
            | (location_metadata['most_detailed'] == 1)
            | (location_metadata['parent_id'].isin([163]))  # India states, ...(others?)...
        ]
        .loc[:, 'location_id']
        .to_list()
    )
    location_metadata['model_location'] = 0
    location_metadata.loc[location_metadata['location_id'].isin(model_locations), 'model_location'] = 1

    location_metadata_covariate = db_queries.get_location_metadata(location_set_id=22, release_id=release_id)

    age_metadata = db_queries.get_age_metadata(age_group_set_id=24, release_id=release_id).sort_values('age_group_years_start').reset_index(drop=True)
    age_metadata['age_mid'] = age_metadata.loc[:, ['age_group_years_start', 'age_group_years_end']].mean(axis=1)

    population = db_queries.get_population(
        release_id=release_id,
        location_id=pd.concat([location_metadata['location_id'], location_metadata_covariate['location_id']]).unique().tolist(),
        year_id='all',
        age_group_id='all',
        sex_id='all',
    ).set_index(['location_id', 'year_id', 'age_group_id', 'sex_id']).loc[:, 'population']
    
    return location_metadata, location_metadata_covariate, age_metadata, population


def load_cod_data(
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    release_id: int = RELEASE_ID, model_years: List[int] = MODEL_YEARS
) -> pd.DataFrame:

    # ONLY INCLUDING VR
    data = db_queries.get_cod_data(
        release_id=release_id,
        cause_id=1048,
        year_id=model_years,
        age_group_id=age_metadata['age_group_id'].to_list(),
        sex_id=[1, 2],
    )
    data = data.loc[data['data_type_name'].str.contains('VR')]

    # data drops -- 
    china_locations = location_metadata.loc[location_metadata['path_to_top_parent'].apply(lambda x: '6' in x.split(',')), 'location_id'].to_list()
    is_china_location = data['location_id'].isin(china_locations)
    data = data.loc[~is_china_location].reset_index(drop=True)
    is_uk = data['location_id'] == 95
    data = data.loc[~is_uk].reset_index(drop=True)

    is_bangladesh = data['location_id'] == 161
    data = data.loc[~is_bangladesh].reset_index(drop=True)

    is_who_icd10 = data['cod_source_label'] == 'ICD10'
    is_guatemala = data['location_id'] == 128
    data = data.loc[~(is_guatemala & is_who_icd10)].reset_index(drop=True)

    data['raw_rate'] = (data['cf_raw'] * data['env']) / data['pop']

    data = data.rename(columns={'rate': 'corr_obs', 'raw_rate': 'raw_obs'})
    data = data.set_index(['data_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']).loc[:, ['raw_obs', 'corr_obs', 'pop']]

    if 4749 not in data.index.get_level_values('location_id').unique():
        england_regions = location_metadata.loc[location_metadata['parent_id'] == 4749, 'location_id'].to_list()
        england_data = data.loc[:, england_regions, :, :, :]
        for obs_var in ['raw_obs', 'corr_obs']:
            england_data[obs_var] *= england_data['pop']
        england_data = england_data.groupby(['year_id', 'age_group_id', 'sex_id']).sum()
        for obs_var in ['raw_obs', 'corr_obs']:
            england_data[obs_var] /= england_data['pop']
        england_data = pd.concat([england_data], keys=[4749], names=['location_id'])
        england_data['data_id'] = -(np.arange(len(england_data)) + 1)
        england_data = england_data.set_index('data_id', append=True)
        england_data = england_data.reorder_levels(data.index.names)
        data = pd.concat([data, england_data])

    data = data.drop('pop', axis=1)

    return data
