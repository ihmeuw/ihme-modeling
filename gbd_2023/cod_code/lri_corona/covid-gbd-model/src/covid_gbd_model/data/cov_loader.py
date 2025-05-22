from typing import List
import pandas as pd

from covid_gbd_model.data.forecasting_products import (
    load_infections,
    load_effective_vaccinations,
    load_infected,
    load_immunity,
    load_idr,
    load_total_covid_asdr,
)
from covid_gbd_model.data.gbd_db import (
    load_gbd_covariate,
    load_gbd_prevalence,
)
from covid_gbd_model.variables import (
    VARIANTS,
    GBD_COVARIATES,
    GBD_CAUSES,
)


def expand_age_sex(data: pd.Series, age_group_ids: List[int]):
    if 'age_group_id' not in data.index.names:
        data = pd.concat([data] * len(age_group_ids), keys=age_group_ids, names=['age_group_id'])
    else:
        data_ages = data.index.get_level_values('age_group_id').unique()
        if set(age_group_ids) != set(data_ages):
            raise ValueError('Age present in dataset, but do not match expected `age_group_id` list.')
    if 'sex_id' not in data.index.names:
        data = pd.concat([data] * 2, keys=[1, 2], names=['sex_id'])
    else:
        data_sexes = data.index.get_level_values('sex_id').unique()
        if set([1, 2]) != set(data_sexes):
            raise ValueError('Age present in dataset, but do not match expected `age_group_id` list.')

    data = data.reorder_levels(['location_id', 'year_id', 'age_group_id', 'sex_id']).sort_index()

    return data


def fill_missing_locations(data: pd.Series, location_metadata: pd.DataFrame) -> pd.Series:
    if data.index.names[0] != 'location_id':
        raise ValueError('Expecting location_id to be first index level.')

    data_locations = data.index.get_level_values('location_id').unique().tolist()
    missing = (
        location_metadata
        .loc[
            (location_metadata['model_location'] == 1)
            & (~location_metadata['location_id'].isin(data.index.get_level_values('location_id').unique())),
        ]
        .loc[:, ['location_id', 'level', 'path_to_top_parent']]
        .rename(columns={'location_id': 'child_location_id'})
    )

    missing_parent_ids = [i for i in missing['child_location_id'].to_list() if location_metadata['path_to_top_parent'].str.contains(f',{i},').any()]
    if any([missing_parent_id in data_locations for missing_parent_id in missing_parent_ids]):
        raise ValueError(f'Model locations require aggregation for {data.name}.')

    missing['location_id'] = missing['path_to_top_parent'].apply(lambda x: [int(i) for i in x.split(',') if int(i) in data_locations][-1])
    missing['n_inheritance_levels'] = missing['path_to_top_parent'].apply(lambda x: (~pd.Series([int(i) in data_locations for i in reversed(x.split(','))]).cummax()).sum()).sort_values()
    is_l3_l0 = missing['level'] <= 3
    is_inheritance_above_2 = missing['n_inheritance_levels'] > 2
    is_inheritance_above_1 = missing['n_inheritance_levels'] > 1

    if (is_inheritance_above_2 | (is_l3_l0 & is_inheritance_above_1)).any():
        raise ValueError('Invalid inheritance (more than two levels for subnat or more than one level for national).')

    child_data = []
    for parent_id, location_id in missing.set_index('location_id').loc[:, 'child_location_id'].items():
        child_data.append(
            pd.concat([data.loc[parent_id]], keys=[location_id], names=['location_id'])
        )
    data = pd.concat([data] + child_data).sort_index()

    data = data.loc[
        location_metadata.loc[
            location_metadata['model_location'] == 1, 'location_id'
        ]
    ]

    return data


def load_covs(
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
):
    covariates = (
        [load_infections(variant) for variant in VARIANTS]
        + [load_effective_vaccinations()]
        + [load_infected()]
        + [load_immunity()]
        + [load_idr(location_metadata)]
        + [load_total_covid_asdr()]
        + [load_gbd_covariate(covariate, covariate_id, demog_spec, age_metadata) for covariate, (covariate_id, demog_spec) in GBD_COVARIATES.items()]
        + [load_gbd_prevalence(cause, cause_id, age_metadata) for cause, cause_id in GBD_CAUSES.items()]
    )
    covariates = [fill_missing_locations(covariate, location_metadata) for covariate in covariates]
    covariates = [expand_age_sex(covariate, age_metadata['age_group_id'].to_list()) for covariate in covariates]
    covariates = pd.concat(covariates, axis=1)

    return covariates
