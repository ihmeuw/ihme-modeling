import pandas as pd

import db_queries

from covid_historical_model.utils.misc import parent_inheritance


def obesity(hierarchy: pd.DataFrame) -> pd.DataFrame:
    data = db_queries.get_covariate_estimates(
        gbd_round_id=6,
        decomp_step='iterative',
        covariate_id=455,  # Prevalence of obesity (age-standardized)
        year_id=2019,
    )
    
    # just averaging sexes here...
    data = (data
            .groupby('location_id')['mean_value'].mean()
            .rename('obesity')
            .to_frame())
    
    # pass down hierarchy
    data = parent_inheritance(data, hierarchy)
    data = data.squeeze()
    
    return data


def smoking(hierarchy: pd.DataFrame) -> pd.DataFrame:
    data = db_queries.get_covariate_estimates(
        gbd_round_id=6,
        decomp_step='iterative',
        covariate_id=282,  # Smoking Prevalence (Age-standardized, both sexes)
        year_id=2019,
    )
    data = (data
            .loc[:, ['location_id', 'mean_value']]
            .rename(columns={'mean_value': 'smoking'})
            .set_index('location_id')
            .sort_index())
        
    # pass down hierarchy
    data = parent_inheritance(data, hierarchy)
    data = data.squeeze()
    
    return data


def uhc(hierarchy: pd.DataFrame) -> pd.DataFrame:
    data = db_queries.get_covariate_estimates(
        gbd_round_id=6,
        decomp_step='iterative',
        covariate_id=1097,  # Universal health coverage
        year_id=2019,
    )
    data = (data
            .loc[:, ['location_id', 'mean_value']]
            .rename(columns={'mean_value': 'uhc'})
            .set_index('location_id')
            .sort_index())
        
    # pass down hierarchy
    data = parent_inheritance(data, hierarchy)
    data = data.squeeze()
    
    return data


def haq(hierarchy: pd.DataFrame) -> pd.DataFrame:
    data = db_queries.get_covariate_estimates(
        gbd_round_id=6,
        decomp_step='iterative',
        covariate_id=1099,  # Healthcare access and quality index
        year_id=2019,
    )
    data = (data
            .loc[:, ['location_id', 'mean_value']]
            .rename(columns={'mean_value': 'haq'})
            .set_index('location_id')
            .sort_index())
        
    # pass down hierarchy
    data = parent_inheritance(data, hierarchy)
    data = data.squeeze()
    
    return data


def get_cause_data(hierarchy: pd.DataFrame, cause_id: int, var_name: str,) -> pd.DataFrame:
    data = db_queries.get_outputs(
        'cause',
        gbd_round_id=6,
        decomp_step='step5',
        version='best',
        location_id='all',
        year_id=2019,
        age_group_id=27,
        sex_id=3,
        measure_id=5,
        metric_id=3,
        cause_id=cause_id,
    )
    data = (data
            .loc[:, ['location_id', 'val']]
            .rename(columns={'val': var_name})
            .set_index('location_id')
            .sort_index())
    
    # pass down hierarchy
    data = parent_inheritance(data, hierarchy)
    data = data.squeeze()
    
    return data


def diabetes(hierarchy: pd.DataFrame) -> pd.DataFrame:
    # Diabetes mellitus
    data = get_cause_data(hierarchy, 587, 'diabetes',)
    
    return data


def ckd(hierarchy: pd.DataFrame) -> pd.DataFrame:
    # Chronic kidney disease
    data = get_cause_data(hierarchy, 589, 'ckd',)
    
    return data


def cancer(hierarchy: pd.DataFrame) -> pd.DataFrame:
    # Neoplasms
    data = get_cause_data(hierarchy, 410, 'cancer',)
    
    return data


def copd(hierarchy: pd.DataFrame) -> pd.DataFrame:
    # Chronic obstructive pulmonary disease
    data = get_cause_data(hierarchy, 509, 'copd',)
    
    return data


def cvd(hierarchy: pd.DataFrame) -> pd.DataFrame:
    # Cardiovascular diseases
    data = get_cause_data(hierarchy, 491, 'cvd',)

    return data


def age_metadata() -> pd.DataFrame:
    data = db_queries.get_age_metadata(
        gbd_round_id=6,
        age_group_set_id=12,
    )
    
    data = data.loc[data['age_group_years_start'] >= 5, ['age_group_years_start', 'age_group_years_end', 'age_group_id']]
    data.loc[data['age_group_years_end'] != 125, 'age_group_years_end'] -= 1
    
    data = pd.concat([pd.DataFrame({'age_group_years_start': 0, 'age_group_years_end': 4, 'age_group_id': 1}, index=[0]),
                      data])
    
    for col in data.columns:
        data[col] = data[col].astype(int)
    
    return data
