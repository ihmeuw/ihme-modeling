from pathlib import Path
from typing import List

import pandas as pd
import numpy as np

from covid_gbd_model.paths import (
    COVID_SEIR_FIT_ROOT,
    COVID_SEIR_OUTPUTS_ROOT,
    TOTAL_COVID_ASDR_PATH,
)
from covid_gbd_model.variables import MODEL_YEARS


def copy_2023_to_2024(data: pd.Series):
    if data.index.get_level_values('year_id').max() >= 2024:
        raise ValueError('Not expecting 2024 values.')
    else:
        data_2024 = data.loc[:, [2023], :, :].reset_index()
        data_2024['year_id'] = 2024
        data_2024 = data_2024.set_index(data.index.names)
        data_2024 = data_2024.loc[:, data.name]
        data = pd.concat([data, data_2024]).sort_index()

        return data


def load_covid_location_hierarchy(covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT):
    hierarchy = pd.read_csv(covid_seir_outputs_root / 'output_miscellaneous' / 'hierarchy.csv')

    return hierarchy


def load_covid_team_population(age_group: str = 'all', covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT) -> pd.Series:
    if age_group != 'all':
        raise ValueError('Invalid age group specified.')
    population = pd.read_csv(covid_seir_outputs_root / 'output_miscellaneous' / 'populations.csv')
    population = population.set_index(['age_group_id', 'sex_id', 'location_id', 'year_id']).loc[:, 'population']
    population = population.loc[22, 3, :, 2019].sort_index()

    return population


def load_effective_vaccinations(covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT, model_years: List[int] = MODEL_YEARS) -> pd.Series:
    data = pd.read_csv(covid_seir_outputs_root / 'output_summaries' / 'cumulative_vaccinations_all_effective.csv')
    data['date'] = (pd.to_datetime(data['date']) + pd.Timedelta(days=25)).astype(str)
    data = data.loc[data['date'].str.endswith('12-31')]
    data['year_id'] = data['date'].str.split('-').str[0].astype(int)
    data = data.loc[data['year_id'] < 2024]
    data = data.set_index(['location_id', 'year_id']).loc[:, 'mean']

    population = load_covid_team_population()
    data = (data / population).rename('cum_eff_vacc')

    data = copy_2023_to_2024(data)
    data = data.loc[:, model_years]

    return data


def load_infected(covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT, model_years: List[int] = MODEL_YEARS) -> pd.Series:
    data = pd.read_csv(covid_seir_outputs_root / 'output_summaries' / 'cumulative_infections_naive.csv')
    data['date'] = (pd.to_datetime(data['date']) + pd.Timedelta(days=25)).astype(str)
    data = data.loc[data['date'].str.endswith('12-31')]
    data['year_id'] = data['date'].str.split('-').str[0].astype(int)
    data = data.loc[data['year_id'] < 2024]
    data = data.set_index(['location_id', 'year_id']).loc[:, 'mean']

    population = load_covid_team_population()
    data = (data / population).rename('cum_inf')

    data = copy_2023_to_2024(data)
    data = data.loc[:, model_years]

    return data


def load_immunity(covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT, model_years: List[int] = MODEL_YEARS):
    vaccinated = load_effective_vaccinations(covid_seir_outputs_root, model_years)
    infected = load_infected(covid_seir_outputs_root, model_years)

    data = (1 - (1 - vaccinated) * (1 - infected)).rename('immunity')

    return data


def load_infections(variant: str, covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT, model_years: List[int] = MODEL_YEARS) -> pd.Series:
    data = pd.read_csv(covid_seir_outputs_root / 'output_summaries' / f'daily_infections_{variant}.csv')
    data['date'] = (pd.to_datetime(data['date']) + pd.Timedelta(days=25)).astype(str)
    data['year_id'] = data['date'].str.split('-').str[0].astype(int)
    data = data.loc[data['year_id'] < 2024]
    data = data.groupby(['location_id', 'year_id'])['mean'].sum()

    population = load_covid_team_population()
    data = (data / population).rename(f'inf_{variant}')

    data = copy_2023_to_2024(data)
    data = data.loc[:, model_years]

    return data


def load_idr(
    location_metadata: pd.DataFrame,
    covid_seir_fit_root: Path = COVID_SEIR_FIT_ROOT, covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT, model_years: List[int] = MODEL_YEARS
) -> pd.Series:
    infections = pd.read_csv(covid_seir_outputs_root / 'output_summaries' / 'daily_infections.csv')
    infections['date'] = pd.to_datetime(infections['date'])

    idr = pd.read_parquet(covid_seir_fit_root / 'summary' / 'prior_idr.parquet')
    data = pd.concat(
        [
            infections.set_index(['location_id', 'date']).loc[:, 'mean'].rename('infections'),
            idr.loc[:, 'mean'].rename('idr'),  # .set_index(['location_id', 'date'])
        ],
        axis=1
    )
    data = data.loc[data['idr'] != np.inf].dropna().reset_index()
    data['date'] = (pd.to_datetime(data['date']) + pd.Timedelta(days=25)).astype(str)
    data['year_id'] = data['date'].str.split('-').str[0].astype(int)
    data = data.loc[data['year_id'] < 2024]
    data['idr'] = data['idr'].clip(0, 1) * data['infections']
    data = data.groupby(['location_id', 'year_id'])[['idr', 'infections']].sum()

    region_data = data.join(location_metadata.set_index(['level', 'location_id']).loc[3].loc[:, 'region_id'], how='inner')
    region_data['region_id'] = region_data['region_id'].astype(int)
    region_data = region_data.groupby(['region_id', 'year_id'])[['idr', 'infections']].sum()
    region_data.index.names = ['location_id', 'year_id']
    data = pd.concat(
        [
            region_data.drop(data.index.get_level_values('location_id').unique().tolist(), errors='ignore'),
            data
        ]
    )

    data = (data['idr'] / data['infections']).rename('idr')

    data = copy_2023_to_2024(data)
    data = data.loc[:, model_years]

    return data


def load_total_covid_asdr(path: Path = TOTAL_COVID_ASDR_PATH, model_years: List[int] = MODEL_YEARS):
    data = pd.read_csv(path)
    data = (
        data
        .set_index(['location_id', 'year_id', 'sex_id'])
        .loc[:, 'covid_asdr']
        .rename('total_covid_asdr')
        .sort_index()
        .loc[:, model_years, :]
    )

    return data
