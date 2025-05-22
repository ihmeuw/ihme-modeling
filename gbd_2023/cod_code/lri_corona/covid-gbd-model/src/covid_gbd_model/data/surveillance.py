from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from covid_gbd_model.paths import (
    WHO_SUPPLEMENT_PATH,
    COVID_SEIR_OUTPUTS_ROOT,
    COVID_MODEL_INPUTS_ROOT,
)

EM_SCALAR_CUTOFF = np.inf


def fill_dates(data: pd.DataFrame, interp_vars: List[str]) -> pd.DataFrame:
    data = data.set_index('date').sort_index()
    data = data.asfreq('D').reset_index()
    data[interp_vars] = data[interp_vars].interpolate(axis=0)
    data['location_id'] = (data['location_id']
                           .fillna(method='pad')
                           .astype(int))

    return data[['location_id', 'date'] + interp_vars]


def get_zaf_subnat_data(covid_model_inputs_root: Path, location_metadata: pd.DataFrame):
    zaf_location_ids = location_metadata.loc[location_metadata['parent_id'] == 196, 'location_id'].to_list()

    data = pd.read_csv(covid_model_inputs_root / 'full_data_unscaled.csv')
    data['date'] = pd.to_datetime(data['Date'])
    data = (data
            .groupby('location_id', as_index=False)
            .apply(lambda x: fill_dates(x, ['Deaths']))
            .reset_index(drop=True))
    data['year_id'] = data['date'].dt.year
    data = (
        data
        .loc[data['date'].astype(str).str.endswith('12-31')]
        .set_index(['location_id', 'year_id'])
        .loc[:, 'Deaths']
        .loc[zaf_location_ids]
        .rename('mean')
    )
    return data


def load_covid_who_supplement(location_metadata: pd.DataFrame, who_supplement_path: Path = WHO_SUPPLEMENT_PATH):
    data = pd.read_csv(who_supplement_path)
    country_rename_dict = {
        'T�rkiye': 'Türkiye',
        'Netherlands (Kingdom of the)': 'Netherlands',
        "C�te d'Ivoire": "Côte d'Ivoire",
        'Kosovo (in accordance with UN Security Council resolution 1244 (1999))': 'Serbia',
        'occupied Palestinian territory, including east Jerusalem': 'Palestine',
        'United Kingdom of Great Britain and Northern Ireland': 'United Kingdom',
    }
    data['Country'] = data['Country'].map(country_rename_dict).fillna(data['Country'])
    data = (
        data
        .merge(
            location_metadata.loc[:, ['location_name', 'location_id']].rename(columns={'location_name': 'Country'}),
            how='left'
        )
    )

    data = data.loc[data['location_id'].notnull()]
    data['location_id'] = data['location_id'].astype(int)
    data['year_id'] = data['Date_reported'].apply(lambda x: x.split('-')[0]).astype(int)

    coverage = data.groupby(['location_id', 'Country', 'year_id'])['New_deaths'].apply(lambda x: x.notnull().mean())
    is_covered = (coverage[:, :, 2023] >= coverage[:, :, 2022] * 0.8)
    is_covered.loc[is_covered]

    data = data.groupby(['location_id', 'year_id'])['New_deaths'].sum().rename('deaths')
    data = pd.concat([data], keys=[22], names=['age_group_id'])
    data = pd.concat([data], keys=[3], names=['sex_id'])
    data = data.reorder_levels(['location_id', 'year_id', 'age_group_id', 'sex_id'])

    return data


def load_surveillance_data(
    location_metadata: pd.DataFrame,
    em_scalar_cutoff: int = EM_SCALAR_CUTOFF,
    covid_model_inputs_root: Path = COVID_MODEL_INPUTS_ROOT, covid_seir_outputs_root: Path = COVID_SEIR_OUTPUTS_ROOT,
):
    data_date = pd.read_csv(covid_seir_outputs_root / 'output_miscellaneous' / 'unscaled_full_data.csv')
    data_date['date'] = pd.to_datetime(data_date['date'])
    data_date = data_date.loc[data_date['cumulative_deaths'].notnull()]
    data_date = data_date.groupby('location_id')['date'].max()
    reporting_year = (
        pd.concat(
            [
                data_date.loc[data_date.dt.month >= 9].dt.year,
                data_date.loc[data_date.dt.month < 9].dt.year - 1,
            ]
        )
        .sort_index()
        .rename('reporting_year')
    )

    data = pd.read_csv(covid_seir_outputs_root / 'output_summaries' / 'unscaled_daily_deaths.csv')
    data['year_id'] = data['date'].str.split('-').str[0].astype(int)
    data = data.groupby(['location_id', 'year_id'])['mean'].sum()
    data = data.reset_index('year_id')
    data = data.join(reporting_year)
    data = data.loc[data['year_id'] <= data['reporting_year']]

    excess_mortality_scalars = pd.read_csv(covid_seir_outputs_root / 'output_miscellaneous' / 'excess_mortality_scalars.csv')
    excess_mortality_scalars = (excess_mortality_scalars.groupby('location_id')['mean'].mean())
    valid_location_ids = (
        excess_mortality_scalars
        .loc[excess_mortality_scalars < em_scalar_cutoff]
        .index
    )
    location_metadata = location_metadata.loc[location_metadata['level'] >= 3]
    valid_location_ids = [vl for vl in valid_location_ids if vl in data.index.unique() and vl in location_metadata['location_id'].to_list()]
    if not any([vl == 196 for vl in valid_location_ids]):
        valid_location_ids += [196]
    data = data.set_index('year_id', append=True).loc[:, 'mean']
    data = data.loc[valid_location_ids, 2020:]

    data = pd.concat([
        data,
        get_zaf_subnat_data(covid_model_inputs_root, location_metadata)
    ])
    data = pd.concat([data], keys=[22], names=['age_group_id'])
    data = pd.concat([data], keys=[3], names=['sex_id'])
    data = data.reorder_levels(['location_id', 'year_id', 'age_group_id', 'sex_id'])

    who_data = load_covid_who_supplement(location_metadata)
    data = pd.concat(
        [
            data.drop(6, level=0),
            who_data.loc[6, [2020, 2021, 2022, 2023]],
        ]
    )

    return data
