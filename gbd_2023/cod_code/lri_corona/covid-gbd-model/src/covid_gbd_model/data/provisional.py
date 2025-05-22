from pathlib import Path
from loguru import logger
from typing import List

import numpy as np
import pandas as pd

from covid_gbd_model.paths import DEMOG_AGE_METADATA_PATH


def expand_age_metadata(
    md_age_metadata: pd.DataFrame, 
    demog_age_metadata_path: Path = DEMOG_AGE_METADATA_PATH, 
    verbose: bool = False
):
    age_metadata = pd.read_csv(demog_age_metadata_path)
    age_metadata = age_metadata.loc[
        ~age_metadata['age_group_id'].isin([
            161,  # "0" (under-1 duplicate)
            49,   # "1" (1-2 duplicate)
            36,   # 2-19 age-standardized
            38,   # 20+ age-standardized
            308,  # "70 plus" (70+ duplicate)
            283,  # 999-999 (dummy age group)

            ## DROPPED BECAUSE OF SETTING CEILING OF 125
            27,   # age-standardized
            294,  # 90-inf (90-125 duplicate -- inf/125)
            371,  # 124-inf (124-125 duplicate -- inf/125)
        ])
    ]
    age_metadata = age_metadata.rename(columns={'age_start': 'age_lower', 'age_end': 'age_upper'})

    if verbose:
        logger.warning('Setting upper bound of age to be 125 (GBD standard...?)')
    age_metadata.loc[age_metadata['age_upper'] > 125, 'age_upper'] = 125
    age_metadata['age_upper'] = age_metadata.loc[:, ['age_lower', 'age_upper']].max(axis=1)

    if not age_metadata.drop(age_metadata[['age_lower', 'age_upper']].drop_duplicates().index).empty:
        raise ValueError('Duplicate age start/end pair(s) in age_metadata.')

    ## AGE GROUP FIXES ##
    age_metadata.loc[age_metadata['age_group_id'] == 388, 'age_upper'] = 0.5
    age_metadata.loc[age_metadata['age_group_id'] == 389, 'age_lower'] = 0.5

    md_age_metadata['most_detailed'] = 1
    age_metadata = age_metadata.merge(md_age_metadata.loc[:, ['age_group_id', 'most_detailed']], how='left')
    age_metadata['most_detailed'] = age_metadata['most_detailed'].fillna(0).astype(int)

    return age_metadata


def map_most_detailed_age_groups(data: pd.Series, md_age_metadata: pd.DataFrame):
    agg_age_metadata = expand_age_metadata(md_age_metadata).set_index('age_group_id').drop('most_detailed', axis=1)

    age_map = {}
    for age_group_id in data.index.get_level_values('age_group_id').unique():
        age_lower, age_upper = agg_age_metadata.loc[age_group_id]
        age_group_ids = (
            md_age_metadata
            .loc[
                (md_age_metadata['age_group_years_start'] >= age_lower)
                & (md_age_metadata['age_group_years_end'] <= age_upper)
            ]
        )
        if age_group_ids['age_group_years_start'].min() != age_lower:
            raise ValueError(f'Unmatched upper age bound - {age_lower}')
        if age_group_ids['age_group_years_end'].max() != age_upper:
            raise ValueError(f'Unmatched upper age bound - {age_upper}')
        age_group_ids = age_group_ids['age_group_id'].to_list()
        age_map[age_group_id] = {
            'age_group_years_start': age_lower,
            'age_group_years_end': age_upper,
            'child_age_group_ids': age_group_ids,
        }

    return age_map


def age_sex_formatting(data: pd.Series, age_metadata: pd.DataFrame):
    data_name = data.name
    data = data.to_frame()

    age_metadata = expand_age_metadata(age_metadata).drop('most_detailed', axis=1)

    age_groups = data.index.get_level_values('age_label').unique().tolist()
    terminal_suffixes = ['+', 'years and over', 'and older']
    for terminal_suffix in terminal_suffixes:
        age_groups = [str(age_group).replace(terminal_suffix, '-124') for age_group in age_groups]
    age_groups = [str(age_group).replace('years', '') for age_group in age_groups]
    age_groups = [age_group if '-' in age_group else f'{age_group}-{age_group}' for age_group in age_groups]

    age_groups = pd.DataFrame(
        [[int(age) for age in age_group.split('-')] for age_group in age_groups],
        columns=['age_lower', 'age_upper'],
        index=data.index.get_level_values('age_label').unique()
    )
    age_groups['age_upper'] += 1
    age_groups = age_groups.reset_index().merge(age_metadata, how='left').set_index('age_label')

    sexes = pd.Series(
        [1, 2, 1, 2, 1, 2, 1, 2],
        name='sex_id',
        index=pd.Index(['Males', 'Females', 'Male', 'Female', 'm', 'ž', 'Men', 'Women'], name='sex_label')
    )

    data = data.join(age_groups, how='left').join(sexes, how='left')
    if data.loc[:, ['age_group_id', 'sex_id']].isnull().any().any():
        raise ValueError('Unmatched age and/or sex metadata.')
    data = data.reset_index().set_index(['location_id', 'year_id', 'age_group_id', 'sex_id']).loc[:, data_name]

    return data


def australia(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2022] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_csv(path, header=2)
    data = data.set_index('Age (years)').loc[:, ['Males', 'Females']].dropna().stack().rename('deaths')
    data = data.str.replace(',', '').astype(int)

    data = pd.concat([data], keys=[2022], names=['year_id'])
    data = pd.concat([data], keys=[71], names=['location_id'])
    data.index.names = ['location_id', 'year_id', 'age_label', 'sex_label']

    data = age_sex_formatting(data, age_metadata)

    return data


def bulgaria(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2022] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')

    data = pd.read_excel(Path(path) / 'Zdr_6.1.1_Umr_EN2.xls', header=[2, 3], sheet_name='2022')
    data = data.set_index(data.columns[0])
    data = data.loc['     of which COVID-19 (U07.1-U07.2)']

    data = data.replace('-', '0').astype(int)

    data = pd.concat([data], keys=[2022], names=['year_id'])
    data = pd.concat([data], keys=[45], names=['location_id'])
    data.index.names = ['location_id', 'year_id', 'age_label', 'sex_label']
    data = data.rename('deaths').drop('Total', level=2)

    data = age_sex_formatting(data, age_metadata)

    return data


def czechia(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2022] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_excel(path, header=[2], index_col=[0, 1, 2])
    data = data.loc['U07','Covid-19'].unstack().drop('Celkem')
    data = data.replace('-', '0').astype(int)

    data = pd.concat([data], keys=[2022], names=['year_id'])
    data = pd.concat([data], keys=[47], names=['location_id'])
    data.index.names = ['location_id', 'year_id', 'age_label', 'sex_label']
    data = data.rename('deaths')

    data = age_sex_formatting(data, age_metadata)

    return data


def denmark(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2021, 2022] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_excel(Path(path) / 'DNK_STATBANK_DEATHS_BY_CAUSE_OF_DEATH_AGE_SEX_2022_Y2024M04D22.XLSX', header=2, index_col=[0, 1, 2])
    data = data.loc[:, :, 'A-23x Covid-19 - Corona'].drop('Age, total', level=1).drop('Total', axis=1).stack()
    data = pd.concat([data.reset_index(level=0, drop=True)], keys=[2022], names=['year_id'])
    data = pd.concat([data], keys=[78], names=['location_id'])
    data.index.names = ['location_id', 'year_id', 'age_label', 'sex_label']
    data.index = data.index.set_levels(data.index.levels[1].astype(int), level=1)
    data = data.rename('deaths').astype(int)

    data = age_sex_formatting(data, age_metadata)

    return data


def estonia(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2022] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_excel(Path(path) / 'RV56_20240425-012531.xlsx', header=2, index_col=[0, 1, 2])
    data = data.loc[['2022'], :, ['Males', 'Females']].loc[:, '..COVID-19 (U07)', :]
    data = data.loc[data['Age groups total'].notnull()]
    data = data.loc[:, ['0', '1-4'] + [f'{i}-{i+4}' for i in range(5, 100, 5)] + ['100 and older']]
    data = data.stack()
    data = pd.concat([data], keys=[58], names=['location_id'])
    data.index.names = ['location_id', 'year_id', 'sex_label', 'age_label']
    data = data.rename('deaths')
    data = data.reorder_levels(['location_id', 'year_id', 'age_label', 'sex_label'])

    data = age_sex_formatting(data, age_metadata)

    return data


def philippines(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2022, 2023] or age_sex != 'Covid deaths - all age/sex':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_excel(path, header=[3, 4], index_col=[0]).loc[['COVID-19 Virus identified U07.1', 'COVID-19 Virus not identified U07.2']]
    data = pd.Series(
        pd.concat([
            data.loc[:, ('Jan-Dec 2022', 'Number')],
            data.loc[:, ('Jan - Dec 2023(p)', 'Number')],
        ]).values,
        index=pd.MultiIndex.from_tuples(
            [[16, 2022, 22, 3, 'U07.1'], [16, 2022, 22, 3, 'U07.2'], [16, 2023, 22, 3, 'U07.1'], [16, 2023, 22, 3, 'U07.2']],
            names=['location_id', 'year_id', 'age_group_id', 'sex_id', 'icd10'],
        )
    )
    data = data.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id']).sum().rename('deaths')

    # UPDATE - CoD team added 2022 VR, just keep 2023
    data = data.loc[:, [2023], :, :]

    return data

def united_states(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2023] or age_sex != 'Covid deaths - all age/sex':
        raise ValueError(f'Unexpected years and age/sex in {country} data')
    data = pd.read_excel(path, sheet_name='table_02')
    data = data.loc[data['Gender'].notnull()]

    location_map = (
        location_metadata
        .loc[location_metadata['parent_id'] == 102, ['location_name', 'location_id']]
        .set_index('location_name')
        .to_dict()['location_id']
    )
    data['location_id'] = data['Residence State'].map(location_map)
    data['year_id'] = 2023
    data['age_group_id'] = 22
    data['sex_id'] = data['Gender'].map({'Male': 1, 'Female': 2})
    data = data.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id']).loc[:, 'Deaths'].rename('deaths')

    return data


def turkiye(
    country: str,
    location_metadata: pd.DataFrame, age_metadata: pd.DataFrame,
    years: List, age_sex: str, path: Path
):
    if years != [2020, 2021, 2022, 2023] or age_sex != 'Covid deaths - age/sex specific':
        raise ValueError(f'Unexpected years and age/sex in {country} data')

    data = pd.read_excel(path, header=[4], index_col=[0, 1])
    year_idx = data.index.get_level_values('Year').unique()
    year_idx = [i for i in year_idx if i in years or any([str(i).startswith(str(year)) for year in years])]
    data = data.loc[year_idx]
    data['sex_label'] = data['Sex'].str.split('-').str[1].str.strip()
    data = data.set_index('sex_label', append=True).drop('Sex', axis=1)
    data = (
        data
        .loc[:, 'COVID-19', :]
        .loc[:, ['Male', 'Female'], :]
        .drop('Total', axis=1)
        .rename_axis('age_label', axis=1)
        .stack()
        .rename('deaths')
        .reset_index()
    )
    data['year_id'] = data['Year'].astype(str).str.replace('\(r\)', '').astype(int)
    data['location_id'] = 155
    data = data.set_index(['location_id', 'year_id', 'age_label', 'sex_label']).loc[:, 'deaths']

    data = age_sex_formatting(data, age_metadata)

    return data


def load_extraction_metadata():
    extraction_metadata = pd.read_excel(
        Path(__file__).parents[0] / 'covid_deaths_prelim_vr_data_seeking_2024_05_31.xlsx',
        sheet_name='most-recent-cod-vr-data-loc-yr'
    )
    extraction_metadata = (
        extraction_metadata
        .loc[extraction_metadata['J:\Incoming filepath'].notnull(), ['Country', 'New years identified', 'Data available', 'J:\Incoming filepath']]
        .set_index('Country')
    )

    extraction_metadata['J:\Incoming filepath'] = extraction_metadata['J:\Incoming filepath'].str.replace('\\', '/').str.replace('J:', '/home/j')
    if (np.isnan(extraction_metadata.loc['Cyprus', 'New years identified'])
        and extraction_metadata.loc['Cyprus', 'J:\Incoming filepath'].split('/')[-1] == '2170010E_20240504-011742.csv'):
        extraction_metadata.loc['Cyprus', ['New years identified']] = '2020, 2021'
    extraction_metadata['New years identified'] = (
        extraction_metadata['New years identified']
        .astype(str)
        .apply(lambda x: [int(y.strip()) for y in x.split(',')])
    )

    if (extraction_metadata.loc['Czech Republic'] == extraction_metadata.loc['Czechoslovakia, Czech Republic']).all():
        # duplicate czech    
        extraction_metadata = extraction_metadata.drop('Czechoslovakia, Czech Republic')
    else:
        raise ValueError('Multiple Czech entries and data are not the same.')
    if extraction_metadata.loc['Cyprus', 'New years identified'] == [2020, 2021]:
        # already have cyprus 2020 and 2021 in CoD db
        extraction_metadata = extraction_metadata.drop('Cyprus')

    return extraction_metadata


EXTRACTION_FUNCTION_MAP = {
    'Bulgaria': bulgaria,
    'Czech Republic': czechia,
    'Denmark': denmark,
    'Philippines': philippines,
    'United States': united_states,
    'Turkiye': turkiye,
}
