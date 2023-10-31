from pathlib import Path
from loguru import logger

import pandas as pd
import numpy as np

from covid_historical_model.etl import helpers

# EM_PATH = ('/mnt/team/demographics/pub/covid_em_estimate/s3-2022-02-17-22-05'
#            '/outputs/covid_em_scalars-draw-s3-2022-02-17-22-05.csv')
EM_PATH = '/ihme/covid-19/mortality-scalars/2022_03_16.01/total_covid_draw.csv'


def testing(testing_root: Path) -> pd.DataFrame:
    data_path = testing_root / 'forecast_raked_test_pc_simple.csv'
    data = pd.read_csv(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.sort_values(['location_id', 'date']).reset_index(drop=True)
    data['population'] = data['population'].fillna(data['pop'])
    data['daily_tests'] = data['test_pc'] * data['population']
    data['cumulative_tests'] = data.groupby('location_id')['daily_tests'].cumsum()
    data = (data
            .loc[:, ['location_id', 'date', 'cumulative_tests']]
            .sort_values(['location_id', 'date'])
            .reset_index(drop=True))
    data = (data.groupby('location_id', as_index=False)
            .apply(lambda x: helpers.fill_dates(x, ['cumulative_tests']))
            .reset_index(drop=True))
    data = data.sort_values(['location_id', 'date']).reset_index(drop=True)
    data['daily_tests'] = (data
                           .groupby('location_id')['cumulative_tests']
                           .apply(lambda x: x.diff()))
    data = data.dropna()
    data = data.sort_values(['location_id', 'date']).reset_index(drop=True)
    data['testing_capacity'] = data.groupby('location_id')['daily_tests'].cummax()

    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, ['daily_tests', 'testing_capacity', 'cumulative_tests']])
    
    return data


def ihr_age_pattern(age_rates_root: Path, hierarchy: pd.DataFrame,) -> pd.Series:
    # can lose hierarchy if IHR becomes location-specific
    data_path = age_rates_root / 'hir_preds_5yr.csv'
    data = pd.read_csv(data_path)
    
    data = data.rename(columns={'age_group_start': 'age_group_years_start',
                                'age_group_end': 'age_group_years_end',
                                'hir': 'ihr',})
    data['age_group_years_end'].iloc[-1] = 125

    data = data.loc[:, ['age_group_years_start', 'age_group_years_end', 'ihr']]
    data['key'] = 1
    
    hierarchy = hierarchy.copy()
    hierarchy['key'] = 1
    
    data = hierarchy.loc[:, ['location_id', 'key']].merge(data)
        
    data = (data
            .set_index(['location_id', 'age_group_years_start', 'age_group_years_end'])
            .sort_index()
            .loc[:, 'ihr'])

    return data


def ifr_age_pattern(age_rates_root: Path, hierarchy: pd.DataFrame,) -> pd.Series:
    # can lose hierarchy if IFR becomes location-specific
    data_path = age_rates_root / 'ifr_preds_5yr_global.csv'
    data = pd.read_csv(data_path)
    
    data = data.rename(columns={'age_group_start': 'age_group_years_start',
                                'age_group_end': 'age_group_years_end',})
    data['age_group_years_end'].iloc[-1] = 125

    data = data.loc[:, ['age_group_years_start', 'age_group_years_end', 'ifr']]
    data['key'] = 1
    
    hierarchy = hierarchy.copy()
    hierarchy['key'] = 1
    
    data = hierarchy.loc[:, ['location_id', 'key']].merge(data)
        
    data = (data
            .set_index(['location_id', 'age_group_years_start', 'age_group_years_end'])
            .sort_index()
            .loc[:, 'ifr'])
    
    return data


def seroprevalence_age_pattern(age_rates_root: Path, hierarchy: pd.DataFrame,) -> pd.Series:
    # can lose hierarchy if sero becomes location-specific
    data_path = age_rates_root / 'seroprev_preds_5yr.csv'
    data = pd.read_csv(data_path)
    
    data = data.rename(columns={'age_group_start': 'age_group_years_start',
                                'age_group_end': 'age_group_years_end',
                                'seroprev': 'seroprevalence',})
    data['age_group_years_end'].iloc[-1] = 125

    data = (data.loc[:, ['age_group_years_start', 'age_group_years_end', 'seroprevalence']])
    data['key'] = 1
    
    hierarchy = hierarchy.copy()
    hierarchy['key'] = 1
    
    data = hierarchy.loc[:, ['location_id', 'key']].merge(data)
        
    data = (data
            .set_index(['location_id', 'age_group_years_start', 'age_group_years_end'])
            .sort_index()
            .loc[:, 'seroprevalence'])
    
    return data


def vaccine_coverage(vaccine_coverage_root: Path, pred_end_date: pd.Timestamp,) -> pd.DataFrame:
    data_path = vaccine_coverage_root / 'slow_scenario_vaccine_coverage.csv'
    data = pd.read_csv(data_path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.loc[data['date'] <= pred_end_date]
    
    keep_columns = [
        # total vaccinated (all and by three groups)
        'cumulative_all_vaccinated',
        'cumulative_essential_vaccinated',
        'cumulative_adults_vaccinated',
        'cumulative_elderly_vaccinated',
        
        # total seroconverted (all and by three groups)
        'cumulative_all_effective',
        'cumulative_essential_effective',
        'cumulative_adults_effective',
        'cumulative_elderly_effective',
        
        # elderly (mutually exclusive)
        'cumulative_hr_effective_wildtype',
        'cumulative_hr_effective_protected_wildtype',
        'cumulative_hr_effective_variant',
        'cumulative_hr_effective_protected_variant',
    
        # other adults (mutually exclusive)
        'cumulative_lr_effective_wildtype',
        'cumulative_lr_effective_protected_wildtype',
        'cumulative_lr_effective_variant',
        'cumulative_lr_effective_protected_variant',
    ]
    
    data = (data
            .set_index(['location_id', 'date'])
            .sort_index()
            .loc[:, keep_columns])
    
    return data


def variant_scaleup(variant_scaleup_root: Path, variant_type: str, verbose: bool = True) -> pd.Series:
    data_path = variant_scaleup_root / 'variant_reference.csv'
    data = pd.read_csv(data_path)
    data['date'] = pd.to_datetime(data['date'])
    
    # use Delhi for Other Union Territories
    delhi_data = data.loc[data['location_id'] == 4849]
    for location_id in [4840, 4845, 4858, 4866, 60896, 44539, 44540]:
        if location_id not in data['location_id'].unique():
            out_data = delhi_data.copy()
            out_data['location_id'] = location_id
            data = pd.concat([data, out_data]).reset_index(drop=True)
    
    variants_in_data = data['variant'].unique().tolist()
    status_path = variant_scaleup_root / 'outputs' / 'variant_by_escape_status.csv'
    status = pd.read_csv(status_path)
    severity = status.loc[status['escape'] == 0, 'variant'].unique().tolist()
    severity = [v for v in severity if v != 'wild_type']
    escape = status.loc[status['escape'] == 1, 'variant'].unique().tolist()
    variants_in_model = ['wild_type'] + severity + escape
    
    if any([v not in variants_in_data for v in variants_in_model]):
        missing_in_data = ', '.join([v for v in variants_in_model if v not in variants_in_data])
        raise ValueError(f'The following variants are expected in the data but not present: {missing_in_data}')
    if any([v not in variants_in_model for v in variants_in_data]):
        missing_in_model = ', '.join([v for v in variants_in_data if v not in variants_in_model])
        raise ValueError(f'The following variants are in the data but not expected: {missing_in_model}')
    
    if variant_type == 'escape':
        is_escape_variant = data['variant'].isin(escape)
        data = data.loc[is_escape_variant]
        if verbose:
            logger.info(f"Escape variants: {', '.join(data['variant'].unique())}")
        data = data.rename(columns={'prevalence': 'escape_variant_prevalence'})
        data = data.groupby(['location_id', 'date'])['escape_variant_prevalence'].sum()
    elif variant_type == 'severity':
        is_variant = data['variant'].isin(severity)
        data = data.loc[is_variant]
        if verbose:
            logger.info(f"Variants: {', '.join(data['variant'].unique())}")
        data = data.rename(columns={'prevalence': 'severity_variant_prevalence'})
        data = data.groupby(['location_id', 'date'])['severity_variant_prevalence'].sum()
    else:
        raise ValueError(f'Invalid variant type specified: {variant_type}')

    
    return data


def excess_mortailty_scalars(excess_mortality: bool,) -> pd.DataFrame:
    data = pd.read_csv(EM_PATH)
    if 'date' in data.columns:
        raise ValueError('Not using date.')
    data = data.rename(columns={'scalar': 'em_scalar'})
    data['draw'] -= 1
    data = data.sort_values(['draw', 'location_id'])
    data = (data
            .set_index('draw')
            .loc[:, ['location_id', 'em_scalar',]])
    
    if not excess_mortality:
        data['em_scalar'] = 1
    
    return data
