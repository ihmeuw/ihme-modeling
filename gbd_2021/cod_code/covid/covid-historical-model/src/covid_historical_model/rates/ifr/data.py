from pathlib import Path
from typing import Dict, List
import itertools

import pandas as pd
import numpy as np

from covid_historical_model.etl import db, model_inputs, estimates


def load_input_data(out_dir: Path,
                    excess_mortality: bool,
                    excess_mortality_draw: int,
                    age_rates_root: Path,
                    shared: Dict, seroprevalence: pd.DataFrame, sensitivity_data: pd.DataFrame,
                    vaccine_coverage: pd.DataFrame,
                    escape_variant_prevalence: pd.Series, severity_variant_prevalence: pd.Series,
                    covariates: List[pd.Series],
                    cross_variant_immunity: float,
                    variant_risk_ratio: float,
                    verbose: bool = True) -> Dict:
    # load data
    cumulative_deaths, daily_deaths = model_inputs.reported_epi(
        out_dir, 'deaths', True,
        shared['hierarchy'], shared['gbd_hierarchy'],
        excess_mortality, excess_mortality_draw,
    )
    assay_map = model_inputs.assay_map(out_dir)
    adj_gbd_hierarchy = model_inputs.validate_hierarchies(shared['hierarchy'].copy(),
                                                          shared['gbd_hierarchy'].copy())
    sero_age_pattern = estimates.seroprevalence_age_pattern(age_rates_root, adj_gbd_hierarchy.copy())
    ifr_age_pattern = estimates.ifr_age_pattern(age_rates_root, adj_gbd_hierarchy.copy())
    ihr_age_pattern = estimates.ihr_age_pattern(age_rates_root, adj_gbd_hierarchy.copy())

    input_data = {
        'cumulative_deaths': cumulative_deaths,
        'daily_deaths': daily_deaths,
        'seroprevalence': seroprevalence,
        'sensitivity_data': sensitivity_data,
        'assay_map': assay_map,
        'vaccine_coverage': vaccine_coverage,
        'covariates': covariates,
        'sero_age_pattern': sero_age_pattern,
        'ifr_age_pattern': ifr_age_pattern,
        'ihr_age_pattern': ihr_age_pattern,
        'escape_variant_prevalence': escape_variant_prevalence,
        'severity_variant_prevalence': severity_variant_prevalence,
        'cross_variant_immunity': cross_variant_immunity,
        'variant_risk_ratio': variant_risk_ratio,
    }
    input_data.update(shared)
    
    return input_data


def create_model_data(cumulative_deaths: pd.Series, daily_deaths: pd.Series,
                      seroprevalence: pd.DataFrame,
                      covariates: List[pd.Series],
                      ifr_data_scalar: pd.Series,
                      hierarchy: pd.DataFrame, population: pd.Series,
                      day_0: pd.Timestamp,
                      durations: Dict,
                      **kwargs) -> pd.DataFrame:
    ifr_data = seroprevalence.loc[seroprevalence['is_outlier'] == 0].copy()
    ifr_data['date'] += pd.Timedelta(days=durations['sero_to_death'])
    ifr_data = (ifr_data
                .set_index(['data_id', 'location_id', 'date'])
                .loc[:, 'seroprevalence'])
    ifr_data = ((cumulative_deaths / (ifr_data * population))
                .dropna()
                .rename('ifr'))

    # get mean day of death int
    loc_dates = (ifr_data
                 .reset_index()
                 .loc[:, ['location_id', 'date']]
                 .drop_duplicates()
                 .values
                 .tolist())
    time = []
    for location_id, survey_end_date in loc_dates:
        locdeaths = daily_deaths.loc[location_id]
        locdeaths = locdeaths.clip(0, np.inf)
        locdeaths = locdeaths.reset_index()
        locdeaths = locdeaths.loc[locdeaths['date'] <= survey_end_date]
        locdeaths['t'] = (locdeaths['date'] - day_0).dt.days
        t = np.average(locdeaths['t'], weights=locdeaths['daily_deaths'] + 1e-6)
        t = int(np.round(t))
        mean_death_date = locdeaths.loc[locdeaths['t'] == t, 'date'].item()
        time.append(
            pd.DataFrame(
                {'t':t, 'mean_death_date':mean_death_date},
                index=pd.MultiIndex.from_arrays([[location_id], [survey_end_date]],
                                                names=('location_id', 'date')),)
        )
    time = pd.concat(time)

    # add time
    model_data = time.join(ifr_data, how='outer')
    
    # add covariates
    for covariate in covariates:
        model_data = model_data.join(covariate, how='outer')
        
    if ifr_data_scalar is None:
        model_data['ratio_data_scalar'] = 1
    else:
        model_data = model_data.join(ifr_data_scalar, how='left')
        if model_data['ratio_data_scalar'].isnull().any():
            raise ValueError('Missing data scalar.')
    
    return model_data.reset_index()


def create_pred_data(hierarchy: pd.DataFrame, gbd_hierarchy: pd.DataFrame, population: pd.Series,
                     covariates: List[pd.Series],
                     pred_start_date: pd.Timestamp, pred_end_date: pd.Timestamp,
                     day_0: pd.Timestamp,
                     **kwargs):
    adj_gbd_hierarchy = model_inputs.validate_hierarchies(hierarchy.copy(), gbd_hierarchy.copy())
    pred_data = pd.DataFrame(list(itertools.product(adj_gbd_hierarchy['location_id'].to_list(),
                                                    list(pd.date_range(pred_start_date, pred_end_date)))),
                         columns=['location_id', 'date'])
    pred_data['intercept'] = 1
    pred_data['t'] = (pred_data['date'] - day_0).dt.days
    pred_data = pred_data.set_index(['location_id', 'date'])
    
    for covariate in covariates:
        pred_data = pred_data.join(covariate, how='outer')
    
    return pred_data.reset_index()
