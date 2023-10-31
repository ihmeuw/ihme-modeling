from pathlib import Path
from typing import Dict, List
import itertools

import pandas as pd
import numpy as np

from covid_historical_model.etl import model_inputs, estimates


def load_input_data(out_dir: Path,
                    excess_mortality: bool,
                    excess_mortality_draw: int,
                    age_rates_root: Path,
                    shared: Dict, seroprevalence: pd.DataFrame, vaccine_coverage: pd.DataFrame,
                    escape_variant_prevalence: pd.Series, severity_variant_prevalence: pd.Series,
                    covariates: List[pd.Series],
                    cross_variant_immunity: float,
                    variant_risk_ratio: float,
                    verbose: bool = True) -> Dict:
    # load data
    cumulative_hospitalizations, daily_hospitalizations = model_inputs.reported_epi(
        out_dir, 'hospitalizations', True,
        shared['hierarchy'], shared['gbd_hierarchy'],
    )
    _, daily_deaths = model_inputs.reported_epi(
        out_dir, 'deaths', True,
        shared['hierarchy'], shared['gbd_hierarchy'],
        excess_mortality, excess_mortality_draw,
    )
    adj_gbd_hierarchy = model_inputs.validate_hierarchies(shared['hierarchy'].copy(),
                                                          shared['gbd_hierarchy'].copy())
    sero_age_pattern = estimates.seroprevalence_age_pattern(age_rates_root, adj_gbd_hierarchy.copy())
    ihr_age_pattern = estimates.ihr_age_pattern(age_rates_root, adj_gbd_hierarchy.copy())
    
    input_data = {
        'cumulative_hospitalizations': cumulative_hospitalizations,
        'daily_hospitalizations': daily_hospitalizations,
        'daily_deaths': daily_deaths,
        'seroprevalence': seroprevalence,
        'vaccine_coverage': vaccine_coverage,
        'covariates': covariates,
        'sero_age_pattern': sero_age_pattern,
        'ihr_age_pattern': ihr_age_pattern,
        'escape_variant_prevalence': escape_variant_prevalence,
        'severity_variant_prevalence': severity_variant_prevalence,
        'cross_variant_immunity': cross_variant_immunity,
        'variant_risk_ratio': variant_risk_ratio,
    }
    input_data.update(shared)
    
    return input_data


def create_model_data(cumulative_hospitalizations: pd.Series,
                      daily_hospitalizations: pd.Series,
                      seroprevalence: pd.DataFrame,
                      covariates: List[pd.Series],
                      ihr_data_scalar: pd.Series,
                      hierarchy: pd.DataFrame, population: pd.Series,
                      day_0: pd.Timestamp,
                      durations: Dict,
                      **kwargs) -> pd.DataFrame:
    ihr_data = seroprevalence.loc[seroprevalence['is_outlier'] == 0].copy()
    ihr_data['date'] -= pd.Timedelta(days=durations['admission_to_sero'])
    ihr_data = (ihr_data
                .set_index(['data_id', 'location_id', 'date'])
                .loc[:, 'seroprevalence'])
    ihr_data = ((cumulative_hospitalizations / (ihr_data * population))
                .dropna()
                .rename('ihr'))

    # get mean day of admission int
    loc_dates = (ihr_data
                 .reset_index()
                 .loc[:, ['location_id', 'date']]
                 .drop_duplicates()
                 .values
                 .tolist())
    time = []
    for location_id, survey_end_date in loc_dates:
        lochosps = daily_hospitalizations.loc[location_id]
        lochosps = lochosps.clip(0, np.inf)
        lochosps = lochosps.reset_index()
        lochosps = lochosps.loc[lochosps['date'] <= survey_end_date]
        lochosps['t'] = (lochosps['date'] - day_0).dt.days
        t = np.average(lochosps['t'], weights=lochosps['daily_hospitalizations'] + 1e-6)
        t = int(np.round(t))
        mean_hospitalization_date = lochosps.loc[lochosps['t'] == t, 'date'].item()
        time.append(
            pd.DataFrame(
                {'t':t, 'mean_hospitalization_date':mean_hospitalization_date},
                index=pd.MultiIndex.from_arrays([[location_id], [survey_end_date]],
                                                names=('location_id', 'date')),)
        )
    time = pd.concat(time)

    # add time
    model_data = time.join(ihr_data, how='outer')
    
    # add covariates
    for covariate in covariates:
        model_data = model_data.join(covariate, how='outer')
        
    model_data = model_data.join(ihr_data_scalar, how='left')
    if model_data['ratio_data_scalar'].isnull().any():
        raise ValueError('Missing data scalar.')
            
    return model_data.reset_index()


def create_pred_data(hierarchy: pd.DataFrame, adj_gbd_hierarchy: pd.DataFrame, population: pd.Series,
                     covariates: List[pd.Series],
                     pred_start_date: pd.Timestamp, pred_end_date: pd.Timestamp,
                     day_0: pd.Timestamp,
                     **kwargs):
    pred_data = pd.DataFrame(list(itertools.product(adj_gbd_hierarchy['location_id'].to_list(),
                                                    list(pd.date_range(pred_start_date, pred_end_date)))),
                         columns=['location_id', 'date'])
    pred_data['intercept'] = 1
    pred_data['t'] = (pred_data['date'] - day_0).dt.days
    pred_data = pred_data.set_index(['location_id', 'date'])
    
    for covariate in covariates:
        pred_data = pred_data.join(covariate, how='outer')
    
    return pred_data.reset_index()
