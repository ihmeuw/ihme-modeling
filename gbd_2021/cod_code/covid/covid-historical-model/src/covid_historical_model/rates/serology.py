import sys
from typing import Dict, Tuple
from pathlib import Path
from loguru import logger
from collections import namedtuple
from datetime import datetime
import functools
import multiprocessing
from tqdm import tqdm

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import seaborn as sns

from covid_historical_model.etl import model_inputs
from covid_historical_model.utils.misc import text_wrap, get_random_state
from covid_historical_model.utils.math import logit, expit, scale_to_bounds
from covid_historical_model.cluster import CONTROLLER_MP_THREADS, OMP_NUM_THREADS
from covid_historical_model.mrbrt import mrbrt

VAX_SERO_PROB = 0.9
SEROREV_LB = 0.

ASSAYS = ['N-Abbott',  # IgG
          'S-Roche', 'N-Roche',  # Ig
          'S-Ortho Ig', 'S-Ortho IgG', # Ig/IgG
          'S-DiaSorin',  # IgG
          'S-EuroImmun',  # IgG
          'S-Oxford',]  # IgG
INCREASING = ['S-Ortho Ig', 'S-Roche']


def bootstrap(sample: pd.DataFrame,):
    n = sample['n'].unique().item()
    random_state = get_random_state(f'bootstrap_{n}')
    rows = random_state.choice(sample.index, size=len(sample), replace=True)
    # rows = np.random.choice(sample.index, size=len(sample), replace=True)
    bootstraped_samples = []
    for row in rows:
        bootstraped_samples.append(sample.loc[[row]])
        
    return pd.concat(bootstraped_samples).reset_index(drop=True).drop('n', axis=1)


def sample_seroprevalence(seroprevalence: pd.DataFrame, n_samples: int,
                          correlate_samples: bool, bootstrap_samples: bool,
                          min_samples: int = 10,
                          floor: float = 1e-5, logit_se_cap: float = 1.,
                          verbose: bool = True):
    logit_se_from_ci = lambda x: (logit(x['seroprevalence_upper']) - logit(x['seroprevalence_lower'])) / 3.92
    logit_se_from_ss = lambda x: np.sqrt((x['seroprevalence'] * (1 - x['seroprevalence'])) / x['sample_size']) / \
                                 (x['seroprevalence'] * (1.0 - x['seroprevalence']))
    
    series_vars = ['location_id', 'is_outlier', 'survey_series', 'date']
    seroprevalence = seroprevalence.sort_values(series_vars).reset_index(drop=True)
    
    if n_samples >= min_samples:
        if verbose:
            logger.info(f'Producing {n_samples} seroprevalence samples.')
        if (seroprevalence['seroprevalence'] < seroprevalence['seroprevalence_lower']).any():
            mean_sub_low = seroprevalence['seroprevalence'] < seroprevalence['seroprevalence_lower']
            raise ValueError(f'Mean seroprevalence below lower:\n{seroprevalence[mean_sub_low]}')
        if (seroprevalence['seroprevalence'] > seroprevalence['seroprevalence_upper']).any():
            high_sub_mean = seroprevalence['seroprevalence'] > seroprevalence['seroprevalence_upper']
            raise ValueError(f'Mean seroprevalence above upper:\n{seroprevalence[high_sub_mean]}')
            
        summary_vars = ['seroprevalence', 'seroprevalence_lower', 'seroprevalence_upper']
        seroprevalence[summary_vars] = seroprevalence[summary_vars].clip(floor, 1 - floor)

        logit_mean = logit(seroprevalence['seroprevalence'].copy())
        logit_se = logit_se_from_ci(seroprevalence.copy())
        logit_se = logit_se.fillna(logit_se_from_ss(seroprevalence.copy()))
        logit_se = logit_se.fillna(logit_se_cap)
        logit_se = logit_se.clip(0, logit_se_cap)
        logit_samples = np.random.normal(loc=logit_mean.to_frame().values,
                                         scale=logit_se.to_frame().values,
                                         size=(len(seroprevalence), n_samples),)
        samples = expit(logit_samples)
        
        ## CANNOT DO THIS, MOVES SOME ABOVE 1
        # # re-center around original mean
        # samples *= seroprevalence[['seroprevalence']].values / samples.mean(axis=1, keepdims=True)
        if correlate_samples:
            logger.info('Correlating seroprevalence samples within location.')
            series_data = (seroprevalence[[sv for sv in series_vars if sv not in ['survey_series', 'date']]]
                           .drop_duplicates()
                           .reset_index(drop=True))
            series_data['series'] = series_data.index
            series_data = seroprevalence.merge(series_data).reset_index(drop=True)
            series_idx_list = [series_data.loc[series_data['series'] == series].index.to_list()
                               for series in range(series_data['series'].max() + 1)]
            sorted_samples = []
            for series_idx in series_idx_list:
                series_samples = samples[series_idx, :].copy()
                series_draw_idx = series_samples[0].argsort().argsort()
                series_samples = np.sort(series_samples, axis=1)[:, series_draw_idx]
                sorted_samples.append(series_samples)
            samples = np.vstack(sorted_samples)
            ## THIS SORTS THE WHOLE SET
            # samples = np.sort(samples, axis=1)

        seroprevalence = seroprevalence.drop(['seroprevalence', 'seroprevalence_lower', 'seroprevalence_upper', 'sample_size'],
                                             axis=1)
        sample_list = []
        for n, sample in enumerate(samples.T):
            _sample = seroprevalence.copy()
            _sample['seroprevalence'] = sample
            _sample['n'] = n
            sample_list.append(_sample.reset_index(drop=True))

    elif n_samples > 1:
        raise ValueError(f'If sampling, need at least {min_samples}.')
    else:
        if verbose:
            logger.info('Just using mean seroprevalence.')
            
        seroprevalence['seroprevalence'] = seroprevalence['seroprevalence'].clip(floor, 1 - floor)
            
        seroprevalence = seroprevalence.drop(['seroprevalence_lower', 'seroprevalence_upper', 'sample_size'],
                                             axis=1)
        
        seroprevalence['n'] = 0
        
        sample_list = [seroprevalence.reset_index(drop=True)]

    if bootstrap_samples:
        if n_samples < min_samples:
            raise ValueError('Not set up to bootstrap means only.')
        with multiprocessing.Pool(CONTROLLER_MP_THREADS) as p:
            bootstrap_list = list(tqdm(p.imap(bootstrap, sample_list), total=n_samples, file=sys.stdout))
    else:
        bootstrap_list = sample_list
    
    return bootstrap_list


def load_seroprevalence_sub_vacccinated(out_dir: Path, hierarchy: pd.DataFrame, vaccinated: pd.Series,
                                        n_samples: int, correlate_samples: bool, bootstrap: bool,
                                        verbose: bool = True,) -> pd.DataFrame:
    seroprevalence = model_inputs.seroprevalence(out_dir, hierarchy, verbose=verbose)
    seroprevalence_samples = sample_seroprevalence(seroprevalence, n_samples, correlate_samples, bootstrap, verbose=verbose)
    
    # ## ## ## ## ## #### ## ## ## ## ## ## ## ## ## ## ##
    # ## tweaks
    # # only take some old age from Danish blood bank data
    # age_spec_population = model_inputs.population(out_dir, by_age=True)
    # pct_65_69 = age_spec_population.loc[78, 65].item() / age_spec_population.loc[78, 65:].sum()
    # danish_sub_70plus = (vaccinated.loc[[78], 'cumulative_adults_vaccinated'] + \
    #     vaccinated.loc[[78], 'cumulative_essential_vaccinated'] + \
    #     (pct_65_69 * vaccinated.loc[[78], 'cumulative_elderly_vaccinated'])).rename('cumulative_all_vaccinated')
    # vaccinated.loc[[78], 'cumulative_all_vaccinated'] = danish_sub_70plus
    
    # # above chunk not sufficient, don't pull vaccinated people out of Danish data
    # vaccinated.loc[[78]] *= 0
    ## ## ## ## ## #### ## ## ## ## ## ## ## ## ## ## ##
    
    # make pop group specific
    age_spec_population = model_inputs.population(out_dir, by_age=True)
    vaccinated = get_pop_vaccinated(age_spec_population, vaccinated)
    
    # use 90% of total vaccinated
    vaccinated['vaccinated'] *= VAX_SERO_PROB
    
    if verbose:
        logger.info('Removing vaccinated from reported seroprevalence.')
    _rv = functools.partial(
        remove_vaccinated,
        vaccinated=vaccinated.copy(),
    )
    seroprevalence = remove_vaccinated(seroprevalence=seroprevalence, vaccinated=vaccinated)
    with multiprocessing.Pool(int(CONTROLLER_MP_THREADS)) as p:
        seroprevalence_samples = list(tqdm(p.imap(_rv, seroprevalence_samples), total=n_samples, file=sys.stdout))
    
    return seroprevalence, seroprevalence_samples


def get_pop_vaccinated(age_spec_population: pd.Series, vaccinated: pd.Series):
    age_spec_population = age_spec_population.reset_index()
    population = []
    for age_start in range(0, 25, 5):
        for age_end in [65, 125]:
            _population = (age_spec_population
                           .loc[(age_spec_population['age_group_years_start'] >= age_start) &
                                (age_spec_population['age_group_years_end'] <= age_end)]
                           .groupby('location_id', as_index=False)['population'].sum())
            _population['age_group_years_start'] = age_start
            _population['age_group_years_end'] = age_end
            population.append(_population)
    population = pd.concat(population)
    vaccinated = vaccinated.reset_index().merge(population)
    is_adult_only = vaccinated['age_group_years_end'] == 65
    vaccinated.loc[is_adult_only, 'vaccinated'] = vaccinated.loc[is_adult_only, ['cumulative_adults_vaccinated',
                                                                                 'cumulative_essential_vaccinated']].sum(axis=1) / \
                                                  vaccinated.loc[is_adult_only, 'population']
    vaccinated.loc[~is_adult_only, 'vaccinated'] = vaccinated.loc[~is_adult_only, 'cumulative_all_vaccinated'] / \
                                                   vaccinated.loc[~is_adult_only, 'population']
    vaccinated = vaccinated.loc[:, ['location_id', 'date', 'age_group_years_start', 'age_group_years_end', 'vaccinated']]
    
    return vaccinated


def remove_vaccinated(seroprevalence: pd.DataFrame,
                      vaccinated: pd.Series,) -> pd.DataFrame:
    seroprevalence['age_group_years_start'] = seroprevalence['study_start_age'].fillna(20)
    seroprevalence['age_group_years_start'] = np.round(seroprevalence['age_group_years_start'] / 5) * 5
    seroprevalence.loc[seroprevalence['age_group_years_start'] > 20, 'age_group_years_start'] = 20
    
    seroprevalence['age_group_years_end'] = seroprevalence['study_end_age'].fillna(125)
    seroprevalence.loc[seroprevalence['age_group_years_end'] <= 65, 'age_group_years_end'] = 65
    seroprevalence.loc[seroprevalence['age_group_years_end'] > 65, 'age_group_years_end'] = 125
    
    ## start
    # seroprevalence = seroprevalence.rename(columns={'date':'end_date'})
    # seroprevalence = seroprevalence.rename(columns={'start_date':'date'})
    ##
    ## midpoint
    seroprevalence = seroprevalence.rename(columns={'date':'end_date'})
    seroprevalence['n_midpoint_days'] = (seroprevalence['end_date'] - seroprevalence['start_date']).dt.days / 2
    seroprevalence['n_midpoint_days'] = seroprevalence['n_midpoint_days'].astype(int)
    seroprevalence['date'] = seroprevalence.apply(lambda x: x['end_date'] - pd.Timedelta(days=x['n_midpoint_days']), axis=1)
    ##
    ## always
    start_len = len(seroprevalence)
    seroprevalence = seroprevalence.merge(vaccinated, how='left')
    if len(seroprevalence) != start_len:
        raise ValueError('Sero data expanded in vax merge.')
    if seroprevalence.loc[seroprevalence['vaccinated'].isnull(), 'date'].max() >= pd.Timestamp('2020-12-01'):
        raise ValueError('Missing vax after model start (2020-12-01).')
    seroprevalence['vaccinated'] = seroprevalence['vaccinated'].fillna(0)
    ##
    ## start
    # seroprevalence = seroprevalence.rename(columns={'date':'start_date'})
    # seroprevalence = seroprevalence.rename(columns={'end_date':'date'})
    ##
    ## midpoint
    del seroprevalence['date']
    del seroprevalence['n_midpoint_days']
    seroprevalence = seroprevalence.rename(columns={'end_date':'date'})
    ##
    
    seroprevalence.loc[seroprevalence['test_target'] != 'spike', 'vaccinated'] = 0
    
    seroprevalence = seroprevalence.rename(columns={'seroprevalence':'reported_seroprevalence'})
    
    seroprevalence['seroprevalence'] = 1 - (1 - seroprevalence['reported_seroprevalence']) / (1 - seroprevalence['vaccinated'])
    
    del seroprevalence['vaccinated']
    
    return seroprevalence


def load_sensitivity(out_dir: Path, n_samples: int,
                     floor: float = 1e-4, logit_se_cap: float = 1.,):
    sensitivity_data = model_inputs.assay_sensitivity(out_dir)
    logit_mean = logit(sensitivity_data['sensitivity_mean'].clip(floor, 1 - floor))
    logit_sd = sensitivity_data['sensitivity_std'] / \
               (sensitivity_data['sensitivity_mean'].clip(floor, 1 - floor) * \
                (1.0 - sensitivity_data['sensitivity_mean'].clip(floor, 1 - floor)))
    logit_sd = logit_sd.clip(0, logit_se_cap)

    logit_samples = np.random.normal(loc=logit_mean.to_frame().values,
                                     scale=logit_sd.to_frame().values,
                                     size=(len(sensitivity_data), n_samples),)
    samples = expit(logit_samples)

    ## CANNOT DO THIS, MOVES SOME ABOVE 1
    # # re-center around original mean
    # samples *= sensitivity_data[['sensitivity_mean']].values / samples.mean(axis=1, keepdims=True)
    
    # sort
    samples = np.sort(samples, axis=1)

    sample_list = []
    for sample in samples.T:
        _sample = sensitivity_data.drop(['sensitivity_mean', 'sensitivity_std',], axis=1).copy()
        _sample['sensitivity'] = sample
        sample_list.append(_sample.reset_index(drop=True))
    
    return sensitivity_data, sample_list


def apply_seroreversion_adjustment(sensitivity_data: pd.DataFrame,
                                   assay_map: pd.DataFrame,
                                   hospitalized_weights: pd.Series,
                                   seroprevalence: pd.DataFrame,
                                   daily_deaths: pd.Series,
                                   pred_ifr: pd.Series,
                                   population: pd.Series,
                                   durations: Dict,
                                   verbose: bool = True,) -> pd.DataFrame:
    data_assays = sensitivity_data['assay'].unique().tolist()
    excluded_data_assays = [da for da in data_assays if da not in ASSAYS]
    if verbose and excluded_data_assays:
        logger.warning(f"Excluding the following assays found in sensitivity data: {', '.join(excluded_data_assays)}")
    if any([a not in data_assays for a in ASSAYS]):
        raise ValueError('Assay mis-labelled.')
    sensitivity_data = sensitivity_data.loc[sensitivity_data['assay'].isin(ASSAYS)]
    
    source_assays = sensitivity_data[['source', 'assay']].drop_duplicates().values.tolist()
    
    sensitivity_locs = seroprevalence['location_id'].unique().tolist()
    sensitivity_locs = [loc for loc in sensitivity_locs if loc in daily_deaths.reset_index()['location_id'].unique()]
    sensitivity_locs = [1] + sensitivity_locs
    raw_sensitivity = []
    for source_assay in source_assays:
        raw_sensitivity.append(
            fit_hospital_weighted_sensitivity_decay(
                source_assay,
                sensitivity=sensitivity_data.set_index(['source', 'assay']).loc[tuple(source_assay)],
                hospitalized_weights=hospitalized_weights.loc[sensitivity_locs],
            )
        )
    raw_sensitivity = (pd.concat(raw_sensitivity)
                       .set_index(['assay', 'source', 'location_id', 't'])
                       .sort_index()
                       .loc[:, 'sensitivity'])
    
    seroprevalence = seroprevalence.loc[seroprevalence['is_outlier'] == 0]
    
    seroprevalence = seroprevalence.merge(assay_map, how='left')
    missing_match = seroprevalence['assay_map'].isnull()
    is_N = seroprevalence['test_target'] == 'nucleocapsid'
    is_S = seroprevalence['test_target'] == 'spike'
    is_other = ~(is_N | is_S)
    seroprevalence.loc[missing_match & is_N, 'assay_map'] = 'N-Roche, N-Abbott'
    seroprevalence.loc[missing_match & is_S, 'assay_map'] = 'S-Roche, S-Ortho Ig, S-Ortho IgG, S-DiaSorin, S-EuroImmun, S-Oxford'
    seroprevalence.loc[missing_match & is_other, 'assay_map'] = 'N-Roche, ' \
                                                                'N-Abbott, ' \
                                                                'S-Roche, ' \
                                                                'S-Ortho Ig, S-Ortho IgG, ' \
                                                                'S-DiaSorin, S-EuroImmun, ' \
                                                                'S-Oxford'
    if seroprevalence['assay_map'].isnull().any():
        raise ValueError(f"Unmapped seroprevalence data: {seroprevalence.loc[seroprevalence['assay_map'].isnull()]}")

    assay_combinations = seroprevalence['assay_map'].unique().tolist()
    
    daily_infections = ((daily_deaths / pred_ifr)
                        .dropna()
                        .rename('daily_infections')
                        .reset_index())
    daily_infections['date'] -= pd.Timedelta(days=durations['sero_to_death'])
    daily_infections = daily_infections.set_index(['location_id', 'date']).loc[:, 'daily_infections']
    daily_infections /= population
    
    sensitivity_list = []
    seroprevalence_list = []
    for assay_combination in assay_combinations:
        if verbose:
            logger.info(f'Adjusting for sensitvity decay: {assay_combination}')
        ac_sensitivity = (raw_sensitivity
                          .loc[assay_combination.split(', ')]
                          .reset_index()
                          .groupby(['location_id', 't'])['sensitivity'].mean())
        ac_seroprevalence = (seroprevalence
                             .loc[seroprevalence['assay_map'] == assay_combination].copy())
        ac_seroprevalence = seroreversion_adjustment(
            daily_infections.copy(),
            ac_sensitivity.copy(),
            ac_seroprevalence.copy(),
            verbose=verbose,
        )
        
        ac_sensitivity = (ac_sensitivity
                          .loc[ac_seroprevalence['location_id'].unique().tolist()]
                          .reset_index())
        ac_sensitivity['assay'] = assay_combination
        sensitivity_list.append(ac_sensitivity)
        
        ac_seroprevalence['is_outlier'] = 0
        ac_seroprevalence['assay'] = assay_combination
        seroprevalence_list.append(ac_seroprevalence)
    sensitivity = pd.concat(sensitivity_list)
    seroprevalence = pd.concat(seroprevalence_list)
    
    # just save global
    raw_sensitivity = raw_sensitivity.loc[:, :, 1, :]
    
    return raw_sensitivity, sensitivity, seroprevalence


def fit_sensitivity_decay_curvefit(t: np.array, sensitivity: np.array, increasing: bool, t_N: int = 720) -> pd.DataFrame:
    def sigmoid(x, x0, k):
        y = 1 / (1 + np.exp(-k * (x-x0)))
        return y
    
    if increasing:
        bounds = ([-np.inf, 1e-4], [np.inf, 0.5])
    else:
        bounds = ([-np.inf, -0.5], [np.inf, -1e-4])
    popt, pcov = curve_fit(sigmoid,
                           t, sensitivity,
                           method='dogbox',
                           bounds=bounds, max_nfev=1000)
    
    t_pred = np.arange(0, t_N + 1)
    sensitivity_pred = sigmoid(t_pred, *popt)
        
    return pd.DataFrame({'t': t_pred, 'sensitivity': sensitivity_pred})


def fit_sensitivity_decay_mrbrt(sensitivity_data: pd.DataFrame, increasing: bool, t_N: int = 720) -> pd.DataFrame:
    sensitivity_data = sensitivity_data.loc[:, ['t', 'sensitivity',]]
    sensitivity_data['sensitivity'] = logit(sensitivity_data['sensitivity'])
    sensitivity_data['intercept'] = 1
    sensitivity_data['se'] = 1
    sensitivity_data['location_id'] = 1

    if increasing:
        mono_dir = 'increasing'
    else:
        mono_dir = 'decreasing'
        
    n_k = min(max(len(sensitivity_data) - 3, 2), 10,)
    k = np.hstack([[0, 0.1], np.linspace(0.1, 1, n_k)[1:]])
    max_t = sensitivity_data['t'].max()

    mr_model = mrbrt.run_mr_model(
        model_data=sensitivity_data,
        dep_var='sensitivity', dep_var_se='se',
        fe_vars=['intercept', 't'], re_vars=[],
        group_var='location_id',
        prior_dict={'intercept':{},
                    't': {'use_spline': True,
                          'spline_knots_type': 'domain',
                          'spline_knots': np.linspace(0, 1, n_k),
                          'spline_degree': 1,
                          'prior_spline_monotonicity': mono_dir,
                          'prior_spline_monotonicity_domain': (60 / max_t, 1),
                         },}
    )
    t_pred = np.arange(t_N + 1)
    sensitivity_pred, _ = mrbrt.predict(
        pred_data=pd.DataFrame({'intercept': 1,
                                't': t_pred,
                                'location_id': 1,
                                'date': t_pred,}),
        hierarchy=None,
        mr_model=mr_model,
        pred_replace_dict={},
        pred_exclude_vars=[],
        dep_var='sensitivity', dep_var_se='se',
        fe_vars=['t'], re_vars=[],
        group_var='location_id',
        sensitivity=True,
    )
        
    return pd.DataFrame({'t': t_pred, 'sensitivity': expit(sensitivity_pred['sensitivity'])})


def fit_hospital_weighted_sensitivity_decay(source_assay: Tuple[str, str],
                                            sensitivity: pd.DataFrame,
                                            hospitalized_weights: pd.Series,) -> pd.DataFrame:
    source, assay = source_assay
    increasing = assay in INCREASING
    
    if source not in ['Peluso', 'Perez-Saez', 'Bond', 'Muecksch', 'Lumley']:
        raise ValueError(f'Unexpected sensitivity source: {source}')
    
    hosp_sensitivity = sensitivity.loc[sensitivity['hospitalization_status'] == 'Hospitalized']
    nonhosp_sensitivity = sensitivity.loc[sensitivity['hospitalization_status'] == 'Non-hospitalized']
    if source == 'Peluso':
        hosp_sensitivity = fit_sensitivity_decay_curvefit(hosp_sensitivity['t'].values,
                                                          hosp_sensitivity['sensitivity'].values,
                                                          increasing,)
        nonhosp_sensitivity = fit_sensitivity_decay_curvefit(nonhosp_sensitivity['t'].values,
                                                             nonhosp_sensitivity['sensitivity'].values,
                                                             increasing,)
    else:
        hosp_sensitivity = fit_sensitivity_decay_mrbrt(hosp_sensitivity.loc[:, ['t', 'sensitivity']],
                                                       increasing,)
        nonhosp_sensitivity = fit_sensitivity_decay_mrbrt(hosp_sensitivity.loc[:, ['t', 'sensitivity']],
                                                          increasing,)
    sensitivity = (hosp_sensitivity
                   .rename(columns={'sensitivity':'hosp_sensitivity'})
                   .merge(nonhosp_sensitivity
                          .rename(columns={'sensitivity':'nonhosp_sensitivity'})))
    sensitivity['key'] = 0
    hospitalized_weights = hospitalized_weights.rename('hospitalized_weights')
    hospitalized_weights = hospitalized_weights.reset_index()
    hospitalized_weights['key'] = 0
    sensitivity = sensitivity.merge(hospitalized_weights, on='key', how='outer')
    
    sensitivity['hosp_sensitivity'] = scale_to_bounds(sensitivity['hosp_sensitivity'],
                                                      SEROREV_LB, 1.,)
    sensitivity['nonhosp_sensitivity'] = scale_to_bounds(sensitivity['nonhosp_sensitivity'],
                                                         SEROREV_LB, 1.,)
    sensitivity['sensitivity'] = (sensitivity['hosp_sensitivity'] * sensitivity['hospitalized_weights']) + \
                                 (sensitivity['nonhosp_sensitivity'] * (1 - sensitivity['hospitalized_weights']))
    sensitivity = sensitivity.reset_index()
    
    sensitivity['source'] = source
    sensitivity['assay'] = assay
    
    return sensitivity.loc[:, ['location_id', 'source', 'assay', 't', 'sensitivity', 'hosp_sensitivity', 'nonhosp_sensitivity']]


def calculate_seroreversion_factor(daily_infections: pd.DataFrame, sensitivity: pd.Series,
                                   sero_date: pd.Timestamp, sero_corr: bool,) -> float:
    daily_infections['t'] = (sero_date - daily_infections['date']).dt.days
    daily_infections = daily_infections.loc[daily_infections['t'] >= 0]
    if sero_corr not in [0, 1]:
        raise ValueError('`manufacturer_correction` should be 0 or 1.')
    if sero_corr == 1:
        # study adjusted for sensitivity, set baseline to 1
        sensitivity /= sensitivity.max()
    daily_infections = daily_infections.merge(sensitivity.reset_index(), how='left')
    if daily_infections['sensitivity'].isnull().any():
        raise ValueError(f"Unmatched sero/sens points: {daily_infections.loc[daily_infections['sensitivity'].isnull()]}")
    
    daily_infections['daily_infections'] *= min(1, 1 / daily_infections['daily_infections'].sum())
    seroreversion_factor = (
        (1 - daily_infections['daily_infections'].sum())
        /
        (1 - (daily_infections['daily_infections'] * daily_infections['sensitivity']).sum())
    )
    seroreversion_factor = max(0, seroreversion_factor)
    seroreversion_factor = min(1, seroreversion_factor)

    return seroreversion_factor
    
    
def location_seroreversion_adjustment(location_id: int,
                                      daily_infections: pd.Series, sensitivity: pd.Series,
                                      seroprevalence: pd.DataFrame) -> pd.DataFrame:
    daily_infections = daily_infections.loc[location_id].reset_index()
    sensitivity = sensitivity.loc[location_id]
    seroprevalence = seroprevalence.loc[seroprevalence['location_id'] == location_id,
                                        ['data_id', 'date', 'manufacturer_correction', 'seroprevalence']
                                       ].reset_index(drop=True)
    adj_seroprevalence = []
    for i, (sero_data_id, sero_date, sero_corr, sero_value) in enumerate(zip(seroprevalence['data_id'],
                                                                             seroprevalence['date'],
                                                                             seroprevalence['manufacturer_correction'],
                                                                             seroprevalence['seroprevalence'],)):
        seroreversion_factor = calculate_seroreversion_factor(
            daily_infections.copy(), sensitivity.copy(), sero_date, sero_corr,
        )
        adj_seroprevalence.append(pd.DataFrame({
            'data_id': sero_data_id,
            'date': sero_date,
            'seroprevalence': 1 - (1 - sero_value) * seroreversion_factor
            # ## SENSITIVITY ANALYSIS - No sensitivity decay in serological assays
            # 'seroprevalence': sero_value
        }, index=[i]))
    adj_seroprevalence = pd.concat(adj_seroprevalence)
    adj_seroprevalence['location_id'] = location_id
    
    return adj_seroprevalence


def seroreversion_adjustment(daily_infections: pd.Series, sensitivity: pd.Series,
                             seroprevalence: pd.DataFrame, verbose: bool) -> pd.DataFrame:
    # # determine waning sensitivity adjustment based on midpoint of survey
    # orig_date = seroprevalence[['data_id', 'date']].copy()
    # seroprevalence['n_midpoint_days'] = (seroprevalence['date'] - seroprevalence['start_date']).dt.days / 2
    # seroprevalence['n_midpoint_days'] = seroprevalence['n_midpoint_days'].astype(int)
    # seroprevalence['date'] = seroprevalence.apply(lambda x: x['date'] - pd.Timedelta(days=x['n_midpoint_days']), axis=1)
    # del seroprevalence['n_midpoint_days']
    
    seroprevalence_list = []
    location_ids = seroprevalence['location_id'].unique().tolist()
    location_ids = [location_id for location_id in location_ids if location_id in daily_infections.reset_index()['location_id'].to_list()]
    
    _lwa = functools.partial(
        location_seroreversion_adjustment,
        daily_infections=daily_infections, sensitivity=sensitivity,
        seroprevalence=seroprevalence,
    )
    with multiprocessing.Pool(int(OMP_NUM_THREADS)) as p:
        if verbose:
            seroprevalence = list(tqdm(p.imap(_lwa, location_ids), total=len(location_ids), file=sys.stdout))
        else:
            seroprevalence = list(p.imap(_lwa, location_ids))
    seroprevalence = pd.concat(seroprevalence).reset_index(drop=True)
    
    # del seroprevalence['date']
    # seroprevalence = seroprevalence.merge(orig_date)
    
    return seroprevalence
