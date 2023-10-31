import sys
from pathlib import Path
from typing import Tuple, List, Dict
import functools
import multiprocessing
from tqdm import tqdm
from loguru import logger

import pandas as pd
import numpy as np

from covid_historical_model.rates import ifr
from covid_historical_model.mrbrt import cascade, mrbrt
from covid_historical_model.cluster import OMP_NUM_THREADS
from covid_historical_model.utils.misc import get_random_seed


def get_coefficients(n_covariate_list: Tuple[int, List[str]],
                     model_data: pd.DataFrame, input_data: Dict,
                     day_0: pd.Timestamp, day_inflection: pd.Timestamp,):
    n, covariate_list = n_covariate_list
    seed = get_random_seed(f'covariate_list_{n}')
    np.random.seed(seed)
    
    model_data, age_stand_scaling_factor, level_lambdas, var_args, \
    global_prior_dict, location_prior_dict, pred_replace_dict, pred_exclude_vars = ifr.model.prepare_model(
        model_data=model_data,
        ifr_age_pattern=input_data['ifr_age_pattern'],
        sero_age_pattern=input_data['sero_age_pattern'],
        age_spec_population=input_data['age_spec_population'],
        day_0=day_0,
        day_inflection=day_inflection,
        covariate_list=covariate_list,
    )

    mr_model_dict = {}
    prior_dict = {fe_var: {} for fe_var in var_args['fe_vars'] if fe_var not in global_prior_dict.keys()}
    prior_dict.update(global_prior_dict)

    global_mr_data = mrbrt.create_mr_data(model_data,
                                          var_args['dep_var'], var_args['dep_var_se'],
                                          var_args['fe_vars'], var_args['group_var'])

    location_mr_model, location_prior_dict = cascade.model_location(
        model_name='ifr',
        location_id=1,
        model_data=model_data,
        prior_dict=prior_dict,
        location_prior_dict=location_prior_dict,
        level_lambda={lk: 1. for lk in level_lambdas[0].keys()},
        global_mr_data=global_mr_data,
        var_args=var_args,
    )
    
    y = location_mr_model.data.obs
    y_hat = location_mr_model.predict(location_mr_model.data)
    r2 = 1 - sum((y - y_hat) ** 2) / sum((y - y.mean()) ** 2)

    return pd.DataFrame({'covariates': '//'.join(covariate_list), 'r2': r2}, index=[0])


def covariate_selection(n_samples: int, test_combinations: List[List[str]],
                        out_dir: Path, excess_mortality: bool,
                        age_rates_root: Path,
                        shared: Dict,
                        reported_seroprevalence: pd.DataFrame,
                        covariate_options: List[str],
                        covariates: List[pd.Series],
                        reported_sensitivity_data: pd.DataFrame,
                        vaccine_coverage: pd.DataFrame,
                        escape_variant_prevalence: pd.Series,
                        severity_variant_prevalence: pd.Series,
                        cross_variant_immunity_samples: List,
                        variant_risk_ratio_samples: List,
                        pred_start_date: pd.Timestamp,
                        pred_end_date: pd.Timestamp,
                        cutoff_pct: float,
                        durations: Dict,
                        exclude_US_UK: bool = True,
                        day_0: pd.Timestamp = pd.Timestamp('2020-03-15'),
                        day_inflection: pd.Timestamp = pd.Timestamp('2021-01-01'),
                        verbose: bool = True,):
    input_data = ifr.data.load_input_data(out_dir=out_dir,
                                          excess_mortality=excess_mortality,
                                          excess_mortality_draw=-1,
                                          age_rates_root=age_rates_root,
                                          shared=shared, seroprevalence=reported_seroprevalence,
                                          covariates=covariates,
                                          sensitivity_data=reported_sensitivity_data.rename(
                                              columns={'sensitivity_mean':'sensitivity'}
                                          ).drop('sensitivity_std', axis=1),
                                          vaccine_coverage=vaccine_coverage,
                                          escape_variant_prevalence=escape_variant_prevalence,
                                          severity_variant_prevalence=severity_variant_prevalence,
                                          cross_variant_immunity=np.mean(cross_variant_immunity_samples),
                                          variant_risk_ratio=np.mean(variant_risk_ratio_samples),
                                          verbose=False)
    # use dumb IFR to do corrections
    naive_pred = pd.Series(
        1,
        index=pd.MultiIndex.from_product([input_data['hierarchy']['location_id'].to_list(),
                                          pd.date_range(pred_start_date, pred_end_date)],
                                         names=['location_id', 'date'])
    ) * 0.005

    # do corrections
    _, _, _, _, seroprevalence, ifr_data_scalar = ifr.runner.derive_intermediate_outputs(
        input_data, naive_pred, durations, verbose=False
    )
    
    # prepare model data with corrected inputs
    input_data['seroprevalence'] = seroprevalence
    model_data = ifr.data.create_model_data(day_0=day_0, durations=durations,
                                            ifr_data_scalar=ifr_data_scalar,
                                            **input_data)
    
    if exclude_US_UK:
        logger.info('Excluding US and UK data from covariate selection, '
                    'serial measureements there are over-representated in dataset.')
        hierarchy = input_data['hierarchy'].copy()
        usa = hierarchy.loc[hierarchy['path_to_top_parent'].apply(lambda x: '102' in x.split(',')), 'location_id'].to_list()
        uk = hierarchy.loc[hierarchy['path_to_top_parent'].apply(lambda x: '95' in x.split(',')), 'location_id'].to_list()
        model_data = model_data.loc[~model_data['location_id'].isin(usa + uk)]
        del hierarchy, usa, uk

    _gc = functools.partial(
        get_coefficients,
        model_data=model_data.copy(), input_data=input_data,
        day_0=day_0, day_inflection=day_inflection,
    )
    with multiprocessing.Pool(int(OMP_NUM_THREADS)) as p:
        performance_data = list(tqdm(p.imap(_gc, enumerate(test_combinations)),
                                     total=len(test_combinations), file=sys.stdout))
    performance_data = pd.concat(performance_data).sort_values(['r2', 'covariates'], ascending=False)
    performance_data = performance_data.reset_index(drop=True)
    
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    # LIMIT REDUNDANCY IN COVARIATE POOL
    if cutoff_pct < 1:
        cutoff_n = int(n_samples * cutoff_pct)
        sorted_covariate_options = performance_data.loc[performance_data['covariates'].isin(covariate_options),
                                                        'covariates'].to_list()
        lim_covariate_combinations = []
        for n, covariate in enumerate(sorted_covariate_options):
            lim_covariate_combinations += \
                performance_data.loc[(performance_data['covariates'].apply(lambda x: covariate in x.split('//'))) &
                                     (performance_data['covariates'].apply(lambda x: all([x_c not in sorted_covariate_options[: n] 
                                                                                          for x_c in x.split('//')]))),
                                     'covariates'].tolist()[:cutoff_n]
        performance_data = performance_data.loc[performance_data['covariates'].isin(lim_covariate_combinations)]
    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    performance_data = performance_data[:n_samples]
    if verbose:
        logger.info(f"Global model performance: {performance_data['r2'].describe()}")

    selected_combinations = performance_data['covariates'].to_list()
    selected_combinations = [cc.split('//') for cc in selected_combinations]

    return selected_combinations
