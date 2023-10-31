from typing import Tuple, Dict, List

import pandas as pd
import numpy as np

from covid_historical_model.utils.math import logit, expit
from covid_historical_model.mrbrt import cascade
from covid_historical_model.rates import age_standardization
from covid_historical_model.etl import model_inputs
from covid_historical_model.rates.covariate_priors import get_covariate_priors, get_covariate_constraints


def run_model(model_data: pd.DataFrame, pred_data: pd.DataFrame,
              ihr_age_pattern: pd.Series, sero_age_pattern: pd.Series, age_spec_population: pd.Series,
              hierarchy: pd.DataFrame, gbd_hierarchy: pd.DataFrame,
              covariate_list: List[str],
              verbose: bool = True,
              **kwargs) -> Tuple[Dict, Dict, pd.Series, pd.Series, pd.Series]:
    age_stand_scaling_factor = age_standardization.get_scaling_factor(
        ihr_age_pattern, sero_age_pattern,
        age_spec_population.loc[[1]], age_spec_population
    )
    model_data = model_data.set_index('location_id')
    model_data['ihr'] *= age_stand_scaling_factor[model_data.index]
    model_data['ihr'] *= model_data['ratio_data_scalar']
    model_data = model_data.reset_index()
    
    model_data['logit_ihr'] = logit(model_data['ihr'])
    model_data['logit_ihr'] = model_data['logit_ihr'].replace((-np.inf, np.inf), np.nan)
    model_data['ihr_se'] = 1
    model_data['logit_ihr_se'] = 1
    model_data['intercept'] = 1
    
    # lose 0s and 1s
    model_data = model_data.loc[model_data['logit_ihr'].notnull()]
    
    covariate_priors = get_covariate_priors(1, 'ihr')
    covariate_priors = {covariate: covariate_priors[covariate] for covariate in covariate_list}
    covariate_constraints = get_covariate_constraints('ihr')
    covariate_constraints = {covariate: covariate_constraints[covariate] for covariate in covariate_list}
    covariate_lambdas_sr_r = {covariate: 3. for covariate in covariate_list}
    covariate_lambdas_admin = {covariate: 100. for covariate in covariate_list}

    var_args = {'dep_var': 'logit_ihr',
                'dep_var_se': 'logit_ihr_se',
                'fe_vars': ['intercept'] + covariate_list,
                'prior_dict': {
                    **covariate_constraints,
                },
                're_vars': [],
                'group_var': 'location_id',}
    global_prior_dict = covariate_priors
    location_prior_dict = {}
    pred_replace_dict = {}
    pred_exclude_vars = []
    level_lambdas = {
        0: {'intercept':   3.,  **covariate_lambdas_sr_r,},  # G->SR
        1: {'intercept':   3.,  **covariate_lambdas_sr_r,},  # SR->R
        2: {'intercept': 100., **covariate_lambdas_admin,},  # R->A0
        3: {'intercept': 100., **covariate_lambdas_admin,},  # A0->A1
        4: {'intercept': 100., **covariate_lambdas_admin,},  # A1->A2
        5: {'intercept': 100., **covariate_lambdas_admin,},  # A2->A3
    }
    
    if var_args['group_var'] != 'location_id':
        raise ValueError('NRMSE data assignment assumes `study_id` == `location_id` (`location_id` must be group_var).')
    
    model_data_cols = ['location_id', 'date', var_args['dep_var'],
                       var_args['dep_var_se']] + var_args['fe_vars']
    model_data = model_data.loc[:, model_data_cols]
    model_data = model_data.dropna()
    mr_model_dict, prior_dicts = cascade.run_cascade(
        model_name='ihr',
        model_data=model_data.copy(),
        hierarchy=hierarchy.copy(),  # run w/ modeling hierarchy
        var_args=var_args.copy(),
        global_prior_dict=global_prior_dict.copy(),
        location_prior_dict=location_prior_dict.copy(),
        level_lambdas=level_lambdas.copy(),
        verbose=False,
    )
    adj_gbd_hierarchy = model_inputs.validate_hierarchies(hierarchy.copy(), gbd_hierarchy.copy())
    pred_data = pred_data.dropna()
    pred, pred_fe, pred_location_map = cascade.predict_cascade(
        pred_data=pred_data.copy(),
        hierarchy=adj_gbd_hierarchy.copy(),  # predict w/ gbd hierarchy
        mr_model_dict=mr_model_dict.copy(),
        pred_replace_dict=pred_replace_dict.copy(),
        pred_exclude_vars=pred_exclude_vars.copy(),
        var_args=var_args.copy(),
        verbose=False,
    )
    
    pred = expit(pred).rename(pred.name.replace('logit_', ''))
    pred_fe = expit(pred_fe).rename(pred_fe.name.replace('logit_', ''))
    
    pred /= age_stand_scaling_factor
    pred_fe /= age_stand_scaling_factor

    return mr_model_dict, prior_dicts, pred.dropna(), pred_fe.dropna(), pred_location_map, \
           age_stand_scaling_factor, level_lambdas
