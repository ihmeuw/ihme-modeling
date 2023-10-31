import sys
import os
from pathlib import Path
from typing import Dict, List
import itertools
from collections import namedtuple
from loguru import logger
import dill as pickle

import pandas as pd

from covid_shared.cli_tools.logging import configure_logging_to_terminal

from covid_historical_model.rates import ifr
from covid_historical_model.rates import ihr
from covid_historical_model.rates import reinfection
from covid_historical_model.rates import serology
from covid_historical_model.rates import age_standardization
from covid_historical_model.rates import variants_vaccines
from covid_historical_model.rates import squeeze

RESULTS = namedtuple('Results',
                     'seroprevalence model_data mr_model_dict pred_location_map daily_numerator level_lambdas ' \
                     'pred pred_unadj pred_fe pred_lr pred_hr pct_inf_lr pct_inf_hr age_stand_scaling_factor')


def runner(input_data: Dict,
           day_inflection: str,
           covariate_list: List[str],
           durations: Dict,
           day_0: pd.Timestamp,
           pred_start_date: pd.Timestamp,
           pred_end_date: pd.Timestamp,
           verbose: bool = True,) -> Dict:
    ## SET UP
    logger.info(f"set up:\n day_inflection: {str(day_inflection)}\n covariates: {', '.join(covariate_list)}")

    model_data = ifr.data.create_model_data(day_0=day_0, durations=durations,
                                            ifr_data_scalar=None,
                                            **input_data)
    pred_data = ifr.data.create_pred_data(
        pred_start_date=pred_start_date, pred_end_date=pred_end_date,
        day_0=day_0, **input_data
    )
    
    ## STAGE 1 MODEL
    # check what NAs in pred data might be about, get rid of them in safer way
    logger.info('stage 1 model')
    mr_model_dict, prior_dicts, pred, pred_fe, pred_location_map, age_stand_scaling_factor, level_lambdas = ifr.model.run_model(
        model_data=model_data.copy(),
        pred_data=pred_data.copy(),
        day_0=day_0, day_inflection=day_inflection,
        covariate_list=covariate_list,
        verbose=False,
        **input_data
    )
    
    ## GET INTERMEDIATE OUTPUTS
    cumul_reinfection_inflation_factor, daily_reinfection_inflation_factor,\
    raw_sensitivity_curves, sensitivity, seroprevalence,\
    ifr_data_scalar = derive_intermediate_outputs(input_data, pred, durations, verbose)
    
    ## SET UP REFIT
    logger.info('set up stage 2')
    refit_input_data = input_data.copy()
    refit_input_data['seroprevalence'] = seroprevalence
    del seroprevalence
    refit_model_data = ifr.data.create_model_data(day_0=day_0, durations=durations,
                                                  ifr_data_scalar=ifr_data_scalar,
                                                  **refit_input_data)
    refit_pred_data = ifr.data.create_pred_data(
        pred_start_date=pred_start_date, pred_end_date=pred_end_date,
        day_0=day_0, **refit_input_data
    )
    
    ## STAGE 2 MODEL
    # check what NAs in pred data might be about, get rid of them in safer way
    logger.info('stage 2 model')
    refit_mr_model_dict, refit_prior_dicts, refit_pred, refit_pred_fe, \
    refit_pred_location_map, refit_age_stand_scaling_factor, refit_level_lambdas = ifr.model.run_model(
        model_data=refit_model_data.copy(),
        pred_data=refit_pred_data.dropna().copy(),
        day_0=day_0, day_inflection=day_inflection,
        covariate_list=covariate_list,
        verbose=False,
        **refit_input_data
    )
    refit_pred_unadj = refit_pred.copy()
    
    ## POST
    logger.info('post')
    refit_pred, refit_pred_lr, refit_pred_hr, pct_inf_lr, pct_inf_hr = variants_vaccines.variants_vaccines(
        rate_age_pattern=refit_input_data['ifr_age_pattern'].copy(),
        denom_age_pattern=refit_input_data['sero_age_pattern'].copy(),
        age_spec_population=refit_input_data['age_spec_population'].copy(),
        rate=refit_pred.copy(),
        day_shift=durations['exposure_to_death'],
        escape_variant_prevalence=refit_input_data['escape_variant_prevalence'].copy(),
        severity_variant_prevalence=refit_input_data['severity_variant_prevalence'].copy(),
        vaccine_coverage=refit_input_data['vaccine_coverage'].copy(),
        population=refit_input_data['population'].copy(),
        variant_risk_ratio=input_data['variant_risk_ratio'],
        verbose=verbose,
    )
    
    ## SQUEEZE
    logger.info('squeeze')
    lr_rr = refit_pred_lr / refit_pred
    hr_rr = refit_pred_hr / refit_pred
    refit_pred = squeeze.squeeze(
        daily=refit_input_data['daily_deaths'].copy(),
        rate=refit_pred.copy(),
        day_shift=durations['exposure_to_death'],
        population=refit_input_data['population'].copy(),
        cross_variant_immunity=refit_input_data['cross_variant_immunity'],
        escape_variant_prevalence=refit_input_data['escape_variant_prevalence'].copy(),
        vaccine_coverage=refit_input_data['vaccine_coverage'].copy(),
    )
    refit_pred_lr = lr_rr * refit_pred
    refit_pred_hr = hr_rr * refit_pred
    
    ## results
    logger.info('compiling results')
    results = RESULTS(
        seroprevalence=input_data['seroprevalence'],
        model_data=model_data,
        mr_model_dict=mr_model_dict,
        pred_location_map=pred_location_map,
        level_lambdas=level_lambdas,
        daily_numerator=input_data['daily_deaths'],
        pred=pred,
        pred_unadj=pred,
        pred_fe=pred_fe,
        pred_lr=None,
        pred_hr=None,
        pct_inf_lr=None,
        pct_inf_hr=None,
        age_stand_scaling_factor=age_stand_scaling_factor,
    )
    refit_results = RESULTS(
        seroprevalence=refit_input_data['seroprevalence'],
        model_data=refit_model_data,
        mr_model_dict=refit_mr_model_dict,
        pred_location_map=refit_pred_location_map,
        level_lambdas=refit_level_lambdas,
        daily_numerator=refit_input_data['daily_deaths'],
        pred=refit_pred,
        pred_unadj=refit_pred_unadj,
        pred_fe=refit_pred_fe,
        pred_lr=refit_pred_lr,
        pred_hr=refit_pred_hr,
        pct_inf_lr=pct_inf_lr,
        pct_inf_hr=pct_inf_hr,
        age_stand_scaling_factor=refit_age_stand_scaling_factor,
    )
    
    return (refit_results, refit_input_data['seroprevalence'], raw_sensitivity_curves, sensitivity,
            cumul_reinfection_inflation_factor, daily_reinfection_inflation_factor)


def derive_intermediate_outputs(input_data: Dict, pred: pd.Series, durations: Dict,
                                verbose: bool):
    ## REINFECTION
    # account for escape variant re-infection
    if verbose:
        logger.info('reinfection')
    cumul_reinfection_inflation_factor, daily_reinfection_inflation_factor, seroprevalence = reinfection.add_repeat_infections(
        pred_ifr=pred.copy(),
        durations=durations,
        verbose=False,
        **input_data,
    )

    ## WANING SENSITIVITY ADJUSTMENT
    # account for waning antibody detection
    if verbose:
        logger.info('sensitivity')
    hospitalized_weights = age_standardization.get_all_age_rate(
        input_data['ihr_age_pattern'].copy(), input_data['sero_age_pattern'].copy(),
        input_data['age_spec_population'].copy()
    )
    raw_sensitivity_curves, sensitivity, seroprevalence = serology.apply_seroreversion_adjustment(
        input_data['sensitivity_data'].copy(),
        input_data['assay_map'].copy(),
        hospitalized_weights.copy(),
        input_data['seroprevalence'].copy(),
        input_data['daily_deaths'].copy(),
        pred.copy(),
        input_data['population'].copy(),
        durations,
        verbose=verbose,
    )
    
    ## REMOVE EFFECTS OF VARIANTS/VACCINATIONS FROM INPUT DATA
    if verbose:
        logger.info('variant/vax data scalar')
    ifr_data_scalar = variants_vaccines.get_ratio_data_scalar(
        rate_age_pattern=input_data['ifr_age_pattern'].copy(),
        denom_age_pattern=input_data['sero_age_pattern'].copy(),
        age_spec_population=input_data['age_spec_population'].copy(),
        rate=pred.copy(),
        day_shift=durations['exposure_to_death'],
        escape_variant_prevalence=input_data['escape_variant_prevalence'].copy(),
        severity_variant_prevalence=input_data['severity_variant_prevalence'].copy(),
        vaccine_coverage=input_data['vaccine_coverage'].copy(),
        population=input_data['population'].copy(),
        daily=input_data['daily_deaths'].copy(),
        location_dates=seroprevalence[['location_id', 'date']].drop_duplicates().values.tolist(),
        durations=durations.copy(),
        variant_risk_ratio=input_data['variant_risk_ratio'],
        verbose=verbose,
    )
    
    return (cumul_reinfection_inflation_factor, daily_reinfection_inflation_factor,
            raw_sensitivity_curves, sensitivity, seroprevalence,
            ifr_data_scalar)


def main(day_inflection: str,
         input_data_path: str,
         covariate_list_path: str,
         outputs_path: str):
    logger.info('reading inputs')
    with Path(input_data_path).open('rb') as file:
        input_data = pickle.load(file)
    with Path(covariate_list_path).open('rb') as file:
        covariate_list = pickle.load(file)
        
    outputs = runner(input_data=input_data,
                     day_inflection=day_inflection,
                     covariate_list=covariate_list,)
    
    logger.info('writing')
    with Path(outputs_path).open('wb') as file:
        pickle.dump({day_inflection: outputs}, file)
    
    logger.info('done')


if __name__ == '__main__':
    os.environ['OMP_NUM_THREADS'] = '6'
    configure_logging_to_terminal(verbose=2)
    
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
