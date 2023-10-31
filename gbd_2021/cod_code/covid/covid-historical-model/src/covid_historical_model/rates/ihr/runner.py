from typing import Dict, List
from pathlib import Path
from collections import namedtuple
from loguru import logger

import pandas as pd

from covid_historical_model.rates import (
    ihr, variants_vaccines, squeeze
)

RESULTS = namedtuple('Results',
                     'seroprevalence model_data mr_model_dict pred_location_map level_lambdas ' \
                     'daily_numerator pred pred_unadj pred_fe pred_lr pred_hr age_stand_scaling_factor')


def runner(input_data: Dict,
           pred_ifr: pd.Series,
           covariate_list: List[str],
           durations: Dict,
           day_0: pd.Timestamp,
           pred_start_date: pd.Timestamp,
           pred_end_date: pd.Timestamp,
           verbose: bool = True,) -> namedtuple:
    logger.info('variant/vax data scalar')
    ihr_data_scalar = variants_vaccines.get_ratio_data_scalar(
        rate_age_pattern=input_data['ihr_age_pattern'].copy(),
        denom_age_pattern=input_data['sero_age_pattern'].copy(),
        age_spec_population=input_data['age_spec_population'].copy(),
        daily=input_data['daily_deaths'].copy(),
        rate=pred_ifr.copy(),
        day_shift=durations['exposure_to_admission'],
        escape_variant_prevalence=input_data['escape_variant_prevalence'].copy(),
        severity_variant_prevalence=input_data['severity_variant_prevalence'].copy(),
        vaccine_coverage=input_data['vaccine_coverage'].copy(),
        population=input_data['population'].copy(),
        location_dates=input_data['seroprevalence'][['location_id', 'date']].drop_duplicates().values.tolist(),
        durations=durations.copy(),
        variant_risk_ratio=input_data['variant_risk_ratio'],
        verbose=verbose,
    )

    logger.info('setup')
    model_data = ihr.data.create_model_data(day_0=day_0, durations=durations,
                                            ihr_data_scalar=ihr_data_scalar,
                                            **input_data)
    pred_data = ihr.data.create_pred_data(
        pred_start_date=pred_start_date, pred_end_date=pred_end_date,
        day_0=day_0, **input_data
    )
    
    logger.info('cascade')
    # check what NAs in data might be about, get rid of them in safer way
    mr_model_dict, prior_dicts, pred, pred_fe, pred_location_map, age_stand_scaling_factor, level_lambdas = ihr.model.run_model(
        model_data=model_data.copy(),
        pred_data=pred_data.copy(),
        covariate_list=covariate_list,
        verbose=verbose,
        **input_data
    )
    pred_unadj = pred.copy()
    
    logger.info('post')
    pred, pred_lr, pred_hr, *_ = variants_vaccines.variants_vaccines(
        rate_age_pattern=input_data['ihr_age_pattern'].copy(),
        denom_age_pattern=input_data['sero_age_pattern'].copy(),
        age_spec_population=input_data['age_spec_population'].copy(),
        rate=pred.copy(),
        day_shift=durations['exposure_to_admission'],
        escape_variant_prevalence=input_data['escape_variant_prevalence'].copy(),
        severity_variant_prevalence=input_data['severity_variant_prevalence'].copy(),
        vaccine_coverage=input_data['vaccine_coverage'].copy(),
        population=input_data['population'].copy(),
        variant_risk_ratio=input_data['variant_risk_ratio'],
        verbose=verbose,
    )
    
    logger.info('squeeze')
    lr_rr = pred_lr / pred
    hr_rr = pred_hr / pred
    pred = squeeze.squeeze(
        daily=input_data['daily_hospitalizations'].copy(),
        rate=pred.copy(),
        day_shift=durations['exposure_to_admission'],
        population=input_data['population'].copy(),
        cross_variant_immunity=input_data['cross_variant_immunity'],
        escape_variant_prevalence=input_data['escape_variant_prevalence'].copy(),
        vaccine_coverage=input_data['vaccine_coverage'].copy(),
    )
    pred_lr = lr_rr * pred
    pred_hr = hr_rr * pred
    
    logger.info('compiling results')
    results = RESULTS(
        seroprevalence=input_data['seroprevalence'],
        model_data=model_data,
        mr_model_dict=mr_model_dict,
        pred_location_map=pred_location_map,
        level_lambdas=level_lambdas,
        daily_numerator=input_data['daily_hospitalizations'],
        pred=pred,
        pred_unadj=pred_unadj,
        pred_fe=pred_fe,
        pred_lr=pred_lr,
        pred_hr=pred_hr,
        age_stand_scaling_factor=age_stand_scaling_factor,
    )

    return results
