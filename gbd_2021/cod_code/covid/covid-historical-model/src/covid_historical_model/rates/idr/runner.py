from typing import Dict, List
from pathlib import Path
from collections import namedtuple
from loguru import logger

import pandas as pd

from covid_historical_model.rates import idr
from covid_historical_model.rates import squeeze
from covid_historical_model.utils.math import scale_to_bounds

RESULTS = namedtuple('Results',
                     'seroprevalence testing_capacity model_data mr_model_dict pred_location_map level_lambdas ' \
                     'floor_data floor_rmse daily_numerator pred pred_fe')


def runner(input_data: Dict,
           pred_ifr: pd.Series,
           covariate_list: List[str],
           durations: Dict,
           pred_start_date: pd.Timestamp,
           pred_end_date: pd.Timestamp,
           verbose: bool = True,) -> namedtuple:
    logger.info('set up')
    model_data = idr.data.create_model_data(pred_ifr=pred_ifr, durations=durations, verbose=verbose, **input_data)
    pred_data = idr.data.create_pred_data(
        pred_start_date=pred_start_date, pred_end_date=pred_end_date,
        **input_data
    )
    
    logger.info('cascade')
    # check what NAs in pred data might be about, get rid of them in safer way
    mr_model_dict, prior_dicts, pred, pred_fe, pred_location_map, level_lambdas = idr.model.run_model(
        model_data=model_data.copy(),
        pred_data=pred_data.copy(),
        covariate_list=covariate_list,
        verbose=verbose,
        **input_data
    )
    
    logger.info('derive floors')
    rmse_data, floor_data = idr.flooring.find_idr_floor(
        pred=pred.copy(),
        daily_cases=input_data['daily_cases'].copy(),
        serosurveys=(input_data['seroprevalence']
                     .set_index(['location_id', 'date'])
                     .sort_index()
                     .loc[:, 'seroprevalence']).copy(),
        population=input_data['population'].copy(),
        hierarchy=input_data['adj_gbd_hierarchy'].copy(),
        test_range=[0.01, 0.1] + list(range(1, 11)),
        verbose=verbose,
    )
    
    logger.info('apply floors')
    pred = (pred.reset_index().set_index('location_id').join(floor_data, how='left'))
    pred = (pred
            .reset_index()
            .groupby('location_id')
            .apply(lambda x: scale_to_bounds(x.set_index('date').loc[:, 'pred_idr'],
                                             x['idr_floor'].unique().item(),
                                             ceiling=1.,))
            .rename('pred_idr'))
    
    logger.info('squeeze')
    pred = squeeze.squeeze(
        daily=input_data['daily_cases'].copy(),
        rate=pred.copy(),
        day_shift=durations['exposure_to_case'],
        population=input_data['population'].copy(),
        cross_variant_immunity=input_data['cross_variant_immunity'],
        escape_variant_prevalence=input_data['escape_variant_prevalence'].copy(),
        vaccine_coverage=input_data['vaccine_coverage'].copy(),
    )
    
    logger.info('derive mean date of infection')
    dates_data = idr.model.determine_mean_date_of_infection(
        location_dates=model_data[['location_id', 'date']].drop_duplicates().values.tolist(),
        daily_cases=input_data['daily_cases'].copy(),
        pred=pred.copy()
    )
    model_data = model_data.merge(dates_data, how='left')
    model_data['mean_infection_date'] = model_data['mean_infection_date'].fillna(model_data['date'])
    model_data = (model_data.loc[:, ['data_id', 'location_id', 'date', 'mean_infection_date', 'idr']].reset_index(drop=True))
    
    logger.info('compiling results')
    results = RESULTS(
        seroprevalence=input_data['seroprevalence'],
        testing_capacity=input_data['testing_capacity'],
        model_data=model_data,
        mr_model_dict=mr_model_dict,
        pred_location_map=pred_location_map,
        level_lambdas=level_lambdas,
        floor_data=floor_data,
        floor_rmse=rmse_data,
        daily_numerator=input_data['daily_cases'],
        pred=pred,
        pred_fe=pred_fe,
    )

    return results
