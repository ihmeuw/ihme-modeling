from typing import Callable, List, Tuple, Dict
from loguru import logger

import pandas as pd
import numpy as np

from mrtool import MRData, LinearCovModel, MRBRT, MRBeRT
from mrtool.core.utils import sample_knots


def estimate_time_series(data: pd.DataFrame,
                         spline_options: Dict,
                         n_knots: int,
                         dep_var: str,
                         dep_trans_in: Callable[[pd.Series], pd.Series] = lambda x: x,
                         weight_data: pd.DataFrame = None,
                         dep_var_se: str = None,
                         dep_se_trans_in: Callable[[pd.Series], pd.Series] = lambda x: x,
                         diff: bool = False,
                         num_submodels: int = 25,
                         single_random_knot: bool = False,
                         min_interval_days: int = 7,
                         dep_trans_out: Callable[[pd.Series], pd.Series] = lambda x: x,
                         split_l_interval: bool = False,
                         split_r_interval: bool = False,
                         verbose: bool = False,) -> Tuple[pd.DataFrame, pd.Series, MRBeRT]:
    if verbose: logger.info('Formatting data.')
    data = data.copy()
    data[dep_var] = dep_trans_in(data[dep_var])
    if diff:
        if verbose: logger.info('For diff model, drop day1 (i.e., if day0 is > 0, day0->day1 diff would be hugely negative).')
        data[dep_var] = data[dep_var].diff()
        data[dep_var] = data[dep_var][data[dep_var].diff().notnull()]
    if data[[dep_var]].shape[1] > 1:
        reshape = True
        data = reshape_data_long(data, dep_var)
        if weight_data is not None:
            weight_data = reshape_data_long(weight_data, dep_var_se)
    else:
        reshape = False
    if weight_data is not None:
        if (data['date'] != weight_data['date']).any():
            raise ValueError('Dates in `data` and `weight_data` not identical.')
        data['se'] = dep_se_trans_in(weight_data[dep_var_se])
    else:
        data['se'] = 1.
    data = data.rename(columns={dep_var:'y'})
    day0 = data['date'].min()
    keep_vars = ['date', 'y', 'se']
    data = data.loc[:, keep_vars]
    start_len = len(data)
    data = data.dropna()
    end_len = len(data)
    if start_len != end_len and not reshape:
        if verbose: logger.debug('NAs in data')
    data['t'] = (data['date'] - day0).dt.days
    
    col_args = {
        'col_obs':'y',
        'col_obs_se':'se',
        'col_covs':['t'],
        #'col_study_id':'date',
    }
    if verbose: logger.info('Getting base knots.')
    min_interval = min_interval_days / data['t'].max()
    if num_submodels == 1 and single_random_knot:
        spline_knots = get_ensemble_knots(n_knots, min_interval, 1)[0]
    else:
        spline_knots = np.linspace(0., 1., n_knots)
        
    if split_l_interval or split_r_interval:
        if num_submodels > 1:
            raise ValueError('Would need to set up functionality to split segments for ensemble.')
        if split_l_interval:
            n_knots += 1
            spline_knots = np.insert(spline_knots, 0, spline_knots[:2].mean())
        if split_r_interval:
            n_knots += 1
            spline_knots = np.insert(spline_knots, -1, spline_knots[-2:].mean())
    
    if verbose: logger.info('Creating model data.')
    mr_data = MRData()
    mr_data.load_df(data, **col_args)
    spline_model = LinearCovModel('t',
                                  use_re=False,
                                  use_spline=True,
                                  use_spline_intercept=True,
                                  spline_knots=spline_knots,
                                  **spline_options)
    if num_submodels > 1:
        if verbose: logger.info('Sampling knots.')
        ensemble_knots = get_ensemble_knots(n_knots, min_interval, num_submodels)
        
        if verbose: logger.info('Initializing model.')
        mr_model = MRBeRT(mr_data, spline_model, ensemble_knots)
    else:
        if verbose: logger.info('Initializing model.')
        mr_model = MRBRT(mr_data, [spline_model])
    
    if verbose: logger.info('Fitting model.')
    mr_model.fit_model()

    if num_submodels > 1:
        if verbose: logger.info('Scoring submodels.')
        mr_model.score_model()
    
    data = data.set_index('date')[['y', 'se']]
    
    if verbose: logger.info('Making prediction.')
    smooth_data = predict_time_series(
        day0=day0,
        dep_var=dep_var,
        mr_model=mr_model,
        dep_trans_out=dep_trans_out,
        diff=diff,
    )
    
    return data, smooth_data, mr_model


def model_intercept(data: pd.DataFrame,
                    dep_var: str,
                    prediction: pd.Series,
                    weight_data: pd.DataFrame = None,
                    dep_var_se: str = None,
                    dep_trans_in: Callable[[pd.Series], pd.Series] = lambda x: x,
                    dep_se_trans_in: Callable[[pd.Series], pd.Series] = lambda x: x,
                    dep_trans_out: Callable[[pd.Series], pd.Series] = lambda x: x,
                    verbose: bool = True):
    data = data.copy()
    data[dep_var] = dep_trans_in(data[dep_var])
    prediction = dep_trans_in(prediction)
    data = reshape_data_long(data, dep_var)
    if weight_data is not None:
        weight_data = reshape_data_long(weight_data, dep_var_se)
        if (data['date'] != weight_data['date']).any():
            raise ValueError('Dates in `data` and `weight_data` not identical.')
        data['se'] = dep_se_trans_in(weight_data[dep_var_se])
    else:
        data['se'] = 1.
    data = data.set_index('date').sort_index()
    data[dep_var] = data[dep_var] - prediction
    data = data.reset_index().dropna()
    data['intercept'] = 1
    
    mr_data = MRData()
    mr_data.load_df(data, 
        col_obs=dep_var,
        col_obs_se='se',
        col_covs=['intercept'],
        col_study_id='date',)
    intercept_model = LinearCovModel('intercept', use_re=False,)
    mr_model = MRBRT(mr_data, [intercept_model])
    mr_model.fit_model()
    
    intercept = mr_model.beta_soln
    
    prediction += intercept
    prediction = dep_trans_out(prediction)
    
    return prediction
    
    
def reshape_data_long(data: pd.DataFrame, value_var: str) -> pd.DataFrame:
    data = data.loc[:, ['date', value_var]]
    data.columns = ['date'] + [f'{value_var}_{i}' for i in range(data.shape[1] - 1)]
    data = pd.melt(data, id_vars='date', value_name=value_var)
    data = data.loc[:, ['date', value_var]]
    
    return data


def get_ensemble_knots(n_knots: int, min_interval: float, num_samples: int):
    # if n_knots > 16:
    num_intervals = n_knots - 1
    first_half = int((num_intervals - 1) / 2)
    second_half = (num_intervals - 1) - first_half
    knot_bounds = np.array([[0., 0.5]] * first_half + [[0.5, 1.]] * second_half)
    interval_sizes = np.array([[min_interval, 1]] * num_intervals)
    # else:
    #     num_intervals = n_knots - 1
    #     knot_bounds = np.array([[0, 1]] * (num_intervals - 1))
    #     interval_sizes = np.array([[min_interval, 1]] * num_intervals)
    # #interval_sizes[0] = [1e-4, min_interval]
    # #interval_sizes[-1] = [1e-4, min_interval]
    
    ensemble_knots = sample_knots(num_intervals, knot_bounds=knot_bounds,
                                  interval_sizes=interval_sizes, num_samples=num_samples)
    
    return ensemble_knots

    
def predict_time_series(day0: pd.Timestamp,
                        dep_var: str, 
                        mr_model: MRBRT,
                        dep_trans_out: Callable[[pd.Series], pd.Series],
                        diff: bool,) -> pd.DataFrame:
    data = mr_model.data.to_df()
    
    pred_data = MRData()
    t = np.arange(0, data['t'].max() + 1)
    pred_data.load_df(pd.DataFrame({'t':t}), col_covs='t')
    pred_data_value = mr_model.predict(pred_data)
    if diff:
        pred_data_value = pred_data_value.cumsum()
    pred_data_value = dep_trans_out(pred_data_value)
    pred_data = pd.DataFrame({'t':t,
                              dep_var:pred_data_value,})
    pred_data['date'] = pred_data['t'].apply(lambda x: day0 + pd.Timedelta(days=x))
    pred_data = pred_data.set_index('date')[dep_var]

    return pred_data
        
