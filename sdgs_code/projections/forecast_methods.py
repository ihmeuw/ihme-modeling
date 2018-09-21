import itertools as it
import logging
import os
import sys

import numpy as np
from scipy.special import expit, logit
from scipy.stats.stats import pearsonr
import xarray as xr
import pandas as pd

import settings

def random_walk(years, val_array, mu_array, sigma_array, trans):
    # Expand sigma if we do not have draws
    if 'draw' not in sigma_array.dims:
        expand = xr.DataArray(np.ones(len(mu_array['draw'])),
                              dims='draw',
                              coords={'draw':mu_array['draw']})
        sigma_array = sigma_array * expand

    # ARC random walk
    np.random.seed(seed=15243)
    arcs = np.random.normal(loc=mu_array.values,
                            scale=sigma_array.values,
                            size=tuple([years[2] - (years[1] - 1)]) + np.shape(mu_array))
    arc_coords = {'year_id': np.arange(years[1], years[2] + 1)}
    for coord in list(mu_array.coords):
        arc_coords.update({coord: mu_array[coord].values})
    arc_array = xr.DataArray(arcs,
                             dims=['year_id'] + list(mu_array.dims),
                             coords=arc_coords)

    # start with last year of past, simulate forecasted time series
    for year in arc_array['year_id'].values:
        past_year = val_array.loc[{'year_id': year-1}]
        past_year = past_year.drop('year_id')
        arc = arc_array.loc[{'year_id': year}]
        next_year = past_year + arc
        val_array = xr.concat([val_array, next_year], 'year_id')

    return val_array.loc[{'year_id': np.arange(years[1], years[2] + 1)}]


def scenario_past_arc(y_past_mean, las_arc, final, final_mean, years):
    '''Use percentiles of past change to produce scenarios'''
    # get percentiles of past ARCs
    scenario_arc = las_arc.quantile(q=[0.15, 0.85], dim=['location_id'])

    # build out projections
    year_change = xr.DataArray(np.arange(years[2] - years[1] + 1) + 1,
                                dims=['year_id'],
                                coords={'year_id': np.arange(years[1], years[2] + 1)})
    scenario_arc = scenario_arc * year_change

    # apply change
    scenarios = y_past_mean.loc[{'year_id': years[1] - 1}] + scenario_arc

    # swap in reference for scenario if is outside limits
    del final_mean['quantile']
    # high first...
    high = scenarios.loc[{'quantile': 0.15}]
    del high['quantile']
    high_diff = high - final_mean
    high_diff = high_diff.max(dim='year_id')
    high_reps = high_diff.where(high_diff >= 0, drop=True).location_id.values
    high_keeps = high_diff.where(high_diff < 0, drop=True).location_id.values
    high = xr.concat([high.loc[{'location_id': high_keeps}], final_mean.loc[{'location_id': high_reps}]], 'location_id')
    # ...then low
    low = scenarios.loc[{'quantile': 0.85}]
    del low['quantile']
    low_diff = low - final_mean
    low_diff = low_diff.max(dim='year_id')
    low_reps = low_diff.where(low_diff <= 0, drop=True).location_id.values
    low_keeps = low_diff.where(low_diff > 0, drop=True).location_id.values
    low = xr.concat([low.loc[{'location_id': low_keeps}], final_mean.loc[{'location_id': low_reps}]], 'location_id')

    # Put scenarios in dataset with reference
    final_scenarios = xr.Dataset({'low': low, 'high': high}, coords=high.coords)
    final = final.to_dataset(name='reference')
    final_w_scenarios = xr.merge([final, final_scenarios])

    return final_w_scenarios


def quiet_weighted_quantile(da, dim, dim_weight, q=0.5):
    """
    Using Pandas (for sorting).

    Args:
        da (dataarray): a dataarray
        dim (str or list of str): dim(s) to reduce over
        dim_weight (float or list of float): dim(s) to reduce over
        q (float or list of float): quantile(s) to evaluate

     Returns:
        dataarray
    """
    # Get stratification variables
    id_vars = list(da.dims)
    id_vars.remove('year_id')

    # Go from array -> dataset -> dataframe
    ds = xr.Dataset({'val': (da.dims, da.values),
                     'weights': (dim, dim_weight)},
                     coords=da.coords)
    df = ds.to_dataframe()
    df = df.reset_index()

    # re-index get weighted distance from quantile of interest
    df = df.sort_values(id_vars + ['val'])
    df['n'] = df.groupby(id_vars)['weights'].cumsum()
    df['N'] = df.groupby(id_vars)['weights'].transform(np.sum)
    df['pctile_diff'] = (df['n'] / df['N']) - q

    # keep the closest above
    df = df.query('pctile_diff > 0')
    df = df.loc[df.groupby(id_vars)['pctile_diff'].idxmin]

    # go back to dataset
    df = df[id_vars + ['val']].set_index(id_vars)
    wq_ds = df.to_xarray()

    # make sure we still have the coords from the original array
    orig_da = da.to_dataset()
    full_ds = xr.merge([orig_da, wq_ds])

    return full_ds['val']


def arc_quantiles(y_past, years, weight_exp, trans, adjust=False, adjust_locs=None):
    """Quantile ARC documentation forthcoming...

    Args:
        y_mean (xarray.DataArray): array with location/age/sex/year dimensions
        years (tuple of year_id): years to include in the past when calculating
            ARC (dUSERts to 1990-2015)
        weight_exp (float): power to raise the increasing year weights to
            weight = (np.arange(years[1] - years[0]) + 1)**weight_exp
        adjust (bool): do we adjust scenarios?
        adjust type (str): one of ['loess'], the type of adjustment to perform
    """
    final_data_year = years[1] - 1
    y_past = y_past.loc[{'year_id':np.arange(years[0], years[1])}]

    forecast_year = years[2]

    if 'draw' in y_past.dims:
        y_past_mean = y_past.mean(dim='draw')
    else:
        y_past_mean = xr.DataArray(y_past)

    # transform
    if trans == 'logit':
        y_past.values[y_past.values > 1 - 1e-12] = 1 - 1e-12
        y_past.values = logit(y_past.values)
        y_past_mean.values[y_past_mean.values > 1 - 1e-12] = 1 - 1e-12
        y_past_mean.values = logit(y_past_mean.values)
    elif trans == 'log':
        y_past.values = np.log(y_past.values)
        y_past_mean.values = np.log(y_past_mean.values)

    # find first differences
    first_diff = y_past.diff('year_id', n=1)
    first_diff_mean = y_past_mean.diff('year_id', n=1)

    year_weights = (np.arange(final_data_year-years[0])+1)**weight_exp

    if weight_exp > 0.:
        if 'draw' in y_past.dims:
            las_mu = quiet_weighted_quantile(da=first_diff, dim=['year_id'], dim_weight=year_weights, q=0.5)
        las_mu_mean = quiet_weighted_quantile(da=first_diff_mean, dim=['year_id'], dim_weight=year_weights, q=0.5)
    else:
        if 'draw' in y_past.dims:
            las_mu = first_diff.quantile(q=0.5, dim=['year_id'])
            del las_mu['quantile']
        las_mu_mean = first_diff_mean.quantile(q=0.5, dim=['year_id'])
        del las_mu_mean['quantile']

    # find future change
    forecast_years = xr.DataArray(np.arange(forecast_year-final_data_year)+1,
        dims=['year_id'],
        coords={'year_id': np.arange(final_data_year+1, forecast_year+1)})
    future_change_mean = forecast_years * las_mu_mean

    # make series from mean
    final_mean = y_past_mean.loc[{'year_id': final_data_year}] + future_change_mean

    # do random walk for error estimation if this is draw level, then scale to mean
    if 'draw' in y_past.dims:
        las_sigma = xr.ufuncs.sqrt(((las_mu - las_mu.mean(dim=['draw']))**2).sum(dim=['draw']) / len(las_mu['draw']))
        las_sigma.values[las_sigma.values < 1e-12] = las_sigma.values[las_sigma.values < 1e-12] + 1e-12 # set incredibly low floor for st dev
        final = random_walk(years, y_past, las_mu, las_sigma, trans)
        # scale in normal space
        if trans == 'log':
            final = np.exp(final)
            final_mean = np.exp(final_mean)
        elif trans == 'logit':
            final = expit(final)
            final_mean = expit(final_mean)
        final = final * (final_mean / final.mean(dim=['draw']))
        if trans == 'log':
            final = np.log(final)
            final_mean = np.log(final_mean)
        elif trans == 'logit':
            final.values[final.values > 1 - 1e-12] = 1 - 1e-12
            final = logit(final)
            final_mean.values[final_mean.values > 1 - 1e-12] = 1 - 1e-12
            final_mean = logit(final_mean)
    else:
        final = xr.DataArray(final_mean)

    # get scenarios
    if adjust:
        final = scenario_past_arc(y_past_mean,
                                  las_mu_mean.loc[{'location_id':adjust_locs}],
                                  final,
                                  final_mean,
                                  years)
    else:
        final = xr.Dataset({'reference':final}, coords=final.coords)

    # transform back
    if trans == 'logit':
        final = final.apply(expit)
    elif trans == 'log':
        final = final.apply(np.exp)

    return final
