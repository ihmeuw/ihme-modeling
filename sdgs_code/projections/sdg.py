import argparse
import os
import sys

import numpy as np
import math
from scipy.special import (logit, expit)
from scipy.stats.stats import pearsonr
import xarray as xr
import pandas as pd

import sqlalchemy as sa

import time

import settings, forecast_methods, data_utils


def calc_rmse(y_draws, years, weight_exp, trans):
    # Take mean of draws, forecast to last year we have data
    y_mean = y_draws.mean(dim='draw')
    forecasts = forecast_methods.arc_quantiles(y_mean, years, weight_exp=weight_exp, trans=trans)
    y_mean = y_mean.loc[{'year_id':np.arange(years[1], years[2]+1)}]
    forecasts = forecasts.loc[{'year_id':np.arange(years[1], years[2]+1)}]
    forecasts = forecasts['reference']

    # Get RMSE using out of sample
    assert len(forecasts.values.flatten()) == len(y_mean.values.flatten()), 'different length arrays for rmse'
    rmse = math.sqrt(((forecasts - y_mean)**2).sum() / len(forecasts.values.flatten()))
    return pd.DataFrame({'weight': weight_exp, 'rmse': rmse}, index=[0])


def main(indicator_type, indicator_type_version, indicator_type_id, years):
    # prep the data
    print("Retrieving and formatting data - " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    if indicator_type == 'uhc':
        indicator_path = os.path.join(settings.io['input_dir'],
                                      indicator_type,
                                      indicator_type_version,
                                      '{}.csv'.format(indicator_type_id))
        indicator_df = pd.read_csv(indicator_path) # For setting demographic coordinates found in data frame
    else:
        indicator_path = os.path.join(settings.io['input_dir'],
                                      indicator_type,
                                      indicator_type_version,
                                      '{}.h5'.format(indicator_type_id))
        indicator_df = pd.read_hdf(indicator_path) # For setting demographic coordinates found in data frame
    d = PastData(measure=indicator_type,
                 other_coords={'indicator_type_id': indicator_type_id,
                               'location_id': indicator_df['location_id'].unique(),
                               'year_id': indicator_df['year_id'].unique(),
                               'age_group_id': indicator_df['age_group_id'].unique(),
                               'sex_id': indicator_df['sex_id'].unique()})
    del indicator_df
    d.load_past_data(indicator_path,
                     draw_prefix='draw_',
                     draw_start=0,
                     chunk=True)
    data_utils.compute_summaries(d.dataset)
    sdg = d.dataset

    # only keep the locations we want
    locs_df = get_location_metadata(35)
    locs_df = locs_df.loc[(locs_df.level >= 3) & \
                         (~locs_df.ihme_loc_id.isin(['ASM', 'BMU', 'GRL', 'GUM', 'MNP', 'PRI', 'VIR']))]
    nats = locs_df.loc[locs_df.location_type == 'admin0', 'location_id'].values
    sdg = sdg.loc[{'location_id': locs_df.location_id.values}]
    assert set(sdg.location_id.values) == set(locs_df.location_id.values), 'Missing locations'

    # create log or logit(sdg)
    mean_sdg = sdg['mean']

    mean_sdg = mean_sdg.loc[{'year_id':np.arange(years[0], years[2])}]

    # save original ages and sexes for final shape
    orig_ages = mean_sdg.age_group_id.values
    orig_sexes = mean_sdg.sex_id.values
    orig_locs = mean_sdg.location_id.values

    # cross-sections that are entirely 0/NaN
    for i in mean_sdg.dims:
        mean_sdg = mean_sdg.dropna(dim=i, how='all')
    mean_sdg.values[mean_sdg.values < 1e-12] = 1e-12

    # format draws, mostly same as mean
    sdg_draws = sdg['value']
    for i in sdg_draws.dims:
        sdg_draws = sdg_draws.dropna(dim=i, how='all')
    sdg_draws.values[sdg_draws.values < 1e-12] = 1e-12

    if (indicator_type in ['sev', 'met_need', 'childhood_overweight', 'covs', 'risk_exposure', 'uhc', 'completeness']) or \
        (indicator_type == 'epi' and indicator_type_id in ['1620116202', '10820', '10475', '10817', '10556', '10558']):
        trans = 'logit'
    else:
        trans = 'log'

    # iterate through weights in sequence to find lowest RMSE
    print("Determining weighting scheme - " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    rmse_dfs = []
    for w in np.arange(0., 2.2, 0.2):
        rmse_df = calc_rmse(sdg_draws.loc[{'location_id': nats}], [years[0], years[1], years[2]-1], w, trans)
        rmse_dfs.append(rmse_df)
    rmse_df = pd.concat(rmse_dfs)

    rmse = min(rmse_df['rmse'])
    weight_exp = rmse_df[rmse_df['rmse'] == rmse]['weight'].values
    assert len(weight_exp) <= 2, "only one weight can be chosen (will take min if two are tied)"
    if len(weight_exp) == 2:
        weight_exp = min(weight_exp)
    else:
        weight_exp = float(weight_exp)
    print("Weight determined ({})".format(str(weight_exp)))

    # weighted quaniltes model
    print("Running forecast - " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    forecasts = forecast_methods.arc_quantiles(y_past=sdg_draws, years=[years[0], years[2], years[3]], trans=trans,
                                               weight_exp=weight_exp, adjust=True, adjust_locs=nats)

    # keep forecast only and convert to DataFrame (as to be compatible with the past data)
    print("Converting to data frame for storage - " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    forecasts = forecasts.to_dataframe()
    forecasts = forecasts.reset_index()
    forecasts['draw'] = 'draw_' + forecasts['draw'].astype(str)
    forecasts = forecasts.pivot_table(index=['location_id','year_id','age_group_id','sex_id','high','low'],
                                      columns='draw',
                                      values='reference')
    forecasts = forecasts.reset_index()

    # save results
    out_dir = os.path.join(settings.io['forecast_dir'], indicator_type, indicator_type_version)
    try:
        os.makedirs(out_dir)
    except OSError:
        pass

    print("Saving - " + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    rmse_df['used'] = weight_exp
    rmse_df.to_csv(os.path.join(out_dir, '{}_RMSE.csv'.format(indicator_type_id)))

    forecasts.to_hdf(os.path.join(out_dir, '{}.h5'.format(indicator_type_id)),
                     format='table', key='data',
                     data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])


if __name__ == '__main__':
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('-type', '--indicator_type', help='Abridged indicator group name',
                        type=str)
    parser.add_argument('-version', '--indicator_type_version', help='Indicator type version',
                        type=str)
    parser.add_argument('-id', '--indicator_type_id', help='Indicator name as an arbitrary code',
                        type=str)
    parser.add_argument('--years', type=str,
                        help='four integers in the format "a:b:c:d",\
                              where a is the first year of the past\
                              b is the first year of knockouts for testing RMSE of different weights\
                              c is the first year of the forecast\
                              and d is the last year of the forecast')

    args = parser.parse_args()

    years = [int(x) for x in args.years.split(':')]
    assert len(years) == 4, "years argument must have 4 elements, e.g. 1990:2008:2017:2030"

    main(args.indicator_type, args.indicator_type_version, args.indicator_type_id, years)
