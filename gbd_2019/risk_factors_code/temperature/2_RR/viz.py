# -*- coding: utf-8 -*-
"""
    viz
    ~~~

    Functions for visiualization of data.
"""
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import utils
import process


def plot_surface(agg_tdata, surface_result, num_levels=10):
    """plot the 3D surface and the level set"""

    # calculate the 3D surface
    num_mt = 101
    num_dt = 101
    mean_temp = np.linspace(agg_tdata.mean_temp.min(),
                            agg_tdata.mean_temp.max(), num_mt)
    daily_temp = np.linspace(agg_tdata.daily_temp.min(),
                             agg_tdata.daily_temp.max(), num_dt)
    MT, DT = np.meshgrid(mean_temp, daily_temp)
    mt = MT.flatten()
    dt = DT.flatten()

    # compute residual surface
    scaled_dt = utils.scale_daily_temp(mt, dt, surface_result.scale_params)
    invalid_id = (scaled_dt <= -1.0) | (scaled_dt > 0.75)

    surface = surface_result.surface_func(mt, dt)
    surface[invalid_id] = np.inf
    surface = surface.reshape(num_mt, num_dt)

    # plot 3D surface
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot(121, projection='3d')
    ax.scatter(
            agg_tdata.mean_temp,
            agg_tdata.daily_temp,
            agg_tdata.obs_mean,
            s=1.0/agg_tdata.obs_std,
            c=agg_tdata.obs_mean)

    ax.plot_surface(MT, DT, surface)
    ax.set_xlabel('mean temp degree')
    ax.set_ylabel('daily temp cat')
    ax.set_zlabel('ln rr')

    # plot the level set
    ax = fig.add_subplot(122)
    ax.scatter(
            agg_tdata.mean_temp,
            agg_tdata.daily_temp,
            s=1.0/agg_tdata.obs_std,
            c=agg_tdata.obs_mean)
    ax.contour(MT, DT, surface,
               levels=np.linspace(agg_tdata.obs_mean.min(),
                                  agg_tdata.obs_mean.max(), num_levels))
    ax.set_xlabel('mean temp degree')
    ax.set_ylabel('daily temp cat')

    min_daily_temp_id = np.array([np.argmin(surface[:, i])
                                  for i in range(num_mt)])
    min_daily_temp = daily_temp[min_daily_temp_id]
    ax.scatter(mean_temp, min_daily_temp, c='r', marker='.')


def plot_trend_slice(mean_temp, tdata, trend_result,
                     study_range=None,
                     ylim=None,
                     ax=None):
    """plot trend at given mean_temp"""
    if ax is None:
        handle = plt
        if ylim is not None: handle.ylim(*ylim)
    else:
        handle = ax
        if ylim is not None: handle.set_ylim(*ylim)
    # plot data
    tdata_amt = plot_data_slice(mean_temp, tdata, ylim=ylim, ax=ax,
                                study_range=study_range)
    min_daily_temp = tdata_amt.daily_temp.min()
    max_daily_temp = tdata_amt.daily_temp.max()
    if study_range is None:
        study_range = range(0, tdata_amt.num_studies)
    else:
        study_range = range(study_range[0], study_range[1])

    # plot the trend line
    mean_temp_id = np.where(trend_result.mean_temp == mean_temp)[0][0]
    beta = trend_result.beta[mean_temp_id]
    random_effects = trend_result.random_effects[mean_temp_id]

    cov = np.array([min_daily_temp, max_daily_temp])
    M = cov.reshape(cov.size, 1) - mean_temp

    for i in study_range:
        handle.plot(cov, M.dot(beta + random_effects[i]))


def plot_data_slice(mean_temp, tdata, ylim=None, ax=None,
                    study_range=None):
    """scatter the data at given mean_temp"""
    if ax is None:
        handle = plt
        if ylim is not None: handle.ylim(*ylim)
    else:
        handle = ax
        if ylim is not None: handle.set_ylim(*ylim)

    tdata_amt = process.extract_at_mean_temp(tdata, mean_temp)
    study_slices = utils.sizes_to_slices(tdata_amt.study_sizes)

    if study_range is None:
        study_range = range(0, tdata_amt.num_studies)
    else:
        study_range = range(study_range[0], study_range[1])

    # plot all the points
    for i in study_range:
        s = study_slices[i]
        handle.scatter(tdata_amt.daily_temp[s],
                       tdata_amt.obs_mean[s],
                       s=1.0/tdata_amt.obs_std[s])

    # plot the trimmed data as red x
    trimming_id = tdata_amt.trimming_weights <= 0.5
    handle.scatter(tdata_amt.daily_temp[trimming_id],
                   tdata_amt.obs_mean[trimming_id],
                   marker='x',
                   color='r')

    return tdata_amt


def plot_slice_uncertainty(mean_temp, tdata, surface_result, trend_result,
                           ylim=None, ax=None, num_samples=100,
                           include_re=True):
    if ax is None:
        handle = plt
        if ylim is not None: handle.ylim(*ylim)
    else:
        handle = ax
        if ylim is not None: handle.set_ylim(*ylim)

    # plot data
    tdata_amt = plot_data_slice(mean_temp, tdata, ylim=ylim, ax=ax)

    # plot the curve
    min_daily_temp = tdata_amt.daily_temp.min()
    max_daily_temp = tdata_amt.daily_temp.max()
    num_dt = 100
    dt = np.linspace(min_daily_temp, max_daily_temp, num_dt)
    curve = surface_result.surface_func(np.repeat(mean_temp, num_dt), dt)

    handle.plot(dt, curve)

    # predict data
    surface_result.sample_fixed_effects(num_samples)
    trend_result.sample_random_effects(num_samples)

    curve_samples = process.sample_surface(np.repeat(mean_temp, dt.size), dt,
                                           num_samples,
                                           surface_result,
                                           trend_result)

    # curve_samples = np.vstack(curve_samples)
    curve_samples_max = curve_samples.max(axis=0)
    curve_samples_min = curve_samples.min(axis=0)

    handle.fill_between(dt, curve_samples_min, curve_samples_max,
                        color='#808080', alpha=0.7)
    handle.plot([dt.min(), dt.max()], [0.0, 0.0], "k--")
