# -*- coding: utf-8 -*-
"""
    surface
    ~~~~~~~

    surface model contains all the action w.r.t. the surface.
"""
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import xspline
import limetr
import mrbrt
import utils
import mtslice


# fit
# ------------------------------------------------------------------------------
def fit_surface(tdata_agg,
                scale_params=[40.0, 1.25],
                linear_no_mono=False):
    """fit the mean surface"""

    # unpack data
    mt = tdata_agg.mean_temp
    dt = tdata_agg.daily_temp
    scaled_dt = utils.scale_daily_temp(mt, dt, scale_params)

    obs_mean = tdata_agg.obs_mean
    obs_std = tdata_agg.obs_std
    study_sizes = tdata_agg.study_sizes

    # create spline
    mt_knots = np.linspace(mt.min(), mt.max(), 2)
    mt_degree = 3
    if linear_no_mono:
        dt_degree = 1
        dt_knots = np.linspace(scaled_dt.min(), scaled_dt.max(), 2)
    else:
        dt_degree = 3
        dt_knots = np.linspace(scaled_dt.min(), scaled_dt.max(), 3)
    spline_list = [xspline.ndxspline(2,
                                     [mt_knots, dt_knots],
                                     [mt_degree, dt_degree])]

    # create mrbrt object
    x_cov_list = [{
        'cov_type': 'ndspline',
        'spline_id': 0,
        'mat': np.vstack((mt, scaled_dt))
    }]
    z_cov_list = [{
        'cov_type': 'linear',
        'mat': np.ones(mt.size)
    }]

    mr = mrbrt.MR_BRT(obs_mean,
                      obs_std,
                      study_sizes,
                      x_cov_list,
                      z_cov_list,
                      spline_list)

    # add priors
    prior_list = [
        {
            'prior_type': 'ndspline_shape_function_uprior',
            'x_cov_id': 0,
            'interval': [[mt.min(), mt.max()],
                         [scaled_dt.min(), scaled_dt.max()]],
            'indicator': [-1.0, 1.0],
            'num_points': [20, 20]
        },
        {
            'prior_type': 'ndspline_shape_monotonicity',
            'x_cov_id': 0,
            'dim_id': 1,
            'interval': [[mt.min(), mt.max()],
                         [0.7, scaled_dt.max()]],
            'indicator': 'increasing',
            'num_points': [20, 10]
        },
        {
            'prior_type': 'z_cov_uprior',
            'z_cov_id': 0,
            'prior': np.array([[0.0]*mr.k_gamma, [0.0]*mr.k_gamma])
        }
    ]
    if not linear_no_mono:
        prior_list += [
            {
                'prior_type': 'ndspline_shape_monotonicity',
                'x_cov_id': 0,
                'dim_id': 1,
                'interval': [[mt.min(), mt.max()],
                             [scaled_dt.min(), -0.75]],
                'indicator': 'decreasing',
                'num_points': [20, 10]
            },
        ]

    mr.addPriors(prior_list)

    # initialization
    k_beta = mr.lt.k_beta
    k_gamma = mr.lt.k_gamma
    X = mr.lt.JF(mr.lt.beta)
    S = mr.lt.S
    V = S**2
    Y = mr.lt.Y
    beta0 = np.linalg.solve((X.T/V).dot(X), (X.T/V).dot(Y))
    gamma0 = np.zeros(k_gamma)
    x0 = np.hstack((beta0, gamma0))

    # fit the model and store the result
    mr.fitModel(x0=x0)

    # compute posterior variance for the beta
    # extract the matrix
    beta_var = (X.T/V).dot(X)

    if mr.lt.use_regularizer:
        H = np.vstack([mr.lt.H(np.hstack(np.eye(1, k_beta, i).reshape(k_beta,),
                               np.zeros(k_gamma)))
                       for i in range(k_beta)]).T
        SH = mr.lt.h[1]
        VH = SH**2
        beta_var += (H.T/VH).dot(VH)

    beta_var = np.linalg.inv(beta_var)

    mean_temp = tdata_agg.unique_mean_temp
    daily_temp_range = []
    for mt in mean_temp:
        tdata_agg_mt = mtslice.extract_mtslice(tdata_agg, mt)
        daily_temp_range.append([tdata_agg_mt.daily_temp.min(),
                                 tdata_agg_mt.daily_temp.max()])

    surface_result = utils.SurfaceResult(
            mr.beta_soln,
            beta_var,
            spline_list[0],
            mean_temp,
            daily_temp_range,
            scale_params=scale_params)

    return surface_result

# viz
# ------------------------------------------------------------------------------
def plot_surface(tdata_agg, surface_result, num_levels=11):
    """plot the 3D surface and the level set"""

    # calculate the 3D surface
    num_mt = 101
    num_dt = 101
    mean_temp = np.linspace(tdata_agg.mean_temp.min(),
                            tdata_agg.mean_temp.max(), num_mt)
    daily_temp = np.linspace(tdata_agg.daily_temp.min(),
                             tdata_agg.daily_temp.max(), num_dt)
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
            tdata_agg.mean_temp,
            tdata_agg.daily_temp,
            tdata_agg.obs_mean,
            s=1.0/tdata_agg.obs_std,
            c=tdata_agg.obs_mean)

    ax.plot_surface(MT, DT, surface)
    ax.set_xlabel('mean temp degree')
    ax.set_ylabel('daily temp cat')
    ax.set_zlabel('ln rr')

    # plot the level set
    ax = fig.add_subplot(122)
    ax.scatter(
            tdata_agg.mean_temp,
            tdata_agg.daily_temp,
            s=1.0/tdata_agg.obs_std,
            c=tdata_agg.obs_mean)
    c = ax.contour(MT, DT, surface,
                   levels=np.linspace(-0.5, 0.5, num_levels))
    ax.set_xlabel('mean temp degree')
    ax.set_ylabel('daily temp cat')
    ax.clabel(c)

    # min_daily_temp_id = np.array([np.argmin(surface[:, i])
    #                               for i in range(num_mt)])
    # min_daily_temp = daily_temp[min_daily_temp_id]
    min_daily_temp = np.array([
        surface_result.tmrl_at_mean_temp(mean_temp[i])
        for i in range(num_mt)
    ])
    ax.scatter(mean_temp, min_daily_temp, c='r', marker='.')

