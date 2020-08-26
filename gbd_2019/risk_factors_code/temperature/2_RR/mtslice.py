# -*- coding: utf-8 -*-
"""
    mtslice
    ~~~~~~~

    mtslice module contains all the actions w.r.t. the mean temperature slices.
"""
import numpy as np
import matplotlib.pyplot as plt
import xspline
import limetr
import utils
import copy


# utils
# -----------------------------------------------------------------------------
def extract_mtslice(tdata, mt):
    """extract the temperature data at given mean_temp"""
    valid_id = tdata.mean_temp == mt

    mean_temp = tdata.mean_temp[valid_id]
    daily_temp = tdata.daily_temp[valid_id]
    obs_mean = tdata.obs_mean[valid_id]
    obs_std = tdata.obs_std[valid_id]
    study_id = tdata.study_id[valid_id]
    if tdata.data_id is not None:
        data_id = tdata.data_id[valid_id]
    else:
        data_id = None
    trimming_weights = tdata.trimming_weights[valid_id]

    return utils.TempData(mean_temp,
                          daily_temp,
                          obs_mean,
                          obs_std,
                          study_id,
                          data_id,
                          trimming_weights=trimming_weights)


def aggregate_mtslice(tdata):
    """aggregate data by the weighted mean"""
    # extract the unique mean and daily pair
    unique_pair = np.unique(np.vstack((tdata.mean_temp,
                                       tdata.daily_temp)).T, axis=0)
    mean_temp = unique_pair[:, 0]
    daily_temp = unique_pair[:, 1]

    obs_mean = []
    obs_std = []

    for p in unique_pair:
        valid_id = (tdata.mean_temp == p[0]) &\
            (tdata.daily_temp == p[1]) &\
            (tdata.trimming_weights > 0.5)
        obs_mean_atp = tdata.obs_mean[valid_id]
        obs_std_atp = tdata.obs_std[valid_id]

        ivar = 1.0/obs_std_atp**2
        obs_mean_atp = obs_mean_atp.dot(ivar)/np.sum(ivar)
        obs_std_atp = np.sqrt(1.0/np.sum(ivar))

        obs_mean.append(obs_mean_atp)
        obs_std.append(obs_std_atp)

    obs_mean = np.array(obs_mean)
    obs_std = np.array(obs_std)

    study_id = np.arange(obs_mean.size)
    data_id = None

    return utils.TempData(mean_temp,
                          daily_temp,
                          obs_mean,
                          obs_std,
                          study_id,
                          data_id)


def stack_mtslice(tdata_list):
        """stack tdata in the list into one instance"""
        mean_temp_list = [tdata.mean_temp for tdata in tdata_list]
        daily_temp_list = [tdata.daily_temp for tdata in tdata_list]
        obs_mean_list = [tdata.obs_mean for tdata in tdata_list]
        obs_std_list = [tdata.obs_std for tdata in tdata_list]
        study_id_list = [tdata.study_id for tdata in tdata_list]
        data_id_list = [tdata.data_id for tdata in tdata_list]
        trimming_weights_list = [tdata.trimming_weights
                                 for tdata in tdata_list]

        mean_temp = np.hstack(mean_temp_list)
        daily_temp = np.hstack(daily_temp_list)
        obs_mean = np.hstack(obs_mean_list)
        obs_std = np.hstack(obs_std_list)
        study_id = np.hstack(study_id_list)
        data_id = np.hstack(data_id_list)
        trimming_weights = np.hstack(trimming_weights_list)

        if np.any(data_id == None):
            data_id = None

        return utils.TempData(mean_temp,
                              daily_temp,
                              obs_mean,
                              obs_std,
                              study_id,
                              data_id,
                              trimming_weights=trimming_weights)


# fit
# -----------------------------------------------------------------------------
def adjust_mean(tdata, ref=None):
    """Adjust the mean of the data by the given ref
    """
    if ref is None:
        ref = tdata.unique_mean_temp

    tdata_list = []
    for i, mt in enumerate(tdata.unique_mean_temp):
        tdata_mt = extract_mtslice(tdata, mt)
        tdata_mt = adjust_mean_mtslice(tdata_mt, ref=ref[i])
        tdata_list.append(tdata_mt)

    return stack_mtslice(tdata_list)


def adjust_mean_mtslice(tdata_mt, ref=None):
    """Adjust the mean of the mtslice by the given ref
    """
    if ref is None:
        ref = tdata_mt.mean_temp[0]

    study_slices = utils.sizes_to_slices(tdata_mt.study_sizes)
    for i in range(tdata_mt.num_studies):
        obs_mean = tdata_mt.obs_mean[study_slices[i]]
        obs_std = tdata_mt.obs_std[study_slices[i]]
        cov = tdata_mt.daily_temp[study_slices[i]]

        # fit the curve
        if tdata_mt.study_sizes[i] >= 5:
            spline = xspline.xspline(
                np.array([cov.min(), ref, cov.max()]),
                2, l_linear=True)
        else:
            spline = xspline.xspline(
                np.array([cov.min(), cov.max()]), 1)

        beta = utils.fit_spline(obs_mean, obs_std, cov, spline)
        ref_lnrr = spline.designMat(ref).dot(beta)

        # adjust the mean
        tdata_mt.obs_mean[study_slices[i]] -= ref_lnrr

    return tdata_mt


def adjust_agg_std(tdata, ref=None):
    """Adjust std of the aggregate the tdata
    """
    if ref is None:
        ref = tdata.unique_mean_temp

    tdata_list = []
    for i, mt in enumerate(tdata.unique_mean_temp):
        tdata_mt = extract_mtslice(tdata, mt)
        tdata_mt = adjust_agg_std_mtslice(tdata_mt, ref=ref[i])
        tdata_list.append(tdata_mt)

    return stack_mtslice(tdata_list)


def adjust_agg_std_mtslice(tdata_mt, ref=None):
    """Adjust std of the aggregate the tdata slices
    """
    if ref is None:
            ref = tdata_mt.mean_temp[0]

    # fit the curve
    spline = xspline.xspline(
            np.array([tdata_mt.daily_temp.min(), ref,
                      tdata_mt.daily_temp.max()]), 2, l_linear=True)

    beta = utils.fit_spline(tdata_mt.obs_mean,
                            tdata_mt.obs_std,
                            tdata_mt.daily_temp,
                            spline)

    residual = (tdata_mt.obs_mean -
                spline.designMat(tdata_mt.daily_temp).dot(beta))
    residual /= tdata_mt.obs_std
    # print(np.maximum(1.0, np.std(residual)))
    tdata_mt.obs_std *= np.maximum(3.0, np.std(residual))

    return tdata_mt


def fit_trend(tdata, surface_result, inlier_pct=1.0):
    # calculate the residual data
    tdata_residual = copy.deepcopy(tdata)
    tdata_residual.obs_mean -= surface_result.surface_func(
            tdata.mean_temp,
            tdata.daily_temp)

    beta = []
    beta_var = []
    gamma = []
    random_effects = []

    for i, mean_temp in enumerate(tdata.unique_mean_temp):
        tdata_at_mean_temp = extract_mtslice(tdata_residual, mean_temp)
        tmrl = surface_result.tmrl[i]
        # print(mean_temp, tmrl)
        (beta_at_mt,
         beta_var_at_mt,
         gamma_at_mt,
         random_effects_at_mt) = fit_trend_mtslice(tdata_at_mean_temp,
                                                   tmrl,
                                                   inlier_pct=inlier_pct)
        beta.append(beta_at_mt)
        beta_var.append(beta_var_at_mt)
        gamma.append(gamma_at_mt)
        random_effects.append(random_effects_at_mt)

    beta = np.vstack(beta)
    beta_var = np.vstack(beta_var)
    gamma = np.vstack(gamma)

    trend_result = utils.TrendResult(beta, beta_var, gamma, random_effects,
                                     tdata.unique_mean_temp)

    return trend_result, tdata_residual


def fit_trend_mtslice(tdata_at_mean_temp, tmrl, inlier_pct=0.9, debug=False):
    """
        Return beta (intercept and slope) and gamma (intercept and slope)
        with given data
    """
    if debug:
        print("number of locations at mean temp",
              tdata_at_mean_temp.num_studies)
        outer_verbose = True
        inner_print_level = 5
    else:
        outer_verbose = False
        inner_print_level = 0

    # construct the linear mixed effect model
    cov = tdata_at_mean_temp.daily_temp
    knots = np.array([cov.min(), tmrl, cov.max()])
    degree = 1
    spline = xspline.xspline(knots, degree)

    l1 = knots[1] - knots[0]
    l2 = knots[2] - knots[1]
    mat_transform = np.array([[1.0, 0.0, 0.0],
                              [1.0, l1,  0.0],
                              [1.0, l1,  l2 ]])
    M = spline.designMat(cov).dot(mat_transform)
    M[:, 1] -= M[:, 0]*l1
    M = M[:, 1:]
    scale = np.linalg.norm(M, axis=0)
    scaled_M = M/scale

    # construct the LimeTr object
    F = lambda beta: scaled_M.dot(beta)
    JF = lambda beta: scaled_M
    Z = scaled_M.copy()

    n = tdata_at_mean_temp.study_sizes
    k_beta = 2
    k_gamma = 2

    Y = tdata_at_mean_temp.obs_mean
    S = tdata_at_mean_temp.obs_std

    uprior = np.array([
            [-np.inf]*k_beta + [1e-7]*k_gamma,
            [np.inf]*k_beta + [1.5]*k_gamma
        ])

    lt = limetr.LimeTr(n, k_beta, k_gamma, Y, F, JF, Z, S=S,
                       uprior=uprior,
                       inlier_percentage=inlier_pct)

    # fit model
    MS = M/S.reshape(S.size, 1)
    YS = Y/S
    beta0 = np.linalg.solve(MS.T.dot(MS), MS.T.dot(YS))
    gamma0 = np.array([0.1, 0.1])
    (beta,
     gamma,
     trimming_weights) = lt.fitModel(x0=np.hstack((beta0, gamma0)),
                                     outer_step_size=200.0,
                                     outer_verbose=outer_verbose,
                                     inner_print_level=inner_print_level)

    # estimate the random effects
    random_effects = lt.estimateRE()

    # estimate the uncertainty of beta
    V = limetr.utils.VarMat(lt.S**2, lt.Z, gamma, lt.n)
    beta_var = np.linalg.inv(M.T.dot(V.invDot(M)))

    # # scale beta and gamma back
    beta /= scale
    beta_var /= scale**2
    gamma /= scale**2
    random_effects /= scale

    return beta, beta_var, gamma, random_effects


# viz
# -----------------------------------------------------------------------------
def plot_mtslice(tdata, mt, ylim=None, ax=None, use_colors=True):
    if ax is None:
        ax = plt
        if ylim is not None:
            ax.ylim(ylim[0], ylim[1])
    else:
        if ylim is not None:
            ax.set_ylim(ylim[0], ylim[1])

    tdata_mt = extract_mtslice(tdata, mt)

    if use_colors:
        study_slices = utils.sizes_to_slices(tdata_mt.study_sizes)
        for i in range(tdata_mt.num_studies):
            ax.scatter(tdata_mt.daily_temp[study_slices[i]],
                       tdata_mt.obs_mean[study_slices[i]],
                       s=1.0/tdata_mt.obs_std[study_slices[i]])
    else:
        ax.scatter(tdata_mt.daily_temp, tdata_mt.obs_mean,
                   s=1.0/tdata_mt.obs_std)


