# -*- coding: utf-8 -*-
"""
    process
    ~~~~~~~

    Functions for processing the data.
"""
import numpy as np
import pandas as pd
import copy
import xspline
import limetr
import mrbrt
import utils


def offsite_data(tdata):
    unique_mean_temp = np.sort(np.unique(tdata.mean_temp))
    processed_data = []
    for mean_temp in unique_mean_temp:
        processed_data.append(offsite_data_at_mean_temp(tdata, mean_temp))

    return stack_data(processed_data)


def offsite_data_at_mean_temp(tdata, mean_temp):
    tdata_at_mean_temp = extract_at_mean_temp(tdata, mean_temp)
    study_slices = utils.sizes_to_slices(tdata_at_mean_temp.study_sizes)
    for i in range(tdata_at_mean_temp.num_studies):
        obs_mean = tdata_at_mean_temp.obs_mean[study_slices[i]]
        obs_std = tdata_at_mean_temp.obs_std[study_slices[i]]
        cov = tdata_at_mean_temp.daily_temp[study_slices[i]]

        # fit the curve
        spline = xspline.xspline(
            np.array([cov.min(), mean_temp, cov.max()]), 2, l_linear=True)
        beta = utils.fit_spline(obs_mean, obs_std, cov, spline)
        ref_lnrr = spline.designMat(mean_temp).dot(beta)

        # shift the data
        tdata_at_mean_temp.obs_mean[study_slices[i]] -= ref_lnrr

        # inflate the std if necessary
        residual = (obs_mean - spline.designMat(cov).dot(beta))/obs_std
        tdata_at_mean_temp.obs_std[study_slices[i]] *= np.maximum(
                1.0, np.std(residual))
    
    return tdata_at_mean_temp

def load_data(path_to_data, outcome):
    """load data csv file"""
    df = pd.read_csv(path_to_data)

    mean_temp = df['meanTempDegree'].values
    daily_temp = df['dailyTempCat'].values
    obs_mean = df['lnRr_' + outcome].values
    obs_std = df['se_' + outcome].values
    study_id = df['adm1'].values
    data_id = np.arange(df.shape[0])

    valid_id = ~(np.isnan(obs_std) |
                 np.isnan(obs_mean) |
                 np.isinf(obs_std) |
                 np.isinf(obs_mean))

    mean_temp = mean_temp[valid_id]
    daily_temp = daily_temp[valid_id]
    obs_mean = obs_mean[valid_id]
    obs_std = obs_std[valid_id]
    study_id = study_id[valid_id]
    data_id = data_id[valid_id]

    return utils.TempData(mean_temp,
                          daily_temp,
                          obs_mean,
                          obs_std,
                          study_id,
                          data_id)


def aggregate_data(tdata):
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
            # obs_std_atp = np.mean(obs_std_atp)

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


def stack_data(tdata_list):
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

        return utils.TempData(mean_temp,
                              daily_temp,
                              obs_mean,
                              obs_std,
                              study_id,
                              data_id,
                              trimming_weights=trimming_weights)


def extract_at_mean_temp(tdata, mean_temp):
    """extract the temperature data at given mean_temp"""
    valid_id = tdata.mean_temp == mean_temp

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


def fit_surface(tdata,
                scale_params=[40.0, 1.25]):
    """fit the mean surface"""
    agg_tdata = aggregate_data(tdata)

    # unpack data
    mt = agg_tdata.mean_temp
    dt = agg_tdata.daily_temp
    scaled_dt = utils.scale_daily_temp(mt, dt, scale_params)

    obs_mean = agg_tdata.obs_mean
    obs_std = agg_tdata.obs_std*15.0
    study_sizes = agg_tdata.study_sizes

    # mt = tdata.mean_temp
    # dt = tdata.daily_temp
    # scaled_dt = utils.scale_daily_temp(mt, dt, scale_params)

    # obs_mean = tdata.obs_mean
    # obs_std = tdata.obs_std
    # study_sizes = np.array([1]*obs_mean.size)

    # create spline
    mt_knots = np.linspace(mt.min(), mt.max(), 2)
    dt_knots = np.linspace(scaled_dt.min(), scaled_dt.max(), 2)
    mt_degree = 3
    dt_degree = 3
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
        # {
        #     'prior_type': 'x_cov_gprior',
        #     'x_cov_id': 0,
        #     'prior': np.array([[0.0]*mr.k_beta, [10.0]*mr.k_beta])
        # },
        {
            'prior_type': 'ndspline_shape_monotonicity',
            'x_cov_id': 0,
            'dim_id': 1,
            'interval': [[mt.min(), mt.max()],
                         [0.5, scaled_dt.max()]],
            'indicator': 'increasing',
            'num_points': [20, 10]
        },
        {
            'prior_type': 'ndspline_shape_monotonicity',
            'x_cov_id': 0,
            'dim_id': 1,
            'interval': [[mt.min(), mt.max()],
                         [scaled_dt.min(), -0.75]],
            'indicator': 'decreasing',
            'num_points': [20, 10]
        },
        {
            'prior_type': 'z_cov_uprior',
            'z_cov_id': 0,
            'prior': np.array([[1e-6]*mr.k_gamma, [1e-6]*mr.k_gamma])
        }
    ]
    mr.addPriors(prior_list)

    # fit the model and store the result
    mr.fitModel()

    # compute posterior variance for the beta
    # extract the matrix
    k_beta = mr.lt.k_beta
    k_gamma = mr.lt.k_gamma
    X = mr.lt.JF(mr.lt.beta)
    S = mr.lt.S
    V = S**2

    beta_var = (X.T/V).dot(X)

    if mr.lt.use_regularizer:
        H = np.vstack([mr.lt.H(np.hstack(np.eye(1, k_beta, i).reshape(k_beta,),
                               np.zeros(k_gamma)))
                       for i in range(k_beta)]).T
        SH = mr.lt.h[1]
        VH = SH**2
        beta_var += (H.T/VH).dot(VH)

    beta_var = np.linalg.inv(beta_var)


    surface_result = utils.SurfaceResult(
            mr.beta_soln,
            beta_var,
            spline_list[0],
            scale_params=scale_params)

    return surface_result, agg_tdata


def fit_trend(tdata, surface_result, inlier_pct=1.0):
    """fit the study structure in the residual"""
    residual_tdata = copy.deepcopy(tdata)
    residual_tdata.obs_mean -= surface_result.surface_func(
            tdata.mean_temp,
            tdata.daily_temp)

    unique_mean_temp = np.sort(np.unique(residual_tdata.mean_temp))

    beta = []
    beta_var = []
    gamma = []
    random_effects = []

    for mean_temp in unique_mean_temp:
        (beta_at_mt,
         beta_var_at_mt,
         gamma_at_mt,
         random_effects_at_mt) = fit_trend_at_mean_temp(residual_tdata,
                                                        mean_temp,
                                                        inlier_pct=inlier_pct)
        beta.append(beta_at_mt)
        beta_var.append(beta_var_at_mt)
        gamma.append(gamma_at_mt)
        random_effects.append(random_effects_at_mt)

    beta = np.vstack(beta)
    beta_var = np.vstack(beta_var)
    gamma = np.vstack(gamma)

    trend_result = utils.TrendResult(beta, beta_var, gamma, random_effects,
                                     unique_mean_temp)

    return trend_result, residual_tdata


def fit_trend_at_mean_temp(tdata, mean_temp, inlier_pct=0.9, debug=False):
    """
        Return beta (intercept and slope) and gamma (intercept and slope)
        with given data
    """
    tdata_at_mean_temp = extract_at_mean_temp(tdata, mean_temp)
    if debug:
        print("number of locations at mean temp",
              tdata_at_mean_temp.num_studies)
        outer_verbose = True
        inner_print_level = 5
    else:
        outer_verbose = False
        inner_print_level = 0

    # construct the linear mapping
    cov = tdata_at_mean_temp.daily_temp

    M = cov.reshape(cov.size, 1) - 1.0*mean_temp
    # scale = np.linalg.norm(M, axis=0)
    scale = 1.0
    M /= scale

    # construct the LimeTr object
    F = lambda beta: M.dot(beta)
    JF = lambda beta: M
    Z = M

    n = tdata_at_mean_temp.study_sizes
    k_beta = 1
    k_gamma = 1

    Y = tdata_at_mean_temp.obs_mean
    S = tdata_at_mean_temp.obs_std

    uprior = np.array([
            [-np.inf]*k_beta + [1e-7]*k_gamma,
            [np.inf]*k_beta + [np.inf]*k_gamma
        ])

    lt = limetr.LimeTr(n, k_beta, k_gamma, Y, F, JF, Z, S=S,
                       uprior=uprior,
                       inlier_percentage=inlier_pct)

    # fit model
    MS = M/S.reshape(S.size, 1)
    YS = Y/S
    beta0 = np.linalg.solve(MS.T.dot(MS), MS.T.dot(YS))
    gamma0 = np.array([0.1])
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
    beta_var = 1.0/M.T.dot(V.invDot(M))

    # scale beta and gamma back
    beta /= scale
    beta_var /= scale**2
    gamma /= scale**2
    random_effects /= scale

    return beta, beta_var, gamma, random_effects


def sample_surface(mt, dt, num_samples, surface_result, trend_result,
                   include_re=True):
    # predict data
    surface_result.sample_fixed_effects(num_samples)
    trend_result.sample_random_effects(num_samples)

    mt_id = np.array([np.where(trend_result.mean_temp == mt[i])[0][0]
                      for i in range(mt.size)])
    tmrl = surface_result.tmrl[mt_id]
    curve_samples = []
    for i in range(num_samples):
        curve_sample = surface_result.surface_func(
                            mt, dt,
                            beta=surface_result.beta_samples[i])
        if include_re:
            u1_samples = trend_result.re_samples[i, 0, mt_id]
            u2_samples = trend_result.re_samples[i, 1, mt_id]
            trend_samples = np.maximum(dt - tmrl[i], 0.0)*u2_samples + \
                np.minimum(dt - tmrl[i], 0.0)*u1_samples
            curve_sample += trend_samples

        curve_samples.append(curve_sample)

    return np.vstack(curve_samples)
