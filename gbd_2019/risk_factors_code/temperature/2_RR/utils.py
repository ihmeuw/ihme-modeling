# -*- coding: utf-8 -*-
"""
    data
    ~~~~

    Utility functions and classes.
"""
import numpy as np
import pandas as pd
import xspline
import ipopt


class TempData:
    """Temperature data class"""
    def __init__(
            self,
            mean_temp,
            daily_temp,
            obs_mean,
            obs_std,
            study_id,
            data_id,
            trimming_weights=None):
        # pass in the data
        sort_id = np.argsort(study_id)
        self.mean_temp = mean_temp[sort_id]
        self.daily_temp = daily_temp[sort_id]
        self.obs_mean = obs_mean[sort_id]
        self.obs_std = obs_std[sort_id]
        self.study_id = study_id[sort_id]
        if data_id is not None:
            self.data_id = data_id[sort_id]
        else:
            self.data_id = None
        self.unique_mean_temp = np.unique(self.mean_temp)

        # construct structure
        unique_study_id, study_sizes = np.unique(self.study_id,
                                                 return_counts=True)
        sort_id = np.argsort(unique_study_id)
        self.unique_study_id = unique_study_id[sort_id]
        self.study_sizes = study_sizes[sort_id]
        self.num_studies = self.study_sizes.size
        self.num_obs = self.obs_mean.size

        # pass in trimming weights if given
        if trimming_weights is None:
            self.trimming_weights = np.ones(self.num_obs)
        else:
            self.trimming_weights = trimming_weights


class TrendResult:
    """Trend fitting result"""
    def __init__(
            self,
            beta,
            beta_var,
            gamma,
            random_effects,
            mean_temp,
            num_beta_spline_knots=6,
            num_gamma_spline_knots=6,
            beta_spline_degree=3,
            gamma_spline_degree=3):
        # pass in the data
        self.num_mean_temp = mean_temp.size
        assert beta.shape == (self.num_mean_temp, 2)
        assert gamma.shape == (self.num_mean_temp, 2)
        self.beta = beta
        self.beta_var = beta_var
        self.gamma = gamma
        self.mean_temp = mean_temp
        self.random_effects = random_effects

        # construct the splines
        self.min_mean_temp = self.mean_temp.min()
        self.max_mean_temp = self.mean_temp.max()
        beta_spline_knots = np.linspace(self.min_mean_temp,
                                        self.max_mean_temp,
                                        num_beta_spline_knots)
        gamma_spline_knots = np.linspace(self.min_mean_temp,
                                         self.max_mean_temp,
                                         num_gamma_spline_knots)
        # gamma_spline_knots = np.array([
        #         self.min_mean_temp,
        #         13.0,
        #         17.0,
        #         22.0,
        #         self.max_mean_temp
        #     ])
        self.beta_spline = xspline.xspline(
            beta_spline_knots, beta_spline_degree,
            l_linear=True, r_linear=True)
        self.gamma_spline = xspline.xspline(
            gamma_spline_knots, gamma_spline_degree,
            l_linear=True, r_linear=True)

        # compute the spline bases coefficients
        X_beta = self.beta_spline.designMat(self.mean_temp)
        X_gamma = self.gamma_spline.designMat(self.mean_temp)
        self.c_beta = np.linalg.solve(X_beta.T.dot(X_beta),
                                      X_beta.T.dot(beta))
        self.c_gamma = np.linalg.solve(X_gamma.T.dot(X_gamma),
                                       X_gamma.T.dot(gamma))

    def beta_at_mean_temp(self, mean_temp):
        """return beta(s) at given mean_temp"""
        X = self.beta_spline.designMat(mean_temp)
        return X.dot(self.c_beta)

    def gamma_at_mean_temp(self, mean_temp):
        """return gamma(s) at give mean_temp"""
        X = self.gamma_spline.designMat(mean_temp)
        return X.dot(self.c_gamma)

    def sample_random_effects(self, num_samples):
        """sample the random effects at the mean temperature"""
        re_samples = []
        for mt in self.mean_temp:
            gamma = np.maximum(1e-6, self.gamma_at_mean_temp(mt))
            beta = self.beta_at_mean_temp(mt)
            re_samples.append(np.random.randn(num_samples, gamma.size)*
                              np.sqrt(gamma))

        self.re_samples = np.dstack(re_samples)


class SurfaceResult:
    """Residual fitting result"""
    def __init__(
            self,
            beta,
            beta_var,
            spline,
            mean_temp,
            daily_temp_range,
            scale_params=[40.0, 1.25]):
        # pass in the data
        self.beta = beta
        self.beta_var = beta_var
        self.spline = spline
        self.scale_params = scale_params
        self.mean_temp = mean_temp
        self.daily_temp_range = daily_temp_range
        # self.tmrl = np.array([
        #     self.tmrl_at_mean_temp(self.mean_temp[i],
        #                            daily_temp_range=self.daily_temp_range[i])
        #     for i in range(self.mean_temp.size)])
        self.tmrl = mean_temp.copy()

    def surface_func(self, mean_temp, daily_temp,
                     beta=None):
        """return surface at given temp_pairs"""
        if beta is None:
            beta = self.beta
        num_points = mean_temp.size
        scaled_daily_temp = scale_daily_temp(mean_temp, daily_temp,
                                             self.scale_params)
        X = self.spline.designMat([mean_temp, scaled_daily_temp],
                                  grid_off=True,
                                  l_extra_list=[True, True],
                                  r_extra_list=[True, True])
        return X.dot(beta)

    def sample_fixed_effects(self, num_samples):
        """sample the fixed effects"""
        beta_samples = np.random.multivariate_normal(
                self.beta, self.beta_var, num_samples)

        self.beta_samples = beta_samples

    def tmrl_at_mean_temp(self, mean_temp, num_points=100,
                          daily_temp_range=None):
        if daily_temp_range is None:
            lb = (mean_temp - 50.0)/44.0
            scaled_daily_temp = np.linspace(lb, 0.7, num_points)
            daily_temp = unscale_daily_temp(mean_temp,
                                            scaled_daily_temp,
                                            self.scale_params)
        else:
            lb = np.maximum((mean_temp - 50.0)/44.0,
                            scale_daily_temp(mean_temp,
                                             np.array([daily_temp_range[0]]),
                                             self.scale_params)[0]*0.7)
            ub = np.minimum(0.7,
                            scale_daily_temp(mean_temp,
                                             np.array([daily_temp_range[1]]),
                                             self.scale_params)[0]*0.7)
            lb = unscale_daily_temp(mean_temp, np.array([lb]),
                                    self.scale_params)[0]
            ub = unscale_daily_temp(mean_temp, np.array([ub]),
                                    self.scale_params)[0]
            daily_temp = np.linspace(lb, ub, num_points)

        val = self.surface_func(np.repeat(mean_temp, num_points),
                                daily_temp)

        return daily_temp[np.argmin(val)]


def scale_daily_temp(mean_temp, daily_temp, scale_params):
    """linear scale daily temp"""
    scaled_daily_temp = (daily_temp - mean_temp)/\
            (scale_params[0] - scale_params[1]*mean_temp)

    return scaled_daily_temp

def unscale_daily_temp(mean_temp, scaled_daily_temp, scale_params):
    """scale back the daily temp"""
    daily_temp = scaled_daily_temp*\
        (scale_params[0] - scale_params[1]*mean_temp) + mean_temp

    return daily_temp

def sizes_to_slices(sizes):
    """convert sizes to slices"""
    break_points = np.cumsum(np.insert(sizes, 0, 0))
    slices = []
    for i in range(len(sizes)):
        slices.append(slice(break_points[i], break_points[i + 1]))
    return slices


def fit_line(obs_mean, obs_std, cov):
    """
        Fit a line by give observations,
        return intercept, slope (beta) and their postier covariance
    """
    y = obs_mean
    v = obs_std**2
    x = cov
    M = np.vstack((np.ones(x.size), x)).T

    beta_var = np.linalg.inv((M.T/v).dot(M))
    beta = beta_var.dot((M.T/v).dot(y))

    return beta, beta_var


def fit_spline(obs_mean, obs_std, cov, spline):
    """Fit a spline by given observatinos and spline
    """

    M = spline.designMat(cov)
    beta = np.linalg.solve((M.T/obs_std**2).dot(M),
                           (M.T/obs_std**2).dot(obs_mean))

    # residual = (obs_mean - M.dot(beta))/obs_std
    # print(np.std(residual))

    return beta

def create_grid_points(mts, ddt,
                       scaled_dt_range=[-1.0, 0.8],
                       scale_params=[40.0, 1.25]):
    mt_list = []
    dt_list = []
    for mt in mts:
        mt_sub, dt_sub = create_grid_points_at_mean_temp(
                mt, ddt,
                scaled_dt_range=scaled_dt_range,
                scale_params=scale_params,
            )
        mt_list.append(mt_sub)
        dt_list.append(dt_sub)
    return np.hstack(mt_list), np.hstack(dt_list)


def create_grid_points_at_mean_temp(mt, ddt,
                                    scaled_dt_range=[-1.0, 0.8],
                                    scale_params=[40.0, 1.25]):
    scaled_ddt = ddt/(scale_params[0] - scale_params[1]*mt)
    scaled_dt = np.arange(scaled_dt_range[0],
                          scaled_dt_range[1], scaled_ddt)
    dt = unscale_daily_temp(mt, scaled_dt, scale_params)

    return np.repeat(mt, dt.size), dt    
