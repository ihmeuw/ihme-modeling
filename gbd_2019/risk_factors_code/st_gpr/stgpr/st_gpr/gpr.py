import numpy as np
import pandas as pd
from pymc import gp


# UTILITY FUNCTIONS
def invlogit(x):
    return np.exp(x)/(np.exp(x)+1)


def logit(x):
    return np.log(x/(1-x))


def invlogit_var(mu, var):
    return (var*(np.exp(mu)/(np.exp(mu)+1)**2)**2)


def logit_var(mu, var):
    return (var/(mu*(1-mu))**2)


# GPR FUNCTION
def fit_gpr(
        df, amp, obs_variable='observed_data',
        obs_var_variable='obs_data_variance', mean_variable='st_prediction',
        year_variable='year_id', scale=10, diff_degree=2, draws=0):

    initial_columns = list(df.columns)

    data = df[(df[obs_variable].notnull()) & (df[obs_var_variable].notnull())]
    mean_prior = df[[year_variable, mean_variable]].drop_duplicates()

    def mean_function(x):
        return np.interp(
            x, mean_prior[year_variable], mean_prior[mean_variable])

    M = gp.Mean(mean_function)
    C = gp.Covariance(
        eval_fun=gp.matern.euclidean, diff_degree=diff_degree,
        amp=amp, scale=scale)

    if len(data) > 0:
        gp.observe(
            M=M, C=C, obs_mesh=data[year_variable],
            obs_V=data[obs_var_variable], obs_vals=data[obs_variable])

    model_mean = M(mean_prior[year_variable]).T
    # model_variance = np.diagonal(C(p_years,p_years)).T
    model_variance = C(mean_prior[year_variable])
    model_lower = model_mean - np.sqrt(model_variance)*1.96
    model_upper = model_mean + np.sqrt(model_variance)*1.96

    if draws > 0:
        """
        The pymc version of drawing realizations... slower than just
        sampling directly from the MVN, but should give the same result:

        realizations = [
            gp.Realization(M, C)(range(1980,2014)) for i in range(draws)]
        """

        real_draws = pd.DataFrame({
            year_variable: mean_prior[year_variable],
            'gpr_mean': model_mean, 'gpr_var': model_variance,
            'gpr_lower': model_lower, 'gpr_upper': model_upper})

        realizations = np.random.multivariate_normal(
            model_mean,
            C(mean_prior[year_variable], mean_prior[year_variable]), draws)

        for i, r in enumerate(realizations):
            real_draws["draw_"+str(i)] = r

        real_draws = pd.merge(df, real_draws, on=year_variable, how='left')
        # gpr_columns = list(set(real_draws.columns) - set(initial_columns))
        gpr_columns = ['gpr_mean', 'gpr_var', 'gpr_lower', 'gpr_upper']
        draw_columns = ['draw_'+str(i) for i in range(draws)]
        initial_columns.extend(gpr_columns)
        initial_columns.extend(draw_columns)

        return real_draws[initial_columns]

    else:
        results = pd.DataFrame({
            year_variable: mean_prior[year_variable], 'gpr_mean': model_mean,
            'gpr_var': model_variance, 'gpr_lower': model_lower,
            'gpr_upper': model_upper})
        gpr_columns = list(set(results.columns) - set(initial_columns))
        initial_columns.extend(gpr_columns)
        results = pd.merge(df, results, on=year_variable, how='left')

        return results