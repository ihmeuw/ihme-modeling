import numpy as np
import pandas as pd
from sklearn.gaussian_process import GaussianProcessRegressor, kernels


# UTILITY FUNCTIONS
def invlogit(x):
    return np.exp(x) / (np.exp(x) + 1)


def logit(x):
    return np.log(x / (1 - x))


def invlogit_var(mu, var):
    return var * (np.exp(mu) / (np.exp(mu) + 1) ** 2) ** 2


def logit_var(mu, var):
    return var / (mu * (1 - mu)) ** 2


# GPR FUNCTION... CONSIDER MOVING THIS TO A MODULE
def fit_gpr(
    df,
    amp,
    random_seed,
    obs_variable="observed_data",
    obs_var_variable="obs_data_variance",
    mean_variable="st_prediction",
    year_variable="year_id",
    scale=10,
    draws=0,
    spacevar="location_id",
    agevar="age_group_id",
    sexvar="sex_id",
):
    """

    Arguments:
        scale: a characteristic time scale for smoothness of the functions
            note that "scale" will be divided by sqrt(2) before being passed as
            length_scale to the Matern covariance function.
    """

    initial_columns = list(df.columns)

    data = df[(df[obs_variable].notnull()) & (df[obs_var_variable].notnull())]
    mean_prior = df[[year_variable, mean_variable]].drop_duplicates()

    # numpy.interp requires increasing values in x
    isort = np.argsort(mean_prior[year_variable])

    def mean_function(x):
        return np.interp(
            x, mean_prior[year_variable].iloc[isort], mean_prior[mean_variable].iloc[isort]
        )

    kernel = kernels.Product(
        kernels.ConstantKernel(
            constant_value=np.power(amp, 2.0), constant_value_bounds="fixed"
        ),
        kernels.Matern(
            length_scale=(scale / np.sqrt(2)), length_scale_bounds="fixed", nu=2.5
        ),
    )
    sk_gpr = GaussianProcessRegressor(kernel=kernel, alpha=data[obs_var_variable].values)

    if len(data) > 0:
        sk_gpr.fit(
            data[year_variable].values.reshape(-1, 1),
            data[obs_variable].values - mean_function(data[year_variable].values),
        )

    xpred = mean_prior[year_variable].values.reshape(-1, 1)
    model_mean, model_sigma = sk_gpr.predict(xpred, return_std=True, return_cov=False)
    model_mean += mean_function(xpred.squeeze())
    model_variance = np.power(model_sigma, 2.0)
    model_lower = model_mean - np.sqrt(model_variance) * 1.96
    model_upper = model_mean + np.sqrt(model_variance) * 1.96

    if draws > 0:
        real_draws = pd.DataFrame(
            {
                year_variable: xpred.squeeze(),
                "gpr_mean": model_mean,
                "gpr_var": model_variance,
                "gpr_lower": model_lower,
                "gpr_upper": model_upper,
            }
        ).reset_index(drop=True)
        realizations = sk_gpr.sample_y(xpred, draws, random_state=random_seed).T
        realizations += mean_function(xpred.squeeze())

        draws_for_real_draws = []
        for i, r in enumerate(realizations):
            draws_for_real_draws.append(pd.Series(r, name=f"draw_{i}"))

        real_draws = pd.concat([real_draws] + draws_for_real_draws, axis=1)
        real_draws = pd.merge(df, real_draws, on=year_variable, how="left")
        gpr_columns = ["gpr_mean", "gpr_var", "gpr_lower", "gpr_upper"]
        draw_columns = ["draw_" + str(i) for i in range(draws)]
        initial_columns.extend(gpr_columns)
        initial_columns.extend(draw_columns)

        return real_draws[initial_columns]

    else:
        results = pd.DataFrame(
            {
                year_variable: xpred.squeeze(),
                "gpr_mean": model_mean,
                "gpr_var": model_variance,
                "gpr_lower": model_lower,
                "gpr_upper": model_upper,
            }
        )
        gpr_columns = list(set(results.columns) - set(initial_columns))
        initial_columns.extend(gpr_columns)
        results = pd.merge(df, results, on=year_variable, how="left")

        return results
