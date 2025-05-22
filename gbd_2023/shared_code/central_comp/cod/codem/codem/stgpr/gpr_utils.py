import re
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.gaussian_process import GaussianProcessRegressor, kernels


def extract_training_data(
    df: pd.DataFrame, training_label: str, input_label: str, response_label: str
) -> Tuple[np.array, np.array, np.array]:
    """Extract training data and variance from a dataframe."""
    df = df[df[training_label]]
    data_x = df[input_label].values.reshape(-1, 1)
    data_y = df[response_label].values
    data_var = df[response_label + "_sd"].values ** 2 + df[response_label + "_nsv"].values
    return data_x, data_y, data_var


def gpr(
    df: pd.DataFrame,
    response: str,
    amplitude: float,
    prior: np.ndarray,
    scale: float,
    n_draws: Optional[int] = None,
    legacy_scaling: bool = True,
    random_seed: int = None,
) -> np.ndarray:
    """
    Runs an instance of Gaussian process smoothing to account for years
    where we do not have data. The data frame is specific to a location-age.

    Args:
        df: The input DataFrame. Required columns are "year_id", "ko{knockout}_train",
            "{response}", "{response}_sd", "{response}_nsv"
        response: The column name for the dependent variable, typically "ln_rate" or "ln_cf"
        amplitude: The factor by which to scale the covariance function
        prior: An array of values representing a prior mean value of the response
        scale: The length scale specification for the covariance function
        n_draws: When provided, n=draws realizations are returned. When not provided
            only the most probable realization is returned.
        legacy_scaling: If True, divides the input scale by sqrt(2) to match PyMC 2
            behavior
        random_seed: int to seed draw sampling, passed as model_version_id

    Returns:
        The most probable GPR predicted values given the mean and data priors.
    """

    def mean_func(x):
        isort = np.argsort(df["year_id"])
        return np.interp(x, df["year_id"].iloc[isort], prior[isort]).squeeze()

    training_label = list(filter(lambda x: re.search(r"ko[0-9]+_train", x), df.columns))[0]
    data_x, data_y, data_var = extract_training_data(
        df=df, training_label=training_label, input_label="year_id", response_label=response
    )

    if legacy_scaling:
        scale /= np.sqrt(2)
    kernel = kernels.Product(
        kernels.ConstantKernel(
            constant_value=np.power(amplitude, 2.0), constant_value_bounds="fixed"
        ),
        kernels.Matern(length_scale=scale, length_scale_bounds="fixed", nu=2.5),
    )

    sk_gpr = GaussianProcessRegressor(kernel=kernel, alpha=data_var)
    if df[training_label].sum() > 0:
        sk_gpr.fit(data_x, data_y - mean_func(data_x))

    xpred = df["year_id"].values

    if n_draws is None:
        prediction = sk_gpr.predict(xpred.reshape(-1, 1), return_std=False, return_cov=False)
        prediction += mean_func(xpred)
        return prediction
    else:
        realizations = sk_gpr.sample_y(
            xpred.reshape(-1, 1), n_draws, random_state=random_seed
        )
        realizations += mean_func(xpred).reshape(-1, 1)
        return realizations
