import math
from typing import Final, List

import numpy as np
import pandas as pd
from scipy.stats import gamma, gumbel_r

_XMAX: Final[int] = 350


def gamma_distribution(
    df: pd.DataFrame, mean: pd.Series, q_col: str, vr_col: str
) -> np.ndarray:
    """Returns the area under the curve of a gamma distribution given a cut-off value, mean,
    and variance.
    """
    gamma_shape = mean**2 / df[vr_col]
    gamma_rate = mean / df[vr_col]
    weight = 0.4
    gamma_cdf = gamma.cdf(x=df[q_col], a=gamma_shape, scale=(1 / gamma_rate))
    return weight * gamma_cdf


def mirrored_gumbel_distribution(
    df: pd.DataFrame, mean: pd.Series, q_col: str, vr_col: str
) -> np.ndarray:
    """Returns the area under the curve of a mirrored right gumbel distribution given a
    cut-off value, mean, and variance.
    """
    mgumbel_alpha = (
        _XMAX - mean - (np.euler_gamma * np.sqrt(df[vr_col]) * math.sqrt(6) / math.pi)
    )
    mgumbel_scale = np.sqrt(df[vr_col]) * math.sqrt(6) / math.pi
    weight = 0.6
    gumbel_cdf = 1 - gumbel_r.cdf(x=_XMAX - df[q_col], loc=mgumbel_alpha, scale=mgumbel_scale)
    return weight * gumbel_cdf


def ensemble_prevalence(
    df: pd.DataFrame, q_col: str, mean_cols: List[str], variance_col: str
) -> pd.Series:
    """Ensemble prevalence function.

    Takes a weighted combination of the area under the curve of two distributions, gamma and
    a mirrored right gumbel, to get prevalence estimates.

    Args:
        df (pd.DataFrame): Dataframe containing relevant columns
        q_col (str): Column containing the cut-off value of the distributions.
        mean_cols (strlist): List of columns to sum together for a mean of the distributions.
        variance_col (str): Column containing the variance of the distributions.
    """
    summed_mean = sum([df[col] for col in mean_cols])
    prev = gamma_distribution(
        df=df, mean=summed_mean, q_col=q_col, vr_col=variance_col
    ) + mirrored_gumbel_distribution(
        df=df, mean=summed_mean, q_col=q_col, vr_col=variance_col
    )
    return prev
