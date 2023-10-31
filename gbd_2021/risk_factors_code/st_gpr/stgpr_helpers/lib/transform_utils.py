"""Helpers for transforming data and variance."""
from typing import TypeVar

import numpy as np
import pandas as pd

from db_stgpr.api.enums import TransformType

SeriesOrDataFrame = TypeVar("SeriesOrDataFrame", pd.Series, pd.DataFrame)


def transform_data(
    mean: SeriesOrDataFrame, transform: str, reverse: bool = False
) -> SeriesOrDataFrame:
    """Applies transformation to convert data into or out of modeling space."""
    if transform == TransformType.logit.name:
        if reverse:
            return np.exp(mean) / (np.exp(mean) + 1)
        return np.log(mean / (1 - mean))
    elif transform == TransformType.log.name:
        if reverse:
            return np.exp(mean)
        return np.log(mean)
    else:
        return mean


def transform_variance(
    mean: pd.Series, variance: pd.Series, transform: str, reverse: bool = False
) -> pd.Series:
    """Applies transformation to convert variance into or out of modeling space."""
    if transform == TransformType.logit.name:
        if reverse:
            return variance / (1 / (mean * (1 - mean))) ** 2
        return variance * (1 / (mean * (1 - mean))) ** 2
    elif transform == TransformType.log.name:
        if reverse:
            return variance / (1 / mean ** 2)
        return variance * (1 / mean ** 2)
    else:
        return variance
