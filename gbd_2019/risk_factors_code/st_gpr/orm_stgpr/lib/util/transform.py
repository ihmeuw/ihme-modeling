import logging

import numpy as np
import pandas as pd

from orm_stgpr.db import lookup_tables


def transform_data(
        mean: pd.Series,
        transform: str,
        reverse: bool = False
) -> pd.Series:
    """
    Applies transformation to mean to convert it into or out of modeling
    space.
    """
    if transform == lookup_tables.TransformType.logit.name:
        if reverse:
            return np.exp(mean) / (np.exp(mean) + 1)
        return np.log(mean / (1 - mean))
    elif transform == lookup_tables.TransformType.log.name:
        if reverse:
            return np.exp(mean)
        return np.log(mean)
    else:
        return mean


def transform_variance(
        mean: pd.Series,
        variance: pd.Series,
        transform: str,
        reverse: bool = False,
) -> pd.Series:
    """
    Applies delta transformation to variance to convert it into or out of
    modeling space. variance_delta_transformed = variance * [g'(x)]**2,
    where g(x) is the specified link function.
    """
    logging.info('Transforming')
    if transform == lookup_tables.TransformType.logit.name:
        if reverse:
            return variance / (1 / (mean * (1 - mean)))**2
        return variance * (1 / (mean * (1 - mean)))**2
    elif transform == lookup_tables.TransformType.log.name:
        if reverse:
            return variance / (1 / mean**2)
        return variance * (1 / mean**2)
    else:
        return variance
