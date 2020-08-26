import logging
from typing import Optional
import warnings

import pandas as pd

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import columns
from orm_stgpr.lib.util import helpers, transform
from orm_stgpr.lib.validation import data_validation


def offset_and_transform_data(
        df: pd.DataFrame,
        gbd_round_id: int,
        decomp_step: str,
        offset: float,
        transform_value: str
) -> pd.DataFrame:
    """
    Offset the data, validate the data bounds, then transform the data.
    Variance transformation must happen before data transformation since
    the variance transform uses the data values.

    Args:
        df: dataframe containing data and variance
        offset: the offset value, already been calculated or passed in
        transform_value: the transform to apply
        decomposition_step: decomp step. Needed to choose which methods to use

    Returns:
        Dataframe containing offset, transformed data and variance
    """
    offset_function = helpers.lookup_method(
        'offset', gbd_round_id, decomp_step)

    offset_df = df.assign(**{columns.DATA: offset_function(
        df[columns.DATA], transform_value, offset
    )})
    data_validation.validate_data_bounds_for_transformation(
        offset_df, columns.DATA, transform_value
    )
    return offset_df\
        .assign(**{columns.VARIANCE: transform.transform_variance(
            offset_df[columns.DATA],
            offset_df[columns.VARIANCE],
            transform_value)})\
        .assign(**{columns.DATA: transform.transform_data(
            offset_df[columns.DATA], transform_value)})


def calculate_offset(
        data: pd.Series,
        transform_value: str,
        offset: Optional[float],
        gbd_round_id: int,
        decomp_step: str
) -> float:
    """
    Calculates the offset based on the transform.
    Uses custom offset if specified.
    Default offset = median(data) * offset_factor (as per Dismod).

    Args:
        data: the data whose median is used to calculate an offset
        transform_value: whether the model is running in log or logit space
        offset: user-specified custom offset

    Returns:
        Calculated offset, or custom offset if given
    """
    if offset is not None:
        if not (helpers.use_old_methods(gbd_round_id, decomp_step)
                and gbd_round_id == 6):
            warnings.warn(
                f'Found offset {offset} in config. Running with a custom offset '
                'is not recommended; if offset is left blank, ST-GPR will pick a '
                'decent offset for you'
            )
        return offset

    # 0.01 is *way* too large for logit models, so use 0.001 instead.
    offset_factor = (
        0.01
        if transform_value == lookup_tables.TransformType.log.name
        else 0.001
    )
    return data.median() * offset_factor


def offset_data(
        data: pd.Series,
        transform_value: str,
        offset: float,
) -> pd.Series:
    """
    Calculates and applies an offset to data.

    Args:
        data: the data to offset
        transform_value: whether the model is running in log or logit space
        offset: user-specified custom offset

    Returns:
        Data with offset applied
    """
    logging.info('Offsetting data')
    data = data.copy()

    if transform_value != lookup_tables.TransformType.none.name:
        if transform_value == lookup_tables.TransformType.log.name:
            data = data + offset
        elif transform_value == lookup_tables.TransformType.logit.name:
            data = (1 - offset) * (data + offset)

    return data


def offset_data_old(
        data: pd.Series,
        transform_value: str,
        offset: float
) -> pd.Series:
    data = data.copy()
    if transform_value == lookup_tables.TransformType.log.name:
        data[data <= 0] = offset
    elif transform_value == lookup_tables.TransformType.logit.name:
        data[data <= 0] = offset
        data[data >= 1] = 1 - offset
    return data
