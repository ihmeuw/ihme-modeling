"""Helpers for calculating and applying offsets."""

import warnings
from typing import Optional

import pandas as pd

import stgpr_schema

from stgpr_helpers.lib import transform_utils
from stgpr_helpers.lib.constants import columns
from stgpr_helpers.lib.validation import data as data_validation


def calculate_offset(
    data: pd.Series, transform: str, custom_offset: Optional[float]
) -> float:
    """Calculates the data offset based on the transform.

    Uses custom offset if specified.

    Default offset = median(data) * offset_factor (as per Dismod).
    Offset factor is 0.01 for log models, 0.001 for logit models.

    Args:
        data: the data whose median is used to calculate an offset.
        transform: transform that will be applied to the data.
        custom_offset: user-specified custom offset.

    Returns:
        Calculated offset, or custom offset if given
    """
    if custom_offset is not None:
        warnings.warn(
            f"Found offset {custom_offset} in config. Running with a custom offset "
            "is not recommended; if offset is left blank, ST-GPR will pick a "
            "decent offset for you"
        )
        return custom_offset

    offset_factor = 0.01 if transform == stgpr_schema.TransformType.log.name else 0.001
    return data.median() * offset_factor


def offset_data(data: pd.Series, transform: str, offset: float) -> pd.Series:
    """Applies an offset to data.

    Args:
        data: the data whose median is used to calculate an offset.
        transform: transform that will be applied to the data.
        offset: offset to apply to the data.

    Returns:
        Data with offset applied.
    """
    data = data.copy()
    if transform != stgpr_schema.TransformType.none.name:
        if transform == stgpr_schema.TransformType.log.name:
            data = data + offset
        elif transform == stgpr_schema.TransformType.logit.name:
            data = (1 - offset) * (data + offset)

    return data


def offset_and_transform_data(
    df: pd.DataFrame, offset: float, transform: str
) -> pd.DataFrame:
    """Offsets data, validates bounds, and transforms.

    Note that variance transformation must happen before data transformation since the
    variance transform uses the data values.

    Args:
        df: dataframe containing data and variance.
        offset: the offset to apply.
        transform: the transform to apply.

    Returns:
        Dataframe containing offset and transformed data and variance.
    """
    # TODO: Once ST-GPR standardizes on "val" over "data," remove check for "data."
    data_col = columns.DATA if columns.DATA in df else columns.VAL
    offset_df = df.assign(**{data_col: offset_data(df[data_col], transform, offset)})
    data_validation.validate_data_bounds_for_transformation(offset_df, data_col, transform)
    return offset_df.assign(
        **{
            columns.VARIANCE: transform_utils.transform_variance(
                offset_df[data_col], offset_df[columns.VARIANCE], transform
            )
        }
    ).assign(**{data_col: transform_utils.transform_data(offset_df[data_col], transform)})
