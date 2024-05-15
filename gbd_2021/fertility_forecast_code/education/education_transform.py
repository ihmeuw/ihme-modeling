"""Functions for transforming education into different modeling-spaces and then
inverse functions for moving predictions back into the identity domain.
"""
import logging

import xarray as xr
from frozendict import frozendict
from scipy.special import expit, logit

LOGGER = logging.getLogger(__name__)
MAX_EDU = 18  # The maximum number of years of education someone can attain

# `EPSILON` is used to form the lower and upper bound before converting
# education data to logit space. This value was picked to include most of the
# past education, and to avoid getting infinities and negative infinities when
# converting to logit space.
EPSILON = 1e-3

# The maximum number of years of education someone can attain within certain
# age groups.
SPECIAL_AGE_CAP_DICT = frozendict({
    6: 3,
    7: 8,
    8: 13
    })


def normal_to_logit(data):
    """Convert education from linear space to logit space, using a lower bound
    of 0 and an upper bound of ``MAX_EDU``.

    Args:
        data (xr.DataArray):
            education in linear space.
    Returns:
        xr.DataArray:
            education in logit space.
    """
    LOGGER.debug("Converting to logit space.")
    data_caps = get_edu_caps(data)
    scaled_data = data / data_caps
    clipped_data = scaled_data.clip(min=EPSILON, max=1 - EPSILON)
    return logit(clipped_data)


def logit_to_normal(data):
    """Convert education from logit space to linear space, using a lower bound
    of 0 and an upper bound of ``MAX_EDU``.

    Args:
        data (xr.DataArray):
            education in logit space.
    Returns:
        xr.DataArray:
            education in linear space.
    """
    LOGGER.debug("Converting to linear space.")
    data_caps = get_edu_caps(data)
    return expit(data) * data_caps


def log_to_normal(data):
    LOGGER.debug("Converting to linear space.")
    data_caps = get_edu_caps(data)
    _, expanded_data_caps = xr.broadcast(data, data_caps)
    return xr.ufuncs.exp(data).clip(min=0, max=expanded_data_caps)


def get_edu_caps(data):
    data_caps = xr.ones_like(data["age_group_id"]) * MAX_EDU
    for age_group_id, edu_cap in SPECIAL_AGE_CAP_DICT.items():
        data_caps = data_caps.where(
            data_caps["age_group_id"] != age_group_id
            ).fillna(edu_cap)
    return data_caps
