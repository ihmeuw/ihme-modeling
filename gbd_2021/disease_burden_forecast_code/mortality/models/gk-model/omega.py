"""This module contains utilities for preparing the Bayesian Omega Priors of the GK Model.
"""

from typing import Dict, Union

import numpy as np
from scipy.optimize import brentq
from scipy.special import gammaln
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import GKModelConstants

logger = get_logger()


def omega_translate(omega_amp: Union[float, Dict[str, float]]) -> Dict[str, float]:
    """Creates a mapping of all omega parameters to their amplification values.

    Creates a dictionary where the keys are the names of all the omega parameters and the
    keys are the amplification value used to scale the values of each omega parameter. If
    a single ``float`` value is given then all omegas will have that value. Missing omega
    parameters will be assigned an amplification value of ``1`` by default.

    Notes:
        * The omega parameters are defined in ``GKModelConstants.OMEGA_PARAMS``.

    Args:
        omega_amp (float | Dict[str, float]): original input for ``omega_amp`` in
            ``GKModel`` class.

    Returns:
        Dict[str, float]: Dictionary of omegas mapped to their corresponding amplification
            values.
    """
    if isinstance(omega_amp, dict):
        logger.debug(
            "Some omegas have specific amplification values",
            bindings=dict(omegas=list(omega_amp.keys())),
        )
        omega_amp_map = {o: omega_amp.get(o, 1.0) for o in GKModelConstants.OMEGA_PARAMS}
    else:
        logger.debug("Setting same single omega amplification value for all omegas")
        omega_amp_map = {o: omega_amp for o in GKModelConstants.OMEGA_PARAMS}

    # Note that we disable a pytype "bad return type" error because it incorrectly thinks
    # we're returning a dictionary containing dictionaries
    return omega_amp_map  # pytype: disable=bad-return-type


def omega_prior_vectors(
    response_array: np.ndarray,
    holdout_index: int,
    level: str,
    omega_amp_map: Dict[str, float],
) -> Dict[str, float]:
    """Returns the vectors of smoothing parameters for each omega priors.

    Gets the vectors for omega priors to be calculated for a response variable as per the
    GK model process.

    Notes:
        * If there are less than 3 ages for the smoothing param, then the value for that omega
          will be 0.

    Args:
        response_array (np.ndarray): 3D response variable indexed on location, age, time
            (i.e., time in years).
        holdout_index (int): The index by which the hold outs are started.
        level (str): The level name to apply to the end of the dict key.
        omega_amp_map (Dict[str, float]): Maps each omega prior to its corresponding
            amplification, i.e, how much to scale the omega priors.

    Returns:
        Dict[str, np.array]: Dictionary of vectors to get the optimal smoothing from.
    """
    location_index, age_index, year_index = response_array.shape

    response_array = np.copy(response_array)
    response_array[:, :, holdout_index:] = np.nan
    response_array_adj = response_array - np.nanmean(response_array, axis=2).reshape(
        (location_index, age_index, 1)
    )
    response_array_adj = response_array_adj[:, :, :holdout_index]

    omega_vectors = dict()

    omega_vectors[f"omega_loc_{level}"] = (
        np.sqrt(np.abs(np.diff(response_array_adj, axis=0))) * omega_amp_map["omega_location"]
    )

    omega_vectors[f"omega_age_{level}"] = (
        np.sqrt(np.abs(np.diff(response_array_adj, axis=1))) * omega_amp_map["omega_age"]
    )

    omega_vectors[f"omega_loc_time_{level}"] = (
        np.sqrt(np.abs(np.diff(np.diff(response_array_adj, axis=2), axis=0)))
        * omega_amp_map["omega_location_time"]
    )

    omega_vectors[f"omega_age_time_{level}"] = (
        np.sqrt(np.abs(np.diff(np.diff(response_array_adj, axis=2), axis=1)))
        * omega_amp_map["omega_age_time"]
    )

    # If there are less than 3 ages for the smoothing param, then the value will be 0.
    smoothing_params = {
        k: _smoothing_param(v) if (age_index > 2 or "age" not in k) else 0
        for k, v in omega_vectors.items()
    }
    return smoothing_params


def _smoothing_param(array: np.ndarray) -> float:
    """Function to define a GK smoothing parameter given its mean and standard deviation.

    This function finds the smoothing param that optimizes the 1/rate value of the Gamma
    distribution in order to find an optimal smoothing parameter for the GK model.

    A reference for this code can be found here:
    https://github.com/IQSS/YourCast/blob/955a88043fa97d71b922585b5bcd28cc5738d75c/R/compute.sigma.R#L194-L270

    Args:
        array (np.ndarray): The one dimensional array like object to compute smoothing
            parameter over.

    Returns:
        float: The appropriate smoothing value given an array.
    """
    ma = np.ma.array(array, mask=np.isnan(array))
    weights = np.ones_like(array).cumsum(axis=2) ** 2
    m = np.ma.average(ma, weights=weights)
    std = np.sqrt(np.ma.average((ma - m) ** 2, weights=weights))
    v = std**2
    # not sure why this is calculated but its in the reference code as well
    # _ = (m**2.) / (v + m**2.)
    d = brentq(_optimize_rate, 2.0000001, 1000, args=(m, v))

    e = (d - 2) * (v + m**2)
    return d / e


def _optimize_rate(x: float, m: float, v: float) -> float:
    """Find the optimization rate corresponding to the observations.

    This function finds the optimization rate corresponding to the reciprocal-rate,
    weighted-mean, and variance of the observations.

    Args:
        x: The **positive** value of 1/rate to test.
        m: The weighted average of an array.
        v: The variance.

    Returns:
        float: The optimization rate
    """
    opr = np.sqrt((x / 2) - 1) * np.exp(gammaln((x / 2) - 0.5) - gammaln(x / 2)) - (
        m / np.sqrt(m**2 + v)
    )
    return opr
