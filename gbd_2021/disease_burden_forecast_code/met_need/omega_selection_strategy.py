"""Strategies for determining the weight for the Annualized Rate-of-Change (ARC) method.

Find where the RMSE is 1 (the RMSE is normalized so that 1 is always the lowest
RMSE). If there are ties, take the lowest weight.

There two options for choosing the weight:
1) Use the weight where the normalized-RMSE is 1.
2) If none of the weights have a normalized-RMSE no more than the
"""

from typing import Any

import numpy as np
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import ArcMethodConstants

logger = get_logger()


def use_omega_with_lowest_rmse(rmse: xr.DataArray, **kwargs: Any) -> float:
    """Use the omega (weight) with the lowest RMSE.

    If there are ties, choose the smallest omega.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        The weight to use for the ARC method.
    """
    chosen_weight = rmse.where(rmse == rmse.min()).dropna("weight")["weight"].values[0]

    logger.debug(f"`use_omega_with_lowest_rmse` weight selected: {chosen_weight}")
    return chosen_weight


def use_smallest_omega_within_threshold(
    rmse: xr.DataArray, threshold: float = 0.05, **kwargs: Any
) -> float:
    """Returns the smallest omega possible compared to normalized-RMSE, using a threshold.

    If none of the weights have a normalized-RMSE (normalized by dividing by
    minimum RMSE) no more than the threshold percent greater than the minimum
    normalized-RMSE, which will be 1, then the weight of 0.0 is used.
    Otherwise, starting at the first weight smaller than the weight of the
    minimum normalized-RMSE and moving in the direction of decreasing weights,
    choose the first weight that is more than the threshold percent greater
    than the minimum normalized-RMSE.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        threshold:
            The threshold percent to use for selecting the weight.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        The weight to use for the ARC method.
    """
    norm_rmse = rmse / rmse.min()

    diffs = norm_rmse - 1

    # If there are, then the set the weight to the first weight with an
    # normalized-RMSE less than threshold percent above the minimum
    # normalized-RMSE.
    weight_with_lowest_rmse = (
        norm_rmse.where(norm_rmse == norm_rmse.min()).dropna("weight")["weight"].values[0]
    )
    weights_to_check = [w for w in norm_rmse["weight"].values if w < weight_with_lowest_rmse]
    diffs_to_check = diffs.sel(weight=weights_to_check)
    diffs_greater = diffs_to_check.where(diffs_to_check >= threshold).dropna("weight")
    if len(diffs_greater) > 0:
        # take the max weight greater than the threshold but less than the
        # with the lowest RMSE.
        chosen_weight = diffs_greater["weight"].values.max()
    else:
        chosen_weight = 0.0

    logger.debug(f"`use_smallest_omega_within_threshold` weight selected: {chosen_weight}")
    return chosen_weight


def use_omega_rmse_weighted_average(rmse: xr.DataArray, **kwargs: Any) -> float:
    r"""Use the RMSE-weighted average of the range of tested-omegas.

    .. math::

        \bar{\omega} = \frac{\sum\limits_{i=0}^{N}\frac{\omega_i}{RMSE_i}}
        {\sum\limits_{i=0}^{N}\frac{1}{RMSE_i}}

    where :math:`N` is the largest in the range of omegas that were tested.

    *Note* under the special case when one or more of the weights has an RMSE
    of 0, we consider any weights with RMSE values of zero, to be weighted
    infinitely, so we just take the mean of all the weights with an RMSE of
    zero.

    Args:
        rmse (xarray.DataArray):
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        The omega to use for the ARC method.
    """
    zero_rmse = rmse == 0
    if zero_rmse.any():
        # Any weights with RMSE values of zero, will be weighted infinitely so
        # just take the mean of all the weights with an RMSE of zero.
        chosen_weight = float(rmse["weight"].where(zero_rmse).dropna("weight").mean("weight"))
    else:
        chosen_weight = float((rmse["weight"] / rmse).sum() / (1 / rmse).sum().values)

    logger.debug(f"`use_omega_rmse_weighted_average` weight selected: {chosen_weight}")
    return chosen_weight


def use_average_omega_within_threshold(
    rmse: xr.DataArray, threshold: float = 0.05, **kwargs: Any
) -> float:
    """Take the average of the omegas with RMSEs within 5% of lowest RMSE.

    Args:
        rmse (xarray.DataArray):
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        threshold (float):
            The threshold percent to use for selecting the weight.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        The weight to use for the ARC method.
    """
    chosen_weight = (
        rmse.where(rmse < rmse.values.min() + rmse.values.min() * threshold)
        .dropna("weight")["weight"]
        .values.mean()
    )

    logger.debug(f"`use_average_omega_within_threshold` weight selected: {chosen_weight}")
    return chosen_weight


def use_average_of_zero_biased_omegas_within_threshold(
    rmse: xr.DataArray, threshold: float = 0.05, **kwargs: Any
) -> float:
    """Calculates weight by averaging omegas.

    Take the average of the omegas less than the omega with the lowest RMSE,
    and with RMSEs within 5% of that lowest RMSE.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        threshold:
            The threshold percent to use for selecting the weight.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        The weight to use for the ARC method.
    """
    norm_rmse = rmse / rmse.min()

    weight_with_lowest_rmse = (
        norm_rmse.where(norm_rmse == norm_rmse.min()).dropna("weight")["weight"].values[0]
    )
    weights_to_check = [w for w in norm_rmse["weight"].values if w <= weight_with_lowest_rmse]

    rmses_to_check = norm_rmse.sel(weight=weights_to_check)
    rmses_to_check_within_threshold = rmses_to_check.where(
        rmses_to_check < 1 + threshold
    ).dropna("weight")

    chosen_weight = rmses_to_check_within_threshold["weight"].values.mean()

    logger.debug(
        (
            "`use_average_of_zero_biased_omegas_within_threshold` weight selected: "
            f"{chosen_weight}"
        )
    )
    return chosen_weight


def use_omega_distribution(
    rmse: xr.DataArray, draws: int, threshold: float = 0.05, **kwargs: Any
) -> xr.DataArray:
    """Samples omegas from a distribution (using RMSE).

    Takes the omegas with RMSEs within the threshold percent of omega with
    the lowest RMSE, and takes the reciprocal RMSEs of those omegas as the
    probabilities of omegas being sampled from multinomial a distribution.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        draws:
            The number of draws to sample from the distribution of omega values
        threshold:
            The threshold percent to use for selecting the weight.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        Samples from a distribution of omegas to use for the ARC method.
    """
    rmses_in_threshold = rmse.where(rmse < rmse.values.min() + rmse.values.min() * threshold)
    reciprocal_rmses_in_threshold = (1 / rmses_in_threshold).fillna(0)
    norm_reciprocal_rmses_in_threshold = (
        reciprocal_rmses_in_threshold / reciprocal_rmses_in_threshold.sum()
    )

    omega_draws = xr.DataArray(
        np.random.choice(
            a=norm_reciprocal_rmses_in_threshold["weight"].values,
            size=draws,
            p=norm_reciprocal_rmses_in_threshold.values,
        ),
        coords=[list(range(draws))],
        dims=[DimensionConstants.DRAW],
    )
    return omega_draws


def use_zero_biased_omega_distribution(
    rmse: xr.DataArray, draws: int, threshold: float = 0.05, **kwargs: Any
) -> xr.DataArray:
    """Samples omegas from a distribution (using RMSE).

    Takes the omegas with RMSEs within the threshold percent of omega with
    the lowest RMSE, and takes the reciprocal RMSEs of those omegas as the
    probabilities of omegas being sampled from multinomial a distribution.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        draws:
            The number of draws to sample from the distribution of omega values
        threshold:
            The threshold percent to use for selecting the weight.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        Samples from a distribution of omegas to use for the ARC method.
    """
    norm_rmse = rmse / rmse.min()

    weight_with_lowest_rmse = (
        norm_rmse.where(norm_rmse == norm_rmse.min()).dropna("weight")["weight"].values[0]
    )
    weights_to_check = [w for w in norm_rmse["weight"].values if w <= weight_with_lowest_rmse]

    rmses_to_check = norm_rmse.sel(weight=weights_to_check)
    rmses_to_check_within_threshold = rmses_to_check.where(
        rmses_to_check < 1 + threshold
    ).dropna("weight")

    reciprocal_rmses_to_check_within_threshold = (1 / rmses_to_check_within_threshold).fillna(
        0
    )
    norm_reciprocal_rmses_to_check_within_threshold = (
        reciprocal_rmses_to_check_within_threshold
        / reciprocal_rmses_to_check_within_threshold.sum()
    )

    omega_draws = xr.DataArray(
        np.random.choice(
            a=norm_reciprocal_rmses_to_check_within_threshold["weight"].values,
            size=draws,
            p=norm_reciprocal_rmses_to_check_within_threshold.values,
        ),
        coords=[list(range(draws))],
        dims=[DimensionConstants.DRAW],
    )
    return omega_draws


def adjusted_zero_biased_omega_distribution(
    rmse: xr.DataArray,
    draws: int,
    seed: int = ArcMethodConstants.DEFAULT_RANDOM_SEED,
    **kwargs: Any,
) -> xr.DataArray:
    """Samples omegas from a distribution (using RMSE).

    Takes the omegas from the lowest RMSE to zero, and takes the reciprocal
    RMSEs of those omegas as the probabilities of omegas being sampled from
    multinomial a distribution.

    Args:
        rmse:
            Array with one dimension, "weight", that contains the tested
            omegas as coordinates. The data is the RMSE (Root _Mean_ Square
            Error or Root _Median_ Square Error) values.
        draws:
            The number of draws to sample from the distribution of omega values
        seed:
            seed to be set for random number generation.
        kwargs:
            Ignores any additional keyword args.

    Returns:
        Samples from a distribution of omegas to use for the ARC method.
    """
    np.random.seed(seed)

    norm_rmse = rmse / rmse.min()

    weight_with_lowest_rmse = (
        norm_rmse.where(norm_rmse == norm_rmse.min()).dropna("weight")["weight"].values[0]
    )
    weights_to_check = [w for w in norm_rmse["weight"].values if w <= weight_with_lowest_rmse]

    rmses_to_check = norm_rmse.sel(weight=weights_to_check)

    reciprocal_rmses_to_check = (1 / rmses_to_check).fillna(0)
    norm_reciprocal_rmses_to_check = (
        reciprocal_rmses_to_check / reciprocal_rmses_to_check.sum()
    )

    omega_draws = xr.DataArray(
        np.random.choice(
            a=norm_reciprocal_rmses_to_check["weight"].values,
            size=draws,
            p=norm_reciprocal_rmses_to_check.values,
        ),
        coords=[list(range(draws))],
        dims=[DimensionConstants.DRAW],
    )
    return omega_draws
