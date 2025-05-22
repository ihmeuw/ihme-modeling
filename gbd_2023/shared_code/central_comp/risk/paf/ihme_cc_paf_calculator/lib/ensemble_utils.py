from typing import Optional

import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr

ensemble_density = importr("ensemble.density")


def get_edensity(
    weights: pd.Series, mean: pd.Series, sd: pd.Series, bounds: Optional[pd.Series] = None
) -> pd.Series:
    """Wrapper for legacy get_edensity R function. Operates on inputs for a single demographic,
    with draws of exposure mean and sd.

    Returns a Series mapping draw number ("draw_0", "draw_1", etc.) to a
    dictionary of vectors that describe an ensemble density function -- i.e.
    a distribution centered on mean that is partially comprised of multiple
    analytical distributions according to the weights provided. This is also
    called a mixture distribution.

    By default the domain of the function is the 0.1th percentile -> 99.9th
    percentile of a lognormal distribution parametrized by mean and sd, but
    bounds can be used to specify a more narrow domain (but not a larger domain).

    Arguments:
        weights: Series where each label is a distribution (e.g. "gamma",
            "lnorm") and each value is a fraction/weight. The weights should sum to 1.
        mean: Series where each label is a draw number ("draw_0", "draw_1", etc.)
            and each value is a mean to center the new distribution on.
        sd: Series where each label is a draw number ("draw_0", "draw_1", etc.)
            and each value is a standard deviation of the new distribution.
        bounds: Optional Series with labels "exposure_min" and "exposure_max" (other
            labels, if any, will be ignored); values define the limits of the new
            distribution (if the exposure_min value is higher than the 0.1th
            percentile of a lognormal distribution with the given mean and sd, and/or
            if the exposure_max value is lower than 99.9th percentile of a lognormal
            distribution with the given mean and sd).

    Returns:
        Series mapping draw number to a dictionary with the following keys:
            X: domain/inputs
            FX: output/probability density at X
            XMIN: lower bound of X
            XMAX: upper bound of X
    """
    kwargs = dict()
    if bounds is not None and bounds.notna().exposure_min:
        kwargs.update({".min": float(bounds.exposure_min)})
    if bounds is not None and bounds.notna().exposure_max:
        kwargs.update({".max": float(bounds.exposure_max)})
    with (ro.default_converter + pandas2ri.converter).context():
        values = ensemble_density.get_edensity_for_draws(
            weights=weights, mean=mean, sd=sd, **kwargs
        )
    return pd.Series(dict(values))


def expand_ensemble_weights(
    ensemble_weights: pd.DataFrame, exposure: pd.DataFrame
) -> pd.DataFrame:
    """Ensemble weight demographics (locations and years) typically don't align with our
    other datasets. They shouldn't vary by location or year, and those columns should
    be expanded to cover the location/years of the exposure draws.
    """
    constant_cols = ["year_id", "location_id"]
    weights_by_loc_year = [
        subdf.reset_index(drop=True).drop(columns=constant_cols)
        for _, subdf in ensemble_weights.groupby(constant_cols)
    ]
    constant_over_loc_year = all(
        subdf.equals(weights_by_loc_year[0]) for subdf in weights_by_loc_year
    )
    if not constant_over_loc_year:
        raise RuntimeError("Found ensemble weights that vary by loc/year")
    else:
        expanded_ensemble_weights = weights_by_loc_year[0].merge(
            exposure[constant_cols].drop_duplicates(), how="cross"
        )
    return expanded_ensemble_weights
