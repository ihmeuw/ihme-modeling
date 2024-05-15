"""This module contains functions for attenuating or removing the drift effect from epsilon.

i.e., residual error, predictions.

This is intended to be used in relation with latent trend models such as ARIMA and Random Walk.
"""

from typing import Tuple

import numpy as np
import statsmodels.api as sm
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()


def get_decayed_drift_preds(
    epsilon_da: xr.DataArray,
    years: YearRange,
    decay: float,
) -> xr.DataArray:
    """Generates attenuated drift predictions.

    Generates attenuated drift predictions for each demographic combo in the input dataset,
    excluding the ``"year_id"`` dimension.

    Args:
        epsilon_da (xr.DataArray): Dataarray with values to fit and remove the drift from.
        years (YearRange): The forecasting time series year range.
        decay (float): Rate at which the slope of the line decays once forecasts start.

    Returns:
        xr.DataArray: Predictions for past and future years -- linear regression for past
            years, and attenuated drift for future years. Predictions are for every demographic
            combo available in the input dataarray.
    """
    # Find the right linear regression parameters to fit the in-sample data to.
    param_ds = _get_all_lr_params(da=epsilon_da)

    # Get the right shape for the prediction dataarray - fill with nans
    year_shape_da = xr.DataArray(
        np.ones(len(years.years)),
        coords=[years.years],
        dims=[DimensionConstants.YEAR_ID],
    )
    pred_da = param_ds["alpha"] * year_shape_da * np.nan

    # Iterate through the demographic combos, and populating the dataarray with predictions for
    # each combo.
    for sex_id in pred_da[DimensionConstants.SEX_ID].values:
        for age_group_id in pred_da[DimensionConstants.AGE_GROUP_ID].values:
            for location_id in pred_da[DimensionConstants.LOCATION_ID].values:
                slice_coord_dict = {
                    DimensionConstants.SEX_ID: sex_id,
                    DimensionConstants.AGE_GROUP_ID: age_group_id,
                    DimensionConstants.LOCATION_ID: location_id,
                }
                alpha = param_ds["alpha"].loc[slice_coord_dict].values
                beta = param_ds["beta"].loc[slice_coord_dict].values

                pred_da.loc[slice_coord_dict] = _make_single_predictions(
                    alpha=alpha,
                    beta=beta,
                    years=years,
                    decay=decay,
                )

    return pred_da


def _get_all_lr_params(da: xr.DataArray) -> xr.Dataset:
    """Fits a linear regression to each demographic time series slice.

    Iterates through all demographic time series slices in an input dataarray, fitting a linear
    regression to each. The parameters of the linear regression fit are returned in a dataset.

    Args:
        da (xr.DataArray): n-dimensional dataarray with ``"year_id"`` as a dimension.

    Returns:
        xr.Dataset: n-1 dimensional xarray containing the intercept, i.e., alpha, and slope,
            i.e., beta, for each combination of coords in ``da`` excluding ``"year_id"``.
    """
    # Create a dataset to store parameters - fill with nans to start, and get rid of
    # ``"year_id"`` dimension.
    param_da = da.sel(**{DimensionConstants.YEAR_ID: da.year_id.values[0]}, drop=True) * np.nan
    param_ds = xr.Dataset({"alpha": param_da.copy(), "beta": param_da.copy()})

    # fit linear regression by location-age-sex
    for sex_id in da[DimensionConstants.SEX_ID].values:
        for age_group_id in da[DimensionConstants.AGE_GROUP_ID].values:
            for location_id in da[DimensionConstants.LOCATION_ID].values:
                slice_coord_dict = {
                    DimensionConstants.SEX_ID: sex_id,
                    DimensionConstants.AGE_GROUP_ID: age_group_id,
                    DimensionConstants.LOCATION_ID: location_id,
                }
                ts = da.loc[slice_coord_dict]
                ts = ts.squeeze()
                alpha, beta = _get_lr_params(ts)

                param_ds["alpha"].loc[slice_coord_dict] = alpha
                param_ds["beta"].loc[slice_coord_dict] = beta

    return param_ds


def _get_lr_params(ts: xr.DataArray) -> Tuple[float, float]:
    """Fits a linear regression to the given time series.

    Fits a linear regression to an input time series and returns the fit params.

    Args:
        ts (xr.DataArray): 1D time series of epsilons.

    Returns:
        Tuple[float, float]: the intercept and slope of the time series
    """
    xdata = np.arange(len(ts))
    xdata = sm.add_constant(xdata)
    model = sm.OLS(ts.values, xdata).fit()
    alpha, beta = model.params

    return alpha, beta


def _make_single_predictions(
    alpha: float,
    beta: float,
    years: YearRange,
    decay: float,
) -> np.ndarray:
    r"""Generates predictions of the form y = alpha + beta*time.

    These predictions are generated for the years in years.past_years (linear regression
    predictions), then attenuates the slope that is added for each year after as the following.

    .. math::

        y_{t+1} = y_t + \beta * \exp( -\text{decay} * \text{time-since-holdout} )

    for each year in the future.

    Args:
        alpha (float): intercept for linear regression
        beta (float): slope for linear regression
        years (YearRange): years to fit and forecast over
        decay (float): rate at which the slope of the line decays once
            forecasts start

    Returns:
        numpy.array: the predictions generated by the input parameters and years
    """
    linear_years = np.arange(len(years.past_years))

    # First, create linear predictions for past years.
    predictions = alpha + (beta * linear_years)

    # Then add the decay year-by-year for future years.
    last = predictions[-1]
    for year_index in range(len(years.forecast_years)):
        current = last + (beta * np.exp(-decay * year_index))

        logger.debug(
            "Applying decay to future year",
            bindings=dict(year_index=year_index, prediction=current),
        )

        predictions = np.append(predictions, current)
        last = current

    return predictions
