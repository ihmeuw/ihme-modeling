"""A function to correlate residual error and modeled results.

This function correlates data of residual error, i.e., epsilon, predictions and modeled
results. The intent is to capture any time trends that remain after the explanatory variables
have been incorporated into the model.

For example, when creating the all-cause ARIMA ensemble, we take the residual error between
modeled past and GBD past and forecast that residual into future years.
"""

import itertools as it
from collections import namedtuple

import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange

from fhs_lib_data_transformation.lib.constants import CorrelateConstants


def correlate_draws(
    epsilon_draws: xr.DataArray,
    modeled_draws: xr.DataArray,
    years: YearRange,
) -> xr.DataArray:
    """Correlates draws of predicted epsilon draws with the modeled draws.

    Correlates high epsilon draws with high modeled draws (currently correlates based on first
    forecasted year) to create a confidence interval that captures both past uncertainty and
    model uncertainty. Returns the correctly ordered epsilons for future years. Time series of
    epsilons are ordered by draw in the first predicted year and made to align in rank with the
    modeled rates for each predicted year.

    Notes:
        * **Pre-condition:** ``epsilon_draws`` and ``modeled_draws`` should have each have
          ``"location_id"``, ``"age_group_id"``, ``"sex_id"``, ``"draw"``, and ``"year_id"`` as
          dims, and only those.

    Args:
        epsilon_draws (x.DataArray): Unordered draw-level epsilon results from a latent trend
            model, e.g., an ARIMA or a Random Walk model.
        modeled_draws (x.DataArray): Modeled draw-level predictions to correlate with, e.g.,
            natural-logged mortality rate draws.
        years (YearRange): The forecasting time series year range.

    Returns:
        xr.DataArray: Epsilon data now with correlated draws for all years.
    """
    # Get both dataarrays lined up in terms of dims and coords.
    epsilon_draws = epsilon_draws.transpose(*CorrelateConstants.DIM_ORDER)
    modeled_draws = modeled_draws.transpose(*CorrelateConstants.DIM_ORDER)

    # We only need the first year of forecasts for ordering.
    moded_draws_forecast_start = modeled_draws.sel(
        **{DimensionConstants.YEAR_ID: years.forecast_start}, drop=True
    )

    # Create cartesian product of all demographic combinations.
    AgeSexLocation = namedtuple("AgeSexLocation", "age sex location")
    demog_slice_combos = it.starmap(
        AgeSexLocation,
        it.product(
            epsilon_draws.age_group_id.values,
            epsilon_draws.sex_id.values,
            epsilon_draws.location_id.values,
        ),
    )

    # The index of the year to order draws by -- the last forecast year.
    order_year_index = years.forecast_end - years.past_start

    # Iterate through each demographic slice and correlate the draws for each as we go.
    for demog_slice_combo in demog_slice_combos:
        slice_coord_dict = {
            DimensionConstants.LOCATION_ID: demog_slice_combo.location,
            DimensionConstants.AGE_GROUP_ID: demog_slice_combo.age,
            DimensionConstants.SEX_ID: demog_slice_combo.sex,
        }

        modeled_slice = moded_draws_forecast_start.loc[slice_coord_dict]

        # Use ``argsort`` twice to get ranks.
        modeled_order = modeled_slice.argsort(
            modeled_slice.dims.index(DimensionConstants.DRAW)
        )
        modeled_ranks = modeled_order.argsort(
            modeled_order.dims.index(DimensionConstants.DRAW)
        )

        # Order by draw, then just take the order from the first predicted year.
        epsilon_slice = epsilon_draws.loc[slice_coord_dict]
        sorted_epsilon_slice = epsilon_slice.argsort(
            epsilon_slice.dims.index(DimensionConstants.DRAW)
        )

        epsilon_order = [
            sorted_epsilon_slice.values[i, order_year_index]
            for i in range(epsilon_slice.shape[0])
        ]

        # Apply modeled ranks to the ordered epsilons to correctly place them.
        epsilon_ordered = epsilon_slice.values[epsilon_order]
        epsilon_ordered = epsilon_ordered[modeled_ranks.values]
        epsilon_draws.load()
        epsilon_draws.loc[slice_coord_dict] = epsilon_ordered

    return epsilon_draws
