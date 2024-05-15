"""This module contains various implementations of the intercept shift operation.
"""

import itertools as it

import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants, ScenarioConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_data_transformation.lib.draws import mean_of_draws
from fhs_lib_data_transformation.lib.validate import assert_coords_same_ignoring

logger = get_logger()


def point_intercept_shift(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    last_past_year: int,
) -> xr.DataArray:
    """Perform point intercept-shift.

    All input data should be point arrays already, so they should not
    have the draw dimension.  The modeled (future) data should have one scenario,
    and the past data should not have any.

    Args:
        modeled_data (xr.DataArray): Estimates based on FHS modeling. Expects past and
            forecast, or at least a value for the last past year and forecast years.
        past_data (xr.DataArray): Past estimates from GBD to base the shift on.
        last_past_year (int): last past year to intercept-shift on.

    Returns:
        (xr.DataArray): intercept-shifted point estimates.
    """
    if DimensionConstants.DRAW in modeled_data.dims:
        raise KeyError("draw dimension in modeled_data for point intercept-shift.")

    if DimensionConstants.DRAW in past_data.dims:
        raise KeyError("draw dimension in past_data for point intercept-shift.")

    if DimensionConstants.SCENARIO in past_data.dims:
        raise KeyError("past data should not have scenario dim.")

    # In point runs, we require ingesting future data using open_xr_scenario()
    # with designated scenario, hence future data no longer has scenario dim.
    if DimensionConstants.SCENARIO in modeled_data.dims:
        raise KeyError("future data should not have scenario during point run.")

    last_year_modeled_data = modeled_data.sel(
        **{DimensionConstants.YEAR_ID: last_past_year}, drop=True
    )

    last_year_past_data = past_data.sel(
        **{DimensionConstants.YEAR_ID: last_past_year}, drop=True
    )

    diff = last_year_modeled_data - last_year_past_data

    shifted_data = modeled_data - diff

    return shifted_data


def mean_intercept_shift(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    years: YearRange,
    shift_from_reference: bool = True,
) -> xr.DataArray:
    """Move the modeled_data up by the difference between the modeled and actual past data.

    Subtract that difference from the modeled data to obtain shifted data so that the GBD past
    and FHS forecasts line up. Mean of draws is taken if there are draws to calculate the
    offset, but it is applied to whatever draws are in the modeled_data.

    May raise IndexError if coordinates or dimensions do not match up
    between modeled data and GBD data.

    Note:
        Expects input data to have the same coordinate dimensions for age, sex, and location,
        but not year (except the one overlapping year).

    Args:
        modeled_data (xr.DataArray): Estimates based on FHS modeling. Expects past and
            forecast, or at least a value for the last past year and forecast years.
        past_data (xr.DataArray): Past estimates from GBD to base the shift on
        years (YearRange): Forecasting time series year range
        shift_from_reference (bool): Optional. Whether to shift values based on difference
            from reference scenario only, or to take difference for each scenario. In most
            cases these will yield the same results unless scenarios have different value in
            last past year than reference.

    Returns:
        xr.DataArray: Modeled data that has been shifted to line up with GBD past in the last
        year of past data.
    """
    if shift_from_reference and DimensionConstants.SCENARIO in modeled_data.dims:
        last_year_modeled_data = modeled_data.sel(
            **{
                DimensionConstants.YEAR_ID: years.past_end,
                DimensionConstants.SCENARIO: ScenarioConstants.REFERENCE_SCENARIO_COORD,
            },
            drop=True,
        )
    else:
        last_year_modeled_data = modeled_data.sel(
            **{DimensionConstants.YEAR_ID: years.past_end},
            drop=True,
        )

    mean_last_year_modeled_data = mean_of_draws(last_year_modeled_data)

    last_year_past_data = past_data.sel(
        **{DimensionConstants.YEAR_ID: years.past_end}, drop=True
    )
    mean_last_year_past_data = mean_of_draws(last_year_past_data)

    assert_coords_same_ignoring(
        mean_last_year_past_data, mean_last_year_modeled_data, ["scenario"]
    )

    diff = mean_last_year_modeled_data - mean_last_year_past_data

    shifted_data = modeled_data - diff

    return shifted_data


def ordered_draw_intercept_shift(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    past_end_year_id: int,
    modeled_order_year_id: int,
    shift_from_reference: bool = True,
) -> xr.DataArray:
    """Move the modeled_data up by the difference between the modeled and actual past data.

    May raise IndexError if coordinates or dimensions do not match up
    between modeled data and GBD data.

    Notes:
        * **Steps:**

            1) Reorder the draws for modeled-past and GBD-past by value in the last past year.
            2) Take the difference between reordered modeled-past and GBD-past in the last past
               year.
            3) Add the difference to the past and forecasted modeled-data such that reordered
               draw difference gets applied to the values of its original unordered draw
               number.

        * **Preconditions:**

            - Expects them to have the same coordinate dimensions for age, sex, and location,
              but not year (except the one overlapping year).
            - GBD-past and modeled-past have the same number of draws.
            - Draw coordinates are zero-indexed integers.

    Args:
        modeled_data (xr.DataArray): Estimates based on FHS modeling. Expects past and
            forecast, or at least a value for the last past year and forecast years.
        past_data (xr.DataArray): Past estimates from GBD to base the shift on
        past_end_year_id (int): Last year of past data from GBD
        modeled_order_year_id (int): What year to base the draw order off of for the modeled
            data. Generally is the last past year or the last forecast year.
        shift_from_reference (bool): Optional. Whether to shift values based on difference
            from reference scenario only, or to take difference for each scenario. In most
            cases these will yield the same results unless scenarios have different value in
            last past year than reference.

    Returns:
        xr.DataArray: Modeled data that has been shifted to line up with GBD past in the last
        year of past data.
    """
    # Subset GBD past and modeled values to the year they will base the draw
    # order on. Also subset modeled data to last past year.
    past_data = past_data.sel(**{DimensionConstants.YEAR_ID: past_end_year_id}, drop=True)
    if shift_from_reference and DimensionConstants.SCENARIO in modeled_data.dims:
        modeled_order_year = modeled_data.sel(
            **{
                DimensionConstants.YEAR_ID: modeled_order_year_id,
                DimensionConstants.SCENARIO: ScenarioConstants.REFERENCE_SCENARIO_COORD,
            },
            drop=True,
        )
        modeled_last_past_year = modeled_data.sel(
            **{
                DimensionConstants.YEAR_ID: past_end_year_id,
                DimensionConstants.SCENARIO: ScenarioConstants.REFERENCE_SCENARIO_COORD,
            },
            drop=True,
        )
    else:
        modeled_order_year = modeled_data.sel(
            **{DimensionConstants.YEAR_ID: modeled_order_year_id},
            drop=True,
        )
        modeled_last_past_year = modeled_data.sel(
            **{DimensionConstants.YEAR_ID: past_end_year_id},
            drop=True,
        )

    assert_coords_same_ignoring(past_data, modeled_order_year, ["scenario"])

    # Make a copy of the modeled data that will contain the shifted data.
    shifted_modeled = modeled_data.copy()

    # Keep track of the original draw number order
    original_draw_order = modeled_order_year.draw.values

    # get dimensions to loop through except for dimension draw
    non_draw_coords = modeled_order_year.drop_vars(DimensionConstants.DRAW).coords
    coords = list(non_draw_coords.indexes.values())
    dims = list(non_draw_coords.indexes.keys())

    for coord in it.product(*coords):
        logger.debug("Shifting for coord", bindings=dict(coord=coord))

        # Get coordinates of data slice to reorder draws for
        slice_dict = {dims[i]: coord[i] for i in range(len(coord))}

        # If we've got a scenario key in the slice_dict; make a copy dict without the scenario
        # key for use when subsetting our past data
        if DimensionConstants.SCENARIO in slice_dict.keys():
            non_scenario_slice_dict = slice_dict.copy()
            del non_scenario_slice_dict[DimensionConstants.SCENARIO]
        else:
            non_scenario_slice_dict = slice_dict.copy()

        # Sort the modeled values for the slice of the order year by draw
        # such that the numbering actually reflects each draw's rank (e.g. draw
        # 0 is the minimum and draw 99 is the maximum for 100 draws).
        modeled_slice = modeled_order_year.sel(**slice_dict)
        modeled_slice_draw_index = modeled_slice.coords.dims.index(DimensionConstants.DRAW)
        modeled_slice_draw_rank = (
            modeled_slice.argsort(modeled_slice_draw_index)
            .argsort(modeled_slice_draw_index)
            .values
        )

        # Also get the slice to take the difference from, since we always take
        # the difference in the last past year regardless of what year we order
        # the modeled data by.
        modeled_last_past_slice = modeled_last_past_year.sel(**slice_dict)
        modeled_last_past_slice = modeled_last_past_slice.assign_coords(
            draw=modeled_slice_draw_rank
        )

        # Sort the GBD past values for the slice of the last past year by draw
        # such that the numbering actually reflects each draw's rank (e.g. draw
        # 0 is the minimum and draw 99 is the maximum for 100 draws).
        past_slice = past_data.sel(**non_scenario_slice_dict)
        past_slice_draw_index = past_slice.coords.dims.index(DimensionConstants.DRAW)
        past_slice_draw_rank = (
            past_slice.argsort(past_slice_draw_index).argsort(past_slice_draw_index).values
        )
        past_slice = past_slice.assign_coords(draw=past_slice_draw_rank)

        # Take difference of sorted GBD past and sorted modeled past (again,
        # just for last year of past).
        diff = past_slice - modeled_last_past_slice

        # Reassign the draw numbers of the modeled data to be consistent with
        # the sorted draw values for the order year of the modeled data.
        # Now add the difference to shift this modeled data for all years.
        modeled_all_years_slice = modeled_data.sel(**slice_dict)
        modeled_all_years_slice = modeled_all_years_slice.assign_coords(
            draw=modeled_slice_draw_rank
        )
        shifted_slice = modeled_all_years_slice + diff

        # Now reassign the draw numbers of the **shifted**-modeled data
        # **again** to their original numbers. That is, at this point draw 0
        # for the last past year may not be the minimum value.
        shifted_slice = shifted_slice.assign_coords(draw=original_draw_order)
        shifted_modeled.loc[slice_dict] = shifted_slice

    return shifted_modeled


def unordered_draw_intercept_shift(
    modeled_data: xr.DataArray,
    past_data: xr.DataArray,
    past_end_year_id: int,
    shift_from_reference: bool = True,
) -> xr.DataArray:
    """Move the modeled_data up by the difference between the modeled and actual past data.

    Move the modeled_data up by the last-year difference between the modeled and actual past
    data.

    May raise IndexError: If coordinates or dimensions do not match up
    between modeled data and GBD data.

    Notes:
        * **Steps:**

            1) Take the difference between modeled data and GBD-past in the last past year.
            2) Add the difference to the past and forecasted modeled-data such that reordered
               draw difference gets applied to the values of its original unordered draw
               number.
        * **Preconditions:**

            - Expects them to have the same coordinate dimensions for age, sex, and location,
              but not year (except the one overlapping year).
            - GBD-past and modeled-past have the same number of draws.
            - Draw coordinates are zero-indexed integers.

    Args:
        modeled_data (xr.DataArray): Estimates based on FHS modeling. Expects past and
            forecast, or at least a value for the last past year and forecast years.
        past_data (xr.DataArray): Past estimates from GBD to base the shift on.
        past_end_year_id (int): Last year of past data from GBD.
        shift_from_reference (bool): Optional. Whether to shift values based on difference
            from reference scenario only, or to take difference for each scenario. In most
            cases these will yield the same results unless scenarios have different value in
            last past year than reference.

    Returns:
        xr.DataArray: Modeled data that has been shifted to line up with GBD past in the last
        year of past data.
    """
    # Subset GBD past and modeled data to last past year.
    past_data = past_data.sel(**{DimensionConstants.YEAR_ID: past_end_year_id}, drop=True)

    if shift_from_reference and DimensionConstants.SCENARIO in modeled_data.dims:
        modeled_last_past_year = modeled_data.sel(
            **{
                DimensionConstants.YEAR_ID: past_end_year_id,
                DimensionConstants.SCENARIO: ScenarioConstants.REFERENCE_SCENARIO_COORD,
            },
            drop=True,
        )
    else:
        modeled_last_past_year = modeled_data.sel(
            **{DimensionConstants.YEAR_ID: past_end_year_id},
            drop=True,
        )

    assert_coords_same_ignoring(past_data, modeled_last_past_year, ["scenario"])

    diff = modeled_last_past_year - past_data

    shifted_modeled = modeled_data - diff

    return shifted_modeled


def max_fanout_intercept_shift(
    modeled_data: xr.DataArray, past_data: xr.DataArray, years: YearRange
) -> xr.DataArray:
    """Ordered-draw intercept-shift based on fan-out trajectories.

    Differs from ordered_draw_intercept_shift, in that trajectories are determined
    by the difference between last forecast year and last past year.

    The function maximizes fan-out via
    1.) ranking the future trajectories and the last past year's values
    2.) connecting the top last past year value to the top trajectry, 2nd to 2nd, etc.

    Assumes there's no scenario dimension.  SHOULD ONLY BE DONE IN NORMAL SPACE,
    never in any non-linear transformation.

    Args:
        modeled_data (xr.DataArray): Estimates based on FHS modeling. Expects past and
            forecast, or at least a value for the last past year and forecast years.
        past_data (xr.DataArray): Past estimates from GBD to base the shift on.
        years (YearRange): first past year:first future year:last future year.

    Returns:
        (xr.DataArray): ordered-draw intercept-shifted with maximum fan-out.
    """
    assert_coords_same_ignoring(
        past_data.sel(year_id=years.past_end, drop=True),
        modeled_data.sel(year_id=years.past_end, drop=True),
        [],
    )

    shifted_modeled = modeled_data.copy()  # this will be modified in place for return

    # we determine draw sort order by trajectory = last year - last past year
    trajectories = modeled_data.sel(year_id=years.forecast_end) - modeled_data.sel(
        year_id=years.past_end
    )  # no more year_id dim

    # these are coords after removing year and draw dims
    non_draw_coords = trajectories.drop_vars(DimensionConstants.DRAW).coords
    coords = list(non_draw_coords.indexes.values())
    dims = list(non_draw_coords.indexes.keys())

    for coord in it.product(*coords):
        slice_dict = {dims[i]: coord[i] for i in range(len(coord))}

        trajs = trajectories.sel(**slice_dict)  # should have only draw dim now
        # using argsort once gives the indices that sort the list,
        # using it twice gives the rank of each value, from low to high
        traj_rank = trajs.argsort().argsort().values

        # from now on we will do some calculations in "rank space".
        # ranked_future has year_id/rank dims.
        ranked_future = (
            modeled_data.sel(**slice_dict)
            .rename({DimensionConstants.DRAW: "rank"})
            .assign_coords(rank=traj_rank)
        )
        # each value is labeled by its trajectory rank now, with 0 being the lowest rank.
        predicted_last_past_rank = ranked_future.sel(
            year_id=years.past_end
        )  # 1-D: value is predicted last past year's value, label is future trajectory rank

        # our goal is to allocate the highest trajectory rank to the highest
        # observed last past rank, and lowest to lowest, etc.
        # so we need to bring the last observed year into rank space as well.
        observed_last_past = past_data.sel(**slice_dict, year_id=years.past_end)  # draws only
        # save the past draw-labels, ordered by rank, so we can convert from rank to draw later
        past_draw_order = observed_last_past[DimensionConstants.DRAW].values[
            observed_last_past.argsort()
        ]

        # get the rank of the past values
        past_rank = observed_last_past.argsort().argsort().values

        # 1-D: value is gbd last past year's value, label is value's rank.
        observed_last_past_rank = observed_last_past.rename(
            {DimensionConstants.DRAW: "rank"}
        ).assign_coords(rank=past_rank)

        # Now observed_last_past_rank and predicted_last_past_rank are both 1-D arrays
        # with a "rank" dimension, where observed_last_past_rank is ranked by value,
        # and predicted_last_past_rank is ranked by future trajectory.
        # The goal now is then to attach the top-ranked future trajectory to the top-ranked
        # past value, 2nd-highest to 2nd-highest, and so on.
        diff = observed_last_past_rank - predicted_last_past_rank  # the diff to shift

        # this is a safeguard in case xarray changes its default arithmetic behavior
        if not (diff["rank"] == observed_last_past_rank["rank"]).all():
            raise ValueError("diff must inherit rank values from observed_last_past_rank")

        # diff added to the future draws in rank space.
        ranked_future = diff + ranked_future  # has year_id / rank dims
        # with this, the top-ranked future trajectory now has the same last past year
        # value as that of top-ranked gbd last past year value, completing the shift.

        # another safeguard
        if not (ranked_future["rank"] == diff["rank"]).all():
            raise ValueError("ranked_future must inherit rank values from diff")

        # Now need to convert back to draw space.
        # ranked_future is now in rank space and labeled by ranks of gbd last past year values,
        # and we want to give it back its original draw labels.
        # We've saved the rank-order of the draws in the past, so if we sort by rank, then
        # the order of the ranks is now the order of the draws in `past_draw_order`, so we can
        # apply that variable as the draws
        ranked_future = (
            ranked_future.sortby("rank")
            .rename({"rank": DimensionConstants.DRAW})
            .assign_coords(draw=past_draw_order)
        )
        ranked_future = ranked_future.sortby("draw")

        # prep the shape of ranked_future before inserting into modeled_data
        dim_order = modeled_data.sel(**slice_dict).dims
        ranked_future = ranked_future.transpose(*dim_order)

        # modify in-place
        shifted_modeled.loc[slice_dict] = ranked_future  # ~ "re_aligned_future"

    return shifted_modeled
