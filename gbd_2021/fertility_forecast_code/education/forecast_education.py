"""Make a forecast and scenarios for education using the ARC method.

>>> python forecast_education.py \
    --reference-scenario mean \
    --transform logit \
    --diff-over-mean \
    --truncate \
    --truncate-quantiles 0.15 0.85 \
    --pv-version 20180917_subnat_pv \
    --forecast-version 20190917_subnat_capped \
    --past-version 20181004_met_demand_gprdraws_gbd17final \
    --gbd-round-id 5 \
    --years 1990:2018:2100 \
    --weight-strategy use_smallest_omega_within_threshold

"""
import logging

import numpy as np
import xarray as xr
from frozendict import frozendict

from fbd_core import argparse, db, etl
from fbd_core.etl import (expand_dimensions, omega_selection_strategies,
                          resample, scenarios)
from fbd_core.file_interface import FBDPath, open_xr, save_xr
from fbd_research.education import education_transform

LOGGER = logging.getLogger(__name__)

EDU_CUTOFF = 25  # no one can be educated after they are 25
MODELED_SEX_IDS = (1, 2)
MODELED_AGE_GROUP_IDS = tuple(range(2, 21)) + (30, 31, 32, 235)
REFERENCE_SCENARIO = 0
TRANSFORMATIONS = frozendict((
    ("no_transform", lambda x: x),
    ("log", np.log),
    ("logit", education_transform.normal_to_logit)
))
INVERSE_TRANSFORMATIONS = frozendict((
    ("no_transform", lambda x: x),
    ("log", education_transform.log_to_normal),
    ("logit", education_transform.logit_to_normal)
))


def get_num_years_to_shift(age_group_years_start):
    num_years_to_shift = age_group_years_start - EDU_CUTOFF
    return max(0, num_years_to_shift)


def lag_scenarios(data, years):
    """Lag scenarios by age-years so that scenarios only deviate from the
    reference in age-time pairs that have not finished education in the first
    year of the forecast.

    Args:
        data (xarray.DataArray):
            data that must include forecasts and can include past.
        years (YearRange):
            forecasting timeseries
    Returns:
        (xarray.DataArray):
            Forecasts with adjusted/lagged scenarios.
    """
    age_groups = db.get_ages()
    age_group_dict = dict(zip(age_groups["age_group_id"],
                              age_groups["age_group_years_start"]))

    forecast = data.sel(year_id=years.forecast_years)

    adjusted_scenarios = []
    for age_group_id in forecast["age_group_id"].values:
        age_group_years_start = age_group_dict[age_group_id]
        num_years_to_shift = get_num_years_to_shift(age_group_years_start)
        age_slice = forecast.sel(age_group_id=age_group_id)

        ref_age_slice = age_slice.sel(scenario=REFERENCE_SCENARIO, drop=True)
        adjusted_scen_age_slice = []
        for scenario in (-1, 1):
            scen_age_slice = age_slice.sel(scenario=scenario).drop(
                "scenario")

            diff = scen_age_slice - ref_age_slice
            shifted_diff = diff.shift(year_id=num_years_to_shift).fillna(0)
            shifted_scen_slice = shifted_diff + ref_age_slice
            shifted_scen_slice["scenario"] = scenario
            adjusted_scen_age_slice.append(shifted_scen_slice)
        ref_age_slice["scenario"] = 0
        adjusted_scen_age_slice.append(ref_age_slice)
        adjusted_scen_age_slice = xr.concat(adjusted_scen_age_slice,
                                            dim="scenario")
        adjusted_scenarios.append(adjusted_scen_age_slice)
    adjusted_scenarios = xr.concat(adjusted_scenarios, dim="age_group_id")

    return adjusted_scenarios.combine_first(data)


def arc_forecast_education(
        past, gbd_round_id, transform, weight_exp, years,
        reference_scenario, diff_over_mean, truncate, truncate_quantiles,
        replace_with_mean, extra_dim=None):
    """Forecasts education using the ARC method.

    Args:
        past (xarray.DataArray):
            Past data with dimensions ``location_id``, ``sex_id``,
            ``age_group_id``, ``year_id``, and ``draw``.
        transform (xarray.DataArray):
            Space to transform education to for forecasting.
        weight_exp (float):
            How much to weight years based on recency
        years (YearRange):
            Forecasting timeseries.
        reference_scenario (str):
            If 'median' then the reference scenarios is made using the
            weighted median of past annualized rate-of-change across all
            past years, 'mean' then it is made using the weighted mean of
            past annualized rate-of-change across all past years
        diff_over_mean (bool):
            If True, then take annual differences for means-of-draws, instead
            of draws.
        truncate (bool):
            If True, then truncates the dataarray over the given dimensions.
        truncate_quantiles (object, optional):
            The tuple of two floats representing the quantiles to take
        replace_with_mean (bool, optional):
            If True and `truncate` is True, then replace values outside of the
            upper and lower quantiles taken across "location_id" and "year_id"
            and with the mean across "year_id", if False, then replace with the
            upper and lower bounds themselves.
        gbd_round_id (int):
            The GBD round of the input data.
    Returns:
        (xarray.DataArray):
            Education forecasts
    """
    LOGGER.debug("diff_over_mean:{}".format(diff_over_mean))
    LOGGER.debug("truncate:{}".format(truncate))
    LOGGER.debug("truncate_quantiles:{}".format(truncate_quantiles))
    LOGGER.debug("replace_with_mean:{}".format(replace_with_mean))
    LOGGER.debug("reference_scenario:{}".format(reference_scenario))

    most_detailed_coords = _get_avail_most_detailed_coords(past, gbd_round_id)
    most_detailed_past = past.sel(**most_detailed_coords)

    zeros_dropped = most_detailed_past.where(most_detailed_past > 0)
    for dim in zeros_dropped.dims:
        zeros_dropped = zeros_dropped.dropna(dim=dim, how="all")

    LOGGER.debug("Transforming the past to {}-space".format(transform))
    transformed_past = TRANSFORMATIONS[transform](zeros_dropped)

    LOGGER.debug("Forecasting education in the transformed space")
    transformed_forecast = scenarios.arc_method(
        transformed_past, gbd_round_id=gbd_round_id, years=years,
        reference_scenario=reference_scenario, weight_exp=weight_exp,
        diff_over_mean=diff_over_mean, truncate=truncate,
        truncate_quantiles=truncate_quantiles,
        replace_with_mean=replace_with_mean, reverse_scenarios=True,
        extra_dim=extra_dim, scenario_roc="national")

    LOGGER.debug("Converting the forecasts to normal/identity space")
    forecast = INVERSE_TRANSFORMATIONS[transform](transformed_forecast)

    refilled_forecast = etl.expand_dimensions(forecast, **most_detailed_coords)
    lagged_scenarios = lag_scenarios(refilled_forecast, years)

    # Since past does get clipped to avoid infs and negative infs, we need to
    # append the actual past onto the data being saved (modelers currently
    # expect the past to be there)
    past_broadcast_scenarios = etl.expand_dimensions(
        most_detailed_past, scenario=lagged_scenarios["scenario"])
    all_data = past_broadcast_scenarios.combine_first(lagged_scenarios)

    bound_err_msg = "the forecasts have NaNs"
    assert not np.isnan(all_data).any(), bound_err_msg
    if np.isnan(all_data).any():
        LOGGER.error(bound_err_msg)
        raise RuntimeError(bound_err_msg)

    return all_data


def forecast_edu_main(transform, past_version, forecast_version, pv_version,
                      weight_strategy, gbd_round_id, years, reference_scenario,
                      diff_over_mean, truncate, truncate_quantiles,
                      replace_with_mean, draws, **kwargs):
    LOGGER.debug("weight strategy: {}".format(weight_strategy.__name__))
    pv_path = FBDPath("".format())  # Path removed for security reasons
    rmse = open_xr(pv_path / "education_arc_weight_rmse.nc").data
    weight_exp = weight_strategy(rmse, draws)
    LOGGER.info("omega selected: {}".format(weight_exp))

    LOGGER.debug("Reading in the past")
    past_path = FBDPath("".format())  # Path removed for security reasons
    past = resample(open_xr(past_path / "education.nc").data, draws)
    past = past.sel(year_id=years.past_years)

    if isinstance(weight_exp, float) or isinstance(weight_exp, int):
        extra_dim = None
    else:
        if not isinstance(weight_exp, xr.DataArray):
            omega_exp_err_msg = (
                "`omega` must be either a float, an int, or an "
                "xarray.DataArray")
            LOGGER.error(omega_exp_err_msg)
            raise RuntimeError(omega_exp_err_msg)
        elif len(weight_exp.dims) != 1 or "draw" not in weight_exp.dims:
            omega_exp_err_msg = (
                "If `omega` is a xarray.DataArray, then it must have only "
                "1 dim, `draw`")
            LOGGER.error(omega_exp_err_msg)
            raise RuntimeError(omega_exp_err_msg)
        elif not weight_exp["draw"].equals(past["draw"]):
            omega_err_msg = (
                "If `omega` is a xarray.DataArray, then it's `draw` dim "
                "must have the coordinates as `past`")
            LOGGER.error(omega_err_msg)
            raise RuntimeError(omega_err_msg)
        else:
            extra_dim = "draw"

    forecast = arc_forecast_education(
        past, gbd_round_id, transform, weight_exp, years,
        reference_scenario, diff_over_mean, truncate, truncate_quantiles,
        replace_with_mean, extra_dim=extra_dim)

    forecast_path = FBDPath("".format())
    if isinstance(weight_exp, xr.DataArray):
        report_omega = float(weight_exp.mean())
    else:
        report_omega = weight_exp
    save_xr(forecast, forecast_path / "education.nc", metric="number",
            space="identity", omega=report_omega,
            omega_strategy=weight_strategy.__name__)
    LOGGER.info("education forecasts have saved")


def _get_avail_most_detailed_coords(data, gbd_round_id):
    location_table = db.get_location_set(gbd_round_id)

    # subset to national and subnat location ids
    modeled_location_ids = list(location_table["location_id"].unique())
    avail_sex_ids = [
        sex for sex in data["sex_id"].values if sex in MODELED_SEX_IDS]
    avail_age_group_ids = [
        age for age in data["age_group_id"].values
        if age in MODELED_AGE_GROUP_IDS]
    return {
        "location_id": modeled_location_ids,
        "sex_id": avail_sex_ids,
        "age_group_id": avail_age_group_ids
    }


if __name__ == "__main__":
    def get_weight_strategy_func(weight_strategy_name):
        weight_strategy_func = getattr(
            omega_selection_strategies, weight_strategy_name)
        return weight_strategy_func

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        "--reference-scenario", type=str, choices=["median", "mean"],
        help=("If 'median' then the reference scenarios is made using the "
              "weighted median of past annualized rate-of-change across all "
              "past years, 'mean' then it is made using the weighted mean of "
              "past annualized rate-of-change across all past years."))
    parser.add_argument(
        "--diff-over-mean", action="store_true",
        help=("If True, then take annual differences for means-of-draws, "
              "instead of draws."))
    parser.add_argument(
        "--truncate", action="store_true",
        help=("If True, then truncates the dataarray over the given "
              "dimensions."))
    parser.add_argument(
        "--truncate-quantiles", type=float, nargs="+",
        help="The tuple of two floats representing the quantiles to take.")
    parser.add_argument(
        "--replace-with-mean", action="store_true",
        help=("If True and `truncate` is True, then replace values outside of "
              "the upper and lower quantiles taken across `location_id` and "
              "`year_id` and with the mean across `year_id`, if False, then "
              "replace with the upper and lower bounds themselves."))
    parser.add_argument(
        "--transform", type=str,
        choices=list(sorted(TRANSFORMATIONS.keys())),
        help="Space to transform education to for forecasting.")
    parser.add_argument(
        "--forecast-version", type=str, required=True,
        help="Version of education forecasts being made and saved now")
    parser.add_argument(
        "--past-version", type=str, required=True,
        help="Version of past education")
    parser.add_argument(
        "--pv-version", type=str, required=True,
        help=("Version of predictive validation done on a range of weights "
              "used in the ARC method"))
    parser.add_argument(
        "--weight-strategy", type=get_weight_strategy_func, required=True,
        help="How the weight used in the ARC method is selected.")
    parser.add_argument(
        "--gbd-round-id", type=int, required=True)
    parser.add_arg_years(required=True)
    parser.add_arg_draws(required=True)

    args = parser.parse_args()

    forecast_edu_main(**args.__dict__)
