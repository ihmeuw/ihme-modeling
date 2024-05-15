"""This script forecasts an entity using the ARC method.

This script runs predictive validity for a entity to determine the weight used
in the arc quantile method. For each entity, this script forecasts the out
of sample window using different weights. Then determines the rmse and bias for
that weight. The output of this script are netCDFs containing the results of
the predictive metrics for each entity.

After the predictive validity step an omega will be selected to forecast each
entity

Notes regarding the truncate/capping optional flags:

1.) truncate-quantiles are used for winsorizing only the past logit
    age-standardized data (before computing the annualized rate of change).
    In case of 0.025/0.975 quantiles (calculated across locations),
    the data above the 97.5th percentile set to the 97.5th percentile wherein
    the data below the 2.5th percentile set to the 2.5th percentile.

2.) cap-quantiles are used for winsorizing only the future entities
    (after generating forecasted entities). In case of 0.01/0.99 quantiles
    (calculated based on the past entities), the forecasts above the 99th
    percentile set to the 99th percentile wherein the forecasts below the
    1st percentile set to the 1st percentile.
"""

import gc
from typing import List, Optional, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib import filter
from fhs_lib_database_interface.lib.constants import SexConstants
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_model.lib.arc_method import arc_method
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_lib_genem.lib import predictive_validity as pv
from fhs_lib_genem.lib.constants import (
    FileSystemConstants,
    ModelConstants,
    SEVConstants,
    TransformConstants,
)

logger = fhs_logging.get_logger()


def determine_entity_name_path(entity: str, stage: str) -> Tuple[str, str]:
    """Take the entity name and determine name and file path."""
    if stage == "sev" and "-" in entity:  # is an iSEV, specified as cause-risk
        acause, rei = entity.split("-")
        sub_folder = "risk_acause_specific"
        file_name = "_".join([acause, rei, SEVConstants.INTRINSIC_SEV_FILENAME_SUFFIX])
    else:
        sub_folder = ""
        file_name = f"{entity}"

    return sub_folder, file_name


def _clip_past(past_mean: xr.DataArray, transform: str) -> xr.DataArray:
    if transform == "logit":
        # it makes sense to ceiling logit-transformable data (since its 0-1)
        clipped_past = past_mean.clip(min=ModelConstants.FLOOR, max=1 - ModelConstants.FLOOR)
    elif transform == "log":
        # log transformable data should only be floored
        clipped_past = past_mean.clip(min=ModelConstants.FLOOR)
    else:
        # data we won't transform shouldn't be clipped.
        clipped_past = past_mean
    return clipped_past


def _find_limits(
    past_age_std_mean: xr.DataArray,
    past_last_year: xr.DataArray,
    upper_quantile: float,
    lower_quantile: float,
) -> xr.DataArray:
    """Find upper/lower limits to cap the forecasts."""
    past_age_std_quantiles = past_age_std_mean.quantile(
        [lower_quantile, upper_quantile], dim=["location_id", "year_id"]
    )
    upper = past_age_std_quantiles.sel(quantile=upper_quantile, drop=True)
    lower = past_age_std_quantiles.sel(quantile=lower_quantile, drop=True)

    past_last_year_gt_upper = past_last_year.where(past_last_year > upper)
    past_last_year_lt_lower = past_last_year.where(past_last_year < lower)

    upper_cap_lims = past_last_year_gt_upper.fillna(upper).rename("upper")
    lower_cap_lims = past_last_year_lt_lower.fillna(lower).rename("lower")

    cap_lims = xr.merge([upper_cap_lims, lower_cap_lims])
    return cap_lims


def _reshape_bound(data: xr.DataArray, bound: xr.DataArray) -> xr.DataArray:
    """Broadcast and align the dims of `bound` so that they match `data`."""
    expanded_bound, _ = xr.broadcast(bound, data)
    return expanded_bound.transpose(*data.coords.dims)


def _cap_forecasts(
    years: YearRange,
    cap_quantiles: Tuple[float, float],
    most_detailed_past: xr.DataArray,
    past_mean: xr.DataArray,
    forecast: xr.DataArray,
) -> xr.DataArray:
    """Cap upper and lower bound on forecasted data, using quantiles from past data."""
    last_year = most_detailed_past.sel(year_id=years.past_end, drop=True)
    lower_quantile, upper_quantile = cap_quantiles
    caps = _find_limits(
        past_mean, last_year, upper_quantile=upper_quantile, lower_quantile=lower_quantile
    )
    returned_past = forecast.sel(year_id=years.past_years)
    forecast = forecast.sel(year_id=years.forecast_years)

    lower_bound = _reshape_bound(forecast, caps.lower)
    upper_bound = _reshape_bound(forecast, caps.upper)

    mean_clipped = forecast.clip(min=lower_bound, max=upper_bound).fillna(0)

    del forecast
    gc.collect()

    capped_forecast = xr.concat([returned_past, mean_clipped], dim="year_id")

    return capped_forecast


def _forecast_entity(
    omega: float,
    past: xr.DataArray,
    transform: str,
    truncate: bool,
    truncate_quantiles: Tuple[float, float],
    replace_with_mean: bool,
    reference_scenario: str,
    years: YearRange,
    gbd_round_id: int,
    cap_forecasts: bool,
    cap_quantiles: Tuple[float, float],
    national_only: bool,
    age_standardize: bool,
    rescale_ages: bool,
    remove_zero_slices: bool,
) -> xr.DataArray:
    """Prepare data for forecasting, run model and post-process results."""
    most_detailed_past = filter.make_most_detailed_location(
        data=past, gbd_round_id=gbd_round_id, national_only=national_only
    )
    if "sex_id" not in most_detailed_past.dims or list(most_detailed_past.sex_id.values) != [
        SexConstants.BOTH_SEX_ID
    ]:
        most_detailed_past = filter.make_most_detailed_sex(data=most_detailed_past)
    if age_standardize:
        most_detailed_past = filter.make_most_detailed_age(
            data=most_detailed_past, gbd_round_id=gbd_round_id
        )

    if "draw" in most_detailed_past.dims:
        past_mean = most_detailed_past.mean("draw")
    else:
        past_mean = most_detailed_past

    clipped_past = _clip_past(past_mean=past_mean, transform=transform)

    processor = TransformConstants.TRANSFORMS[transform](
        years=years,
        gbd_round_id=gbd_round_id,
        age_standardize=age_standardize,
        remove_zero_slices=remove_zero_slices,
        rescale_age_weights=rescale_ages,
    )

    transformed_past = processor.pre_process(clipped_past)

    del clipped_past
    gc.collect()

    transformed_forecast = arc_method.arc_method(
        past_data_da=transformed_past,
        gbd_round_id=gbd_round_id,
        years=years,
        diff_over_mean=ModelConstants.DIFF_OVER_MEAN,
        truncate=truncate,
        reference_scenario=reference_scenario,
        weight_exp=omega,
        replace_with_mean=replace_with_mean,
        truncate_quantiles=truncate_quantiles,
        scenario_roc="national",
    )

    forecast = processor.post_process(transformed_forecast, past_mean)

    if np.isnan(forecast).any():
        raise ValueError("NaNs in forecasts")

    if cap_forecasts:
        forecast = _cap_forecasts(
            years, cap_quantiles, most_detailed_past, past_mean, forecast
        )

    return forecast


def arc_all_omegas(
    entity: str,
    stage: str,
    intrinsic: bool,
    subfolder: str,
    versions: Versions,
    model_name: str,
    omega_min: float,
    omega_max: float,
    omega_step_size: float,
    transform: str,
    truncate: bool,
    truncate_quantiles: Optional[Tuple[float, float]],
    replace_with_mean: bool,
    reference_scenario: str,
    years: YearRange,
    gbd_round_id: int,
    cap_forecasts: bool,
    cap_quantiles: Optional[Tuple[float, float]],
    national_only: bool,
    age_standardize: bool,
    rescale_ages: bool,
    predictive_validity: bool,
    remove_zero_slices: bool,
) -> None:
    """Forecast an entity with different omega values.

    If a SEV, the rei input could be a risk, or it could be a cause-risk.
    If it's a cause-risk (connected via hyphen), it's meant to be an
    intrinsic SEV, which would come from
    in_version/risk_acause_specific/{cause}_{risk}_intrinsic.nc,
    and the forecasted result would go to
    out_version/risk_acause_specific/{cause}_{risk}_intrinsic.nc.

    Args:
        entity (str): Entity to forecast
        stage (str): Stage of the run. E.x. sev, death, etc.
        intrinsic (bool): Whether this entity obtains the _intrinsic suffix
        subfolder (str): Optional subfolder for reading and writing files.
        versions (Versions): versions object with both past and future (input and output).
        model_name (str): Name to save the model under.
        omega_min (float): The minimum omega to try
        omega_max (float): The maximum omega to try
        omega_step_size (float): The step size of omegas to try between 0 and omega_max
        transform (str): Space to forecast data in
        truncate (bool): If True, then truncates the dataarray over the given dimensions
        truncate_quantiles (Tuple[float, float]): The tuple of two floats representing the
            quantiles to take
        replace_with_mean (bool): If True and `truncate` is True, then replace values outside
            of the upper and lower quantiles taken across `location_id` and `year_id` and with
            the mean across `year_id`, if False, then replace with the upper and lower bounds
            themselves
        reference_scenario (str): If 'median' then the reference scenario is made using the
            weighted median of past annualized rate-of-change across all past years, 'mean'
            then it is made using the weighted mean of past annualized rate-of-change across
            all past years
        years (YearRange): forecasting year range
        gbd_round_id (int): the gbd round id
        cap_forecasts (bool): If used, forecasts will be capped. To forecast without caps,
            dont use this
        cap_quantiles (tuple[float]): Quantiles for capping the future
        national_only (bool): Whether to run national only data or not
        rescale_ages (bool): whether to rescale during ARC age standardization. We are
            currently only setting this to true for the sevs pipeline.
        age_standardize (bool): whether to age_standardize before modeling.
        predictive_validity (bool): whether to do predictive validity or real forecasts
        remove_zero_slices (bool): If True, remove zero-slices along certain dimensions, when
            pre-processing inputs, and add them back in to outputs.
    """
    logger.debug(f"Running `forecast_one_risk_main` for {entity}")

    input_version_metadata = versions.get(past_or_future="past", stage=stage)

    file_name = entity
    if intrinsic:  # intrinsic entities have _intrinsic attached at file name
        file_name = entity + "_intrinsic"

    data = open_xr_scenario(
        file_spec=FHSFileSpec(
            version_metadata=input_version_metadata,
            sub_path=(subfolder,),
            filename=f"{file_name}.nc",
        )
    )

    # rid the past data of point coords because they throw off weighted-quantile
    superfluous_coords = [d for d in data.coords.keys() if d not in data.dims]
    data = data.drop_vars(superfluous_coords)

    past = data.sel(year_id=years.past_years)

    if predictive_validity:
        holdouts = data.sel(year_id=years.forecast_years)
        all_omega_pv_results: List[xr.DataArray] = []

    for omega in pv.get_omega_weights(omega_min, omega_max, omega_step_size):
        logger.debug("omega:{}".format(omega))

        forecast = _forecast_entity(
            omega=omega,
            past=past,
            transform=transform,
            truncate=truncate,
            truncate_quantiles=truncate_quantiles,
            replace_with_mean=replace_with_mean,
            reference_scenario=reference_scenario,
            years=years,
            gbd_round_id=gbd_round_id,
            cap_forecasts=cap_forecasts,
            cap_quantiles=cap_quantiles,
            national_only=national_only,
            age_standardize=age_standardize,
            rescale_ages=rescale_ages,
            remove_zero_slices=remove_zero_slices,
        )

        if predictive_validity:
            all_omega_pv_results.append(
                pv.calculate_predictive_validity(
                    forecast=forecast, holdouts=holdouts, omega=omega
                )
            )

        else:
            output_version_metadata = versions.get(past_or_future="future", stage=stage)

            output_file_spec = FHSFileSpec(
                version_metadata=output_version_metadata,
                sub_path=(FileSystemConstants.SUBMODEL_FOLDER, model_name, subfolder),
                filename=f"{file_name}_{omega}.nc",
            )

            save_xr_scenario(
                xr_obj=forecast,
                file_spec=output_file_spec,
                metric="rate",
                space="identity",
                omega=omega,
                transform=transform,
                truncate=str(truncate),
                truncate_quantiles=str(truncate_quantiles),
                replace_with_mean=str(replace_with_mean),
                reference_scenario=str(reference_scenario),
                cap_forecasts=str(cap_forecasts),
                cap_quantiles=str(cap_quantiles),
            )

    if predictive_validity:
        pv_df = pv.finalize_pv_data(pv_list=all_omega_pv_results, entity=entity)

        pv.save_predictive_validity(
            file_name=file_name,
            gbd_round_id=gbd_round_id,
            model_name=model_name,
            pv_df=pv_df,
            stage=stage,
            subfolder=subfolder,
            versions=versions,
        )
