from fhs_lib_year_range_manager.lib.year_range import YearRange
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_model.lib.arc_method import arc_method
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_data_transformation.lib.constants import DimensionConstants
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.constants import AgeConstants, SexConstants
from fhs_lib_genem.lib.predictive_validity import root_mean_square_error
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_genem.lib.constants import TransformConstants
from fhs_lib_data_transformation.lib.resample import resample
from scipy.special import expit, logit



import gc
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import xarray as xr

stage = "u5m"
entity = "u5m"
input_version = "20230508_u5m_future_as_fake_past"
out_version   = "20230515_u5m_to_2150"
transform = "logit"
truncate = True 
truncate_quantiles = (0.15, 0.85)
reference_scenario = "mean"
gbd_round_id = 7
pv_years = YearRange(2020,2030,2050)
years = YearRange(2020, 2051, 2150)
min_omega = 0
max_omega = 3
draws = 1000
omega_step_size = 0.5
replace_with_mean = False
national_only = False
uncertainty=True

cap_percentile = 0.95


def _forecast_urbanicity(
	omega: float,
	past: xr.DataArray,
	transform: str,
	truncate: bool,
	truncate_quantiles: Tuple[float, float],
	replace_with_mean: bool,
	reference_scenario: str,
	years: YearRange,
	gbd_round_id: int,
	uncertainty: bool,
	national_only,
	extra_dim=None):
	modeled_location_ids = get_location_set(
	    gbd_round_id, national_only=national_only
	).location_id.values
	most_detailed_past = past.sel(location_id = modeled_location_ids)
	original_coords = dict(most_detailed_past.coords)
	original_coords.pop("draw", None)
	zeros_dropped = most_detailed_past
	if uncertainty:
		most_detailed_past = most_detailed_past.sel(year_id=years.past_end)
	else:
		most_detailed_past=None
		gc.collect()
	if "draw" in zeros_dropped.dims:
		past_mean = zeros_dropped.mean("draw")
	else:
		past_mean = zeros_dropped.copy()
	transformed_past = logit(past_mean)
	transformed_forecast = arc_method.arc_method(
		past_data_da = transformed_past,
		gbd_round_id = gbd_round_id,
		years = years,
		truncate = truncate,
		reference_scenario=reference_scenario,
		weight_exp=omega,
		replace_with_mean=replace_with_mean,
		truncate_quantiles=truncate_quantiles,
		scenario_roc = "national",
		extra_dim=extra_dim)
	scaled_forecast = expit(transformed_forecast)
	zeros_appended = expand_dimensions(scaled_forecast, fill_value=0.0, **original_coords)
	return(zeros_appended)


def omega_strategy(rmse, draws, threshold=0.05, **kwargs):
	norm_rmse = rmse / rmse.min()
	weight_with_lowest_rmse = norm_rmse.where(
	    norm_rmse == norm_rmse.min()).dropna("weight")["weight"].values[0]
	weights_to_check = [
	    w for w in norm_rmse["weight"].values if w <= weight_with_lowest_rmse]
	rmses_to_check = norm_rmse.sel(weight=weights_to_check)
	rmses_to_check_within_threshold = rmses_to_check.where(
	    rmses_to_check < 1 + threshold).dropna("weight")
	reciprocal_rmses_to_check_within_threshold = (
	    1 / rmses_to_check_within_threshold).fillna(0)
	norm_reciprocal_rmses_to_check_within_threshold = (
	    reciprocal_rmses_to_check_within_threshold
	    / reciprocal_rmses_to_check_within_threshold.sum())
	omega_draws = xr.DataArray(
	    np.random.choice(
	        a=norm_reciprocal_rmses_to_check_within_threshold["weight"].values,
	        size=draws,
	        p=norm_reciprocal_rmses_to_check_within_threshold.sel(
	        	pv_metric = "root_mean_square_error").values),
	    coords=[list(range(draws))], dims=["draw"])
	return omega_draws

input_dir = f"FILEPATH"

data = open_xr(f"{input_dir}/{entity}.nc").data

if "draw" in data.dims:
    data = data.mean("draw")


superfluous_coords = [d for d in data.coords.keys() if d not in data.dims]
data = data.drop(superfluous_coords)

holdouts = data.sel(year_id=pv_years.forecast_years)
past = data.sel(year_id=pv_years.past_years)
past = past.clip(max = 0.999)

all_omega_pv_results = []
pv_metrics = [root_mean_square_error]
pv_dims = [DimensionConstants.LOCATION_ID, DimensionConstants.SEX_ID]

for omega in np.arange(min_omega, max_omega, omega_step_size):
        all_pv_metrics_results = []
        for pv_metric in pv_metrics:
            predicted_holdouts = _forecast_urbanicity(
                                    omega, past, transform, truncate,
                                    truncate_quantiles, replace_with_mean,
                                    reference_scenario, pv_years,
                                    gbd_round_id, uncertainty, national_only)
            pv_data = pv_metric(predicted_holdouts.sel(scenario=0), holdouts)
            one_pv_metric_result = xr.DataArray(
                [[pv_data.values]], [[omega], [pv_metric.__name__]],
                dims=["weight", "pv_metric"])
            all_pv_metrics_results.append(one_pv_metric_result)
        all_pv_metrics_results = xr.concat(
            all_pv_metrics_results, dim="pv_metric")
        all_omega_pv_results.append(all_pv_metrics_results)


all_omega_pv_results = xr.concat(all_omega_pv_results, dim="weight")

pv_dir = FBDPath(f"FILEPATH")
pv_dir.mkdir(parents=True, exist_ok=True)
pv_file = pv_dir / f"{entity}_pv.nc"
all_omega_pv_results.to_netcdf(str(pv_file))


input_dir = FBDPath(f"FILEPATH")
past = open_xr(input_dir / f"{entity}.nc").data
past = past.clip(max = 0.999)

pv_path = FBDPath(f"FILEPATH")
pv_file = pv_path / f"{entity}_pv.nc"
all_pv_data = open_xr(pv_file).data
omega = omega_strategy(all_pv_data, draws)

#Dataset has only one age and sex, year is 2022 so ignore those dimensions
#We want to take 0.95 quantile of mean draws over locations (So 0.95 quantile of 346 value array)
forecast = _forecast_urbanicity(omega, past, transform, truncate,
                         truncate_quantiles, replace_with_mean,
                         reference_scenario, years,
                         gbd_round_id, uncertainty, national_only,
                         extra_dim="draw")

if isinstance(omega, xr.DataArray):
    report_omega = float(omega.mean())
else:
    report_omega = omega


future_output_dir = FBDPath(f"FILEPATH")

save_xr(forecast,
        future_output_dir / f"{entity}.nc", metric="rate",
        space="identity", omega_strategy="use_zero_biased_omega_distribution",
        omega=report_omega, pv_metric=pv_metric.__name__)