"""
Apply Random Walk on every-5-year migration data without draws.
Take mean of random walk to model latent trends.
Generate draws from normal distribution mean=point estimate, sd=past sd

python FILEPATH/arima_and_generate_draws.py
--eps-version click_20210510_limetr_fixedint
--arima-version click_20210510_limetr_fixedint
--measure migration
--gbd-round-id 6
--draws 1000
--years 1950:2020:2050
--locations-to-shock-smooth 12345 133 98
"""
import argparse
import numpy as np
import xarray as xr
from typing import List, Tuple

from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr, open_xr
from fhs_lib_model.lib.random_walk.random_walk import RandomWalk
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging
from migration import remove_drift

logger = fhs_logging.get_logger()
rng = np.random.default_rng(47)

RATE_CAP = 10  # per 1000 people
N_PAST_ANCHOR_YEARS = 20
UI_MAX_RATE = 10  # per 1000 people
UI_MIN_RATE = 0.5  # per 1000 people

def save_y_star(
    eps_version: str,
    arima_version: str,
    years: YearRange,
    measure: str,
    draws: int,
    decay: float,
    gbd_round_id: int,
    locations_to_shock_smooth: List[int],
) -> None:
    """
    apply random walk and save the output
    """
    mig_dir = FBDPath("FILEPATH")
    # load "true" observed past migration rate
    past_migration_rate_path = mig_dir / "migration_single_years.nc"
    past_migration_rate = open_xr(past_migration_rate_path).sel(year_id=years.past_years)
    
    eps_path = mig_dir / "mig_eps.nc"
    ds = open_xr(eps_path)
    try:
        eps_preds = open_xr(mig_dir / "eps_star.nc")
    except Exception:
        ds = shock_smooth_locations(ds, years, locations_to_shock_smooth, N_PAST_ANCHOR_YEARS)
        eps_preds = arima_migration(ds, years, draws, decay) 
        epsilon_hat_out = mig_dir / "eps_star.nc"
        save_xr(eps_preds, epsilon_hat_out, metric="rate", space="identity")

    # cap residuals between -`RATE_CAP` and `RATE_CAP`
    # population forecast ui is not credible when residuals are uncapped
    eps_past = eps_preds.sel(year_id=years.past_years)
    eps_preds = eps_preds.sel(year_id=years.forecast_years)
    eps_preds = eps_preds.clip(min=-RATE_CAP, max=RATE_CAP)
    eps_preds = xr.concat([eps_past, eps_preds], dim="year_id")
    eps_preds = eps_preds.mean(dim="draw")

    pred_path = mig_dir / "mig_hat.nc"
    preds = open_xr(pred_path)
    preds = preds.sel(year_id=years.years)
    y_star = preds + eps_preds
    y_star_past = y_star.sel(year_id=years.past_years)
    y_star_draws = generate_draws_from_predicted_mean(
        y_star,
        past_migration_rate, 
        years, 
        draws, 
        std_dev_bounds=(UI_MIN_RATE, UI_MAX_RATE))
    y_star_draws = xr.concat([y_star_past, y_star_draws], dim="year_id")

    save_path = FBDPath("FILEPATH")
    ystar_out = save_path / "mig_star.nc"

    save_xr(y_star_draws, ystar_out, metric="rate", space="identity")


def arima_migration(epsilon_past, years, draws, decay):
    """
    apply drift attenuation and fit random walk on the dataset
    """
    drift_component = remove_drift.get_decayed_drift_preds(epsilon_past, years, decay)
    remainder = epsilon_past - drift_component.sel(year_id=years.past_years)
    ds = xr.Dataset(dict(y=remainder.copy()))

    rw_model = RandomWalk(ds, years, draws)
    rw_model.fit()
    rw_preds = rw_model.predict()

    return drift_component + rw_preds


def generate_draws_from_predicted_mean(
    da: xr.DataArray,
    true_past: xr.DataArray,
    years: YearRange,
    draws: int,
    std_dev_bounds: Tuple[int, int],
) -> xr.DataArray:
    """
    args:
        da: y_star array of regression estimates + mean of random walk fit on residual
        true_past: data array of "true" past migration rate ETLed from WPP
        years: past_start:forecast_start:forecast_end
        draws: number of draws to generate
        std_dev_cap: rate per thousand at which to cap std_dev of generated draws

    Generates draws from a normal distribution with mean = y_star, std dev = the std dev
    for each location over all past years *
    (forecast_start - (forecast_start - 40)) / `scale_value`
    For `scale_value`= 120, forecast year 1: sd = 0.33 * sd_past, increasing by 2.5% each year
    In forecast year 80: sd = 1 * sd_past. Draws are left unordered
    """
    scale_value = 120  # scale expanding uncertainty to reasonable value

    forecast_da = da.sel(year_id=years.forecast_years)
    forecast_da = forecast_da.expand_dims(
        draw=range(0, draws)
    )  # create appropriate draw coords

    # calculate past SD for each nation, scale into future
    past_da = true_past.sel(scenario=0, drop=True).expand_dims(sex_id=[3], age_group_id=[22])
    past_sd = past_da.std(dim="year_id")
    past_sd = past_sd.clip(min=std_dev_bounds[0], max=std_dev_bounds[1])  # we want to cap 
    # predictions of sustained high migration into future based purely on extreme events in recent past
    past_sd = past_sd.expand_dims(draw=range(0, draws), year_id=years.forecast_years)
    past_sd = past_sd.transpose(*forecast_da.dims)

    # sd widening equation
    coef_da = xr.DataArray(
        data=(forecast_da.year_id.values - (years.forecast_start - 40)) / scale_value,
        dims={"year_id": forecast_da.year_id.values},
    )

    coef_da = coef_da.expand_dims(
        location_id=forecast_da.location_id.values,
        draw=forecast_da.draw.values,
        sex_id=forecast_da.sex_id.values,
        age_group_id=forecast_da.age_group_id.values,
    )

    # uncertainty is generated from 3 arrays
    logger.info("generating draws from mean predicted migration rate")
    forecast_with_uncertainty = rng.normal(forecast_da, past_sd * coef_da)

    nd_labeled_uncertainty = xr.DataArray(
        data=forecast_with_uncertainty,
        coords=forecast_da.coords,
    )

    return nd_labeled_uncertainty


def shock_smooth_locations(
    ds: xr.DataArray,
    years: YearRange,
    locations_to_shock_smooth: List[int],
    n_past_anchor_years: int,
):
    """
    args:
        ds: data array of epsilons (migration model error)
        years: past_start:forecast_start:forecast_end
        locations_to_shock_smooth: location ids for locations we want to shock smooth
        n_past_anchor_year: number of most recent past years to calculate mean epsilon from

    This function provides a way of smoothing recent shocks/spikes/extreme migration events
    prior to fitting and forecasting model-error using a random-walk. For each location 
    provided the epsilon of the last-past year is set to the mean of `n_past_anchor_years`.
    Example: if n_past_anchor_years=3, last_past_year=2022, then epsilon of 2022 will be set 
    to the mean epsilon off [2020, 2021, 2022].

    Overall this means extreme events in the last past years in supplied countries will have 
    less impact on forecasts.

    NOTE: Because its hard to define what a `shocked` location is it might be prefereble to 
    find an entirely new method to forecast model error.
    """
    logger.info(
        f"Smoothing migration shocks in the year {years.past_end} "
        f"from the following locations: {locations_to_shock_smooth}"
    )

    locs_to_shock_smooth = ds.sel(location_id=locations_to_shock_smooth)
    mean_eps = locs_to_shock_smooth.sel(
        year_id=list(range(years.past_end - n_past_anchor_years, years.past_end))
    ).mean(dim="year_id")

    for location in locations_to_shock_smooth:
        loc_specific_mean_eps = mean_eps.sel(location_id=location).squeeze().values
        ds.loc[
            {
                "location_id": location,
                "age_group_id": 22,
                "sex_id": 3,
                "year_id": years.past_end,
            }
        ] = loc_specific_mean_eps

    return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--eps-version", type=str, required=True, help="The version the eps are saved under."
    )
    parser.add_argument(
        "--arima-version",
        type=str,
        required=True,
        help="The version the arima results are saved under.",
    )
    parser.add_argument("--measure", type=str, required=True, choices=["migration", "death"])
    parser.add_argument(
        "--decay-rate",
        type=float,
        required=False,
        default=0.1,
        help="Rate at which drift on all-cause epsilons decay in future",
    )
    parser.add_argument(
        "--gbd-round-id", type=int, required=True, help="Which gbd round id to use"
    )
    parser.add_argument("--draws", type=int, help="Number of draws")
    parser.add_argument("--years", type=str, help="past_start:forecast_start:forecast_end")
    parser.add_argument(
        "--locations-to-shock-smooth",
        type=int,
        nargs="*",
        help="Location ids" "to `shock smooth`, can supply multiple location ids.",
    )
    args = parser.parse_args()

    years = YearRange.parse_year_range(args.years)

    save_y_star(
        args.eps_version,
        args.arima_version,
        years,
        args.measure,
        args.draws,
        args.decay_rate,
        args.gbd_round_id,
        args.locations_to_shock_smooth,
    )
