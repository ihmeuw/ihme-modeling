"""
Produce aggregate versions of shocks for use in
modeling migration, saving them as csvs in their original directories.

Example:

.. code:: bash

python FILEPATH/aggregate_shocks_and_sdi.py \
--shocks_version 20210419_shocks_only_decay_weight_15 \
--past_pop_version 20200206_etl_gbd_decomp_step4_1950_2019_run_id_192 \
--forecast_pop_version 20200513_arc_method_new_locs_ratio_subnats_else_pop_paper \
--gbd_round_id 6 \
--years 1950:2020:2050

"""
import argparse

from fhs_lib_data_aggregation.lib.aggregator import Aggregator
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.query.age import get_age_weights
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

ALL_AGE_GROUP_ID = 22
BOTH_SEX_ID = 3
REFERENCE_SCENARIO = 0
SHOCK_ACAUSES = ("inj_disaster", "inj_war_execution", "inj_war_warterror")


def load_past_pop(gbd_round_id, version, years):
    """
    Load past population data. This will generally be from 1950 to the start of
    the forecasts. Takes the mean of draws.

    Args:
        gbd_round_id (int):
            The gbd round ID that the past population is from
        version (str):
            The version of past population to read from

    Returns:
        xarray.DataArray: The past population xarray dataarray
    """
    past_pop_path = FBDPath("FILEPATH")
    past_pop_file = past_pop_path / "population.nc"
    past_pop_da = open_xr(past_pop_file)

    # slice to correct years
    past_pop_da = past_pop_da.sel(year_id=years.past_years)

    return past_pop_da


def load_forecast_pop(gbd_round_id, version, years):
    """
    Load forecast population data. Aggregates if necessary. Takes mean of draws.

    Args:
        gbd_round_id (int):
            The gbd round ID that the past population is from
        version (str):
            The version of forecast population to read from
        years (YearRange):
            The Forecasting format years to use.

    Returns:
        xarray.DataArray: The past population xarray dataarray
    """
    forecast_pop_path = FBDPath("FILEPATH")
    try:
        forecast_pop_file = forecast_pop_path / "population_agg.nc"
        forecast_pop_da = open_xr(forecast_pop_file)
        try:  # Sometimes saved with draws in agg
            forecast_pop_da = forecast_pop_da.mean("draw")
        except:
            pass
    except OSError:  # Need to make agg version
        forecast_pop_file = forecast_pop_path / "population.nc"
        forecast_pop_da = open_xr(forecast_pop_file)
        forecast_pop_da = forecast_pop_da.mean("draw")
        forecast_pop_da = Aggregator.aggregate_everything(forecast_pop_da, gbd_round_id).pop
        forecast_pop_out_file = forecast_pop_path / "population_agg.nc"
        save_xr(forecast_pop_da, forecast_pop_out_file, metric="number", space="identity")

    # slice to correct years
    forecast_pop_da = forecast_pop_da.sel(year_id=years.forecast_years)

    return forecast_pop_da


def main(
    shocks_version,
    past_pop_version,
    forecast_pop_version,
    gbd_round_id,
    years,
):
    """
    Load pops and shocks data, aggregate, convert to csv, save
    """
    past_pop_da = load_past_pop(gbd_round_id, past_pop_version, years)
    forecast_pop_da = load_forecast_pop(gbd_round_id, forecast_pop_version, years)
    most_detailed_ages = list(
        get_age_weights(
            gbd_round_id=gbd_round_id,
            most_detailed=True,
        )["age_group_id"]
    )

    # Give past populations dummy scenarios to be concatenated with forecast pops
    past_pop_da = expand_dimensions(past_pop_da, scenario=forecast_pop_da["scenario"].values)

    forecast_pop_da = forecast_pop_da.sel(
        scenario=REFERENCE_SCENARIO, age_group_id=most_detailed_ages
    )

    past_pop_da = past_pop_da.sel(
        scenario=REFERENCE_SCENARIO,
        age_group_id=most_detailed_ages,
        location_id=forecast_pop_da.location_id,
    )

    # Combine past and forecast pop
    pop_da = past_pop_da.combine_first(forecast_pop_da)

    # Save out shocks
    if shocks_version:
        for acause in ["inj_disaster", "inj_war_execution", "inj_war_warterror"]:
            # Load data
            shock_path = FBDPath(
                "FILEPATH".format(g=gbd_round_id, version=shocks_version)
            )
            shock_file = shock_path / "{}.nc".format(acause)
            shock_da = open_xr(shock_file)
            # Take mean of draws
            shock_da = shock_da.mean("draw")
            # Aggregate everything
            pop_agg = Aggregator(pop_da)

            shock_da = pop_agg.aggregate_everything(
                pop_da, gbd_round_id, data=shock_da
            ).data.rate

            shock_da = shock_da.sel(age_group_id=ALL_AGE_GROUP_ID, sex_id=BOTH_SEX_ID)
            # Convert to dataframe and save
            shock_df = shock_da.to_dataframe(name="mortality")
            shock_out_file = shock_path / "mean_{}.csv".format(acause)
            shock_df.to_csv(shock_out_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--shocks_version",
        type=str,
        required=False,
        help="Which version of shocks to convert",
    )
   
    parser.add_argument(
        "--past_pop_version", type=str, help="Which version of past populations to use"
    )
    parser.add_argument(
        "--forecast_pop_version",
        type=str,
        help="Which version of forecasted populations to use",
    )
    parser.add_argument(
        "--gbd_round_id", type=int, help="Which gbd round id to use for pops and shocks"
    )
    parser.add_argument("--years", type=str, help="past_start:forecast_start:forecast_end")
    args = parser.parse_args()

    years = YearRange.parse_year_range(args.years)

    if args.shocks_version:
        main(
            shocks_version=args.shocks_version,
            past_pop_version=args.past_pop_version,
            forecast_pop_version=args.forecast_pop_version,
            gbd_round_id=args.gbd_round_id,
            years=years,
        )
