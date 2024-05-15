"""
Convert migration rates output by draw generation step to counts for use in
age-sex splitting step.

Example:

.. code:: bash
    python -m pdb FILEPATH/migration_rate_to_count.py \
        --migration_version click_20210510_limetr_fixedint \
        --past_pop_version 20200206_etl_gbd_decomp_step4_1950_2019_run_id_192 \
        --forecast_pop_version 20200513_arc_method_new_locs_ratio_subnats_else_pop_paper \
        --gbd_round_id 6 \
        --years 1950:2020:2050

Counts are calculated on mean population to avoid over-dispersed draws. ie: a high population 
draw is arbitrarily multiplied by high in-migration draw or visa-versa.
"""
import argparse

from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange

SCALE_FACTOR = 1000

def load_past_pop(gbd_round_id, version):
    """
    Load past population data. This will generally be from 1950 to the start of
    the forecasts.

    Args:
        gbd_round_id (int):
            The gbd round ID that the past population is from
        version (str):
            The version of past population to read from

    Returns:
        xarray.DataArray: The past population xarray dataarray
    """
    past_pop_dir = FBDPath("FILEPATH")
    past_pop_path = past_pop_dir / "population.nc"
    past_pop_da = open_xr(past_pop_path)

    return past_pop_da


def load_forecast_pop(gbd_round_id, version, years):
    """
    Load forecast population data. Aggregates if necessary.

    Args:
        gbd_round_id (int): The gbd round ID that the past population is from
        version (str): The version of forecast population to read from
        years (YearRange): The Forecasting format years to use.
        draws (int): Population is resampled to number of draws supplied.

    Returns:
        xarray.DataArray: The past population xarray dataarray
    """
    forecast_pop_dir = FBDPath("FILEPATH")
    forecast_pop_path = forecast_pop_dir / "population_agg.nc"
    forecast_pop_da = open_xr(forecast_pop_path)

    # slice to correct years
    forecast_pop_da = forecast_pop_da.sel(year_id=years.forecast_years)

    return forecast_pop_da


def main(migration_version, past_pop_version, forecast_pop_version,
    gbd_round_id, years):
    """
    Load pops and migration rate, multiply to get counts
    """
    # Load migration data
    mig_dir = FBDPath("FILEPATH")
    mig_path = mig_dir / "mig_star.nc"
    mig_da = open_xr(mig_path)

    # Load pops
    past_pop_da = load_past_pop(gbd_round_id, past_pop_version)
    forecast_pop_da = load_forecast_pop(gbd_round_id, forecast_pop_version,
        years)

    past_pop_da =  expand_dimensions(past_pop_da,
        scenario=forecast_pop_da["scenario"].values)

    # Subset to coordinates relevant to mig_da
    forecast_pop_da = forecast_pop_da.sel(sex_id=3, age_group_id=22,
        location_id=mig_da.location_id.values, scenario=0)
    past_pop_da = past_pop_da.sel(sex_id=3, age_group_id=22,
        location_id=mig_da.location_id.values, scenario=0)

    # Combine past and forecast pop
    pop_da = past_pop_da.combine_first(forecast_pop_da)

    # Multiply rates by pop to get counts
    mig_counts = mig_da * pop_da
    mig_counts = mig_counts / SCALE_FACTOR

    # Save out
    mig_counts_path = mig_dir / "mig_counts.nc"
    save_xr(mig_counts, mig_counts_path, metric="number", space="identity")

    # Load past migration data
    past_mig_dir = FBDPath("FILEPATH")
    past_mig_path = past_mig_dir / "wpp_past.nc"
    past_mig_da = open_xr(past_mig_path)

    # Multiply rates by past pop to get counts
    past_mig_counts = past_mig_da * past_pop_da
    past_mig_counts = past_mig_counts / SCALE_FACTOR

    past_mig_counts_path = past_mig_dir / "wpp_past_counts.nc"
    save_xr(past_mig_counts, past_mig_counts_path, metric="number", space="identity")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--migration_version", type=str, required=True,
        help="Which version of migration to convert")
    parser.add_argument("--past_pop_version", type=str, required=True,
        help="Which version of past populations to use")
    parser.add_argument("--forecast_pop_version", type=str, required=True,
        help="Which version of forecasted populations to use")
    parser.add_argument("--gbd_round_id", type=int, required=True,
        help="Which gbd round id to use for populations")
    parser.add_argument("--years", type=str, required=True,
        help="past_start:forecast_start:forecast_end")
    args = parser.parse_args()

    years = YearRange.parse_year_range(args.years)

    main(migration_version=args.migration_version,
        past_pop_version=args.past_pop_version,
        forecast_pop_version=args.forecast_pop_version,
        gbd_round_id=args.gbd_round_id,
        years=years)
