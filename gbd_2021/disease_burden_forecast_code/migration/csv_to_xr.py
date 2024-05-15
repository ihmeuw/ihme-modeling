"""
* Convert csv predictions to xarray file and makes epsilon.
* Past takes csv and formats to xarray, or xarray if it's already created w/
LimeTr output.

Example Call:

python FILEPATH/csv_to_xr.py
--mig-version click_20210510_limetr_fixedint
--model-version click_20210510_limetr_fixedint
--model-name model_6_single_years
--gbd-round-id 6
--years 1950:2020:2050

"""
import argparse
import os.path
import pandas as pd
import xarray as xr

from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr, open_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

AGE_GROUP_ID = 22
SEX_ID = 3

def make_eps(mig_version, model_version, model_name, gbd_round_id, years):
    logger.info("Making Epsilons")
    model_dir = FBDPath("FILEPATH")
    model_path = model_dir / f"{model_name}.csv"
    df = pd.read_csv(model_path)
    # add all-sex and all-age id columns
    df["sex_id"] = SEX_ID
    df["age_group_id"] = AGE_GROUP_ID
    # select the columns we need
    df = df[["location_id", "year_id", "age_group_id", "sex_id", "predictions",
             "migration_rate"]]
    # set index columns
    index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]

    dataset = df.set_index(index_cols).to_xarray()
    dataset["eps"] = dataset["migration_rate"] - dataset["predictions"]
    mig_dir = FBDPath("FILEPATH")
    eps_path = mig_dir / "mig_eps.nc"
    save_xr(dataset["eps"].sel(year_id=years.past_years),
            eps_path, metric="rate", space="identity")

    pred_path = mig_dir / "mig_hat.nc"
    save_xr(dataset["predictions"].sel(year_id=years.years),
            pred_path, metric="rate", space="identity")

    mig_path = mig_dir / "wpp_hat.nc"
    save_xr(dataset["migration_rate"].sel(year_id=years.years),
            mig_path, metric="rate", space="identity")


def make_past(mig_version, gbd_round_id, years):
    past_dir = FBDPath("FILEPATH")
    if os.path.isfile(past_dir / f"past_mig_rate_single_years.csv"):
        logger.info("Past in csv, converting to xarray")
        past_path = past_dir / f"past_mig_rate_single_years.csv"
        df = pd.read_csv(past_path)
        # add all-sex and all-age id columns
        df["sex_id"] = SEX_ID
        df["age_group_id"] = AGE_GROUP_ID
        # select the columns we need
        df = df[["location_id",
                 "year_id",
                 "age_group_id",
                 "sex_id",
                 "migration_rate"]]
        # set index columns
        index_cols = ["location_id",
                      "year_id",
                      "age_group_id",
                      "sex_id"]

        dataset = df.set_index(index_cols).to_xarray()
        save_xr(
            dataset.sel(year_id=years.past_years), past_dir / "wpp_past.nc",
            metric="rate", space="identity")
    elif os.path.isfile(past_dir / f"past_mig_rate_single_years.nc"):
        dataset = open_xr(past_dir / f"past_mig_rate_single_years.nc")
        save_xr(
            dataset.sel(year_id=years.past_years), past_dir / "wpp_past.nc",
            metric="rate", space="identity")
        logger.info("Past Mig Rate Xarray already created: copied "
                    "to wpp_past.nc")
    else:
        raise OSError("Past Mig not in csv or not already an xarray")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--mig-version", type=str, required=True,
        help="The version the migration data are saved under.")
    parser.add_argument(
        "--model-version", type=str, required=True,
        help="The version the model results are saved under.")
    parser.add_argument(
        "--model-name", type=str, required=True,
        help="The name of the model.")
    parser.add_argument("--gbd-round-id", type=int, required=True,
        help="Which gbd round id to use")
    parser.add_argument("--years", type=str,
        help="past_start:forecast_start:forecast_end")
    args = parser.parse_args()

    years = YearRange.parse_year_range(args.years)

    make_eps(args.mig_version, args.model_version, args.model_name,
        args.gbd_round_id, years)

    make_past(args.mig_version, args.gbd_round_id, years)
