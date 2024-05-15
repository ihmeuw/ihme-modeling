
"""
Combine the separate location files from the age-sex splitting of migration.
Balance the migration data to be zero at each age-sex-draw combo.
Add any missing locations (all level 3 and level 4 locations) and fill with 0s.

Example:

.. code:: bash
    python FILEPATH/balance_migration.py \
    --version click_20210510_limetr_fixedint \
    --gbd_round_id 6
"""

import argparse
import pandas as pd
import xarray as xr

from db_queries import get_location_metadata
from fhs_lib_data_transformation.lib.pandas_to_xarray import df_to_xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_summary_maker.lib.summary import compute_summary
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

LOCATION_SET_ID = 39
RELEASE_ID = 6
ID_VARS = ["location_id", "year_id", "age_group_id", "sex_id", "draw"]
WPP_LOCATION_IDS = pd.read_csv(
                         "FILEPATH"
                         )["location_id"].unique().tolist()


def combine_and_save_mig(version):
    """
    Load location csvs of migration files and combine into an xarray dataarray.

    Args:
        version (str):
            The version of migration to combine and save

    Returns:
        xarray.DataArray: The combined migration data xarray dataarray.
    """
    logger.info("Combining migration csvs to xarray")
    all_locs_xr_list = []
    # Put dataframes for each location into a list
    for loc in WPP_LOCATION_IDS + subnat_location_ids:
        temp = pd.read_csv(f'FILEPATH/{loc}.csv')
        #temp = temp.set_index(ID_VARS)
        temp = df_to_xr(temp, dims=ID_VARS)
        all_locs_xr_list.append(temp)
    # Concat all locations together
    result = xr.concat(all_locs_xr_list, dim='location_id')

    # Save to forecasting directory
    result.to_netcdf(f'FILEPATH/migration_split.nc')
    logger.info("Saved migration xarray to FHS WPP directory")
    return result

def balance_migration(mig_da):
    """
    Ensure that net migration is zero at each loc-sex-age combo.
    Calculate K = sqrt(-sum of positive values/sum of negative values)
    Divide positive values by K
    Multiply negative values by K

    Args:
        mig_da (xarray.DataArray):
            The input migration xarray dataarray that is being balanced

    Returns:
        xarray.DataArray: The balanced migration data,
            in dataarray.
    """
    logger.info("Entered balancing step")
    # only balance national locations (level 3), do not balance subnational (level 4)
    subnat_location_ids = list(set(mig_da.location_id.values) - set(WPP_LOCATION_IDS))
    balance_mig_da = mig_da.sel(location_id=WPP_LOCATION_IDS)
    no_balance_mig_da = mig_da.sel(location_id=subnat_location_ids)

    negatives = balance_mig_da.where(mig_da < 0)
    positives = balance_mig_da.where(mig_da > 0)
    zeros = balance_mig_da.where(mig_da == 0)

    sum_dims = [dim for dim in mig_da.dims if dim not in (
        "draw", "age_group_id", "sex_id", "year_id")]
    k = positives.sum(sum_dims) / negatives.sum(sum_dims)

    # Add a case for if sum of either is zero

    # Multiply constant by positives
    adjusted_positives = xr.ufuncs.sqrt(1 / -k) * positives
    adjusted_negatives = xr.ufuncs.sqrt(-k) * negatives

    # Combine
    balanced_mig_da = adjusted_positives.combine_first(adjusted_negatives)
    balanced_mig_da = balanced_mig_da.combine_first(zeros)
    balanced_mig_da = balanced_mig_da.combine_first(no_balance_mig_da)

    logger.info("Balanced migration")
    return balanced_mig_da

def add_missing_locs(mig_da, location_ids):
    """
    Append any missing locations in location_id and fill with 0 net migration
    Args:
        mig_da (xarray.DataArray):
            The input migration xarray dataarray that locations are being appended to
        location_ids ([int]):
            A list of the location ids that are expected in mig_da

    Returns:
        xarray.DataArray:
            The migration data with all location_ids
    """
    missing_locs = list(set(location_ids).difference(set(mig_da.location_id.values)))
    logger.info(f"Filling missing locations { {*missing_locs} } with 0")
    mig_da = expand_dimensions(mig_da, location_id=missing_locs, fill_value=0)
    return mig_da

def _clean_migration_locations(migration, pop, gbd_round_id):
    """Migration uses weird locations. Sometimes, locations are missing
    migration data. Other times, locations have migration data but they
    shouldn't.

    In the case where locations have migration data, but they should really be
    part of another location (e.g. Macao is part of China), that migration will
    be added into the "parent" location.

    In the case where locations are missing migration data, those locations
    will get the average migration of their regions. This averaging happens
    AFTER too-specific locations are merged into their parents.
    """
    merged_migration = _merge_too_specific_locations(migration)
    filled_migration = _fill_missing_locations(
            merged_migration, pop, gbd_round_id)
    return filled_migration

def _fill_missing_locations(data_per_capita, pop, gbd_round_id):
    """Missing locations need to be filled in with region averages."""
    avail_locs = set(data_per_capita.location_id.values)
    desired_locs = fbd_core.db.get_modeled_locations(gbd_round_id)
    missing_locs = set(desired_locs.location_id.values) - avail_locs
    if not missing_locs:
        return data_per_capita
    logger.info("These locations are missing: {}".format(missing_locs))
    parent_locs = desired_locs.query("location_id in @missing_locs")[
            "parent_id"].values
    logger.info("Children of these locations will be averaged to fill in "
                "missing data: {}".format(parent_locs))
    hierarchy = desired_locs.query(
            "parent_id in @parent_locs and location_id in @avail_locs"
        )[
            ["location_id", "parent_id"]
        ].set_index(
            "location_id"
        ).to_xarray()["parent_id"]
    hierarchy.name = "location_id"
    pop_location_slice = pop.sel(location_id=hierarchy.location_id.values)

    data = data_per_capita * pop_location_slice

    mean_data = data.sel(
            location_id=hierarchy.location_id.values
            ).groupby(hierarchy).mean("location_id")
    pop_agged = pop_location_slice.sel(
            location_id=hierarchy.location_id.values
            ).groupby(hierarchy).mean("location_id")
    mean_data_per_capita = (mean_data / pop_agged).fillna(0)
    location_da = xr.DataArray(
            desired_locs.location_id.values,
            dims="location_id",
            coords=[desired_locs.location_id.values])

    filled_data_per_capita, _ = xr.broadcast(data_per_capita, location_da)

    for missing_location in desired_locs.query(
            "location_id in @missing_locs").iterrows():
        loc_slice = {"location_id": missing_location[1].location_id}
        loc_parent_slice = {"location_id": missing_location[1].parent_id}
        filled_data_per_capita.loc[loc_slice] = (
                mean_data_per_capita.sel(**loc_parent_slice))
    match_already_existing_locations = (
            filled_data_per_capita == data_per_capita).all()
    does_not_match_err_msg = (
           "Result should match input data for locations that are present.")
    assert match_already_existing_locations, does_not_match_err_msg
    if not match_already_existing_locations:
        logger.error(does_not_match_err_msg)
        raise MigrationError(does_not_match_err_msg)
    has_new_locations = missing_locs.issubset(
            filled_data_per_capita.location_id.values)
    does_not_have_new_locs_err_msg = (
           "Missing locations {} are still missing.".format(missing_locs))
    assert has_new_locations, does_not_have_new_locs_err_msg
    if not has_new_locations:
        logger.error(does_not_have_new_locs_err_msg)
        raise MigrationError(does_not_have_new_locs_err_msg)
    return filled_data_per_capita

def _merge_too_specific_locations(data):
    """Locations that are too specific (i.e. level 4) need to be merged into
    their respective parent locations."""
    avail_locs = set(data.location_id.values)
    level_four_locs = fbd_core.db.get_locations_by_level(4)
    too_specific_locs = set(level_four_locs.location_id) & avail_locs
    if len(too_specific_locs) == 0:  # nothing to merge to parent.  just return input.
        return data
    logger.info("These locations are too specific: {}".format(
        too_specific_locs))
    children_into_parents = level_four_locs.query(
            "location_id in @too_specific_locs"
        )[
            ["location_id", "parent_id"]
        ].set_index(
            "location_id"
        ).to_xarray()["parent_id"]
    children_into_parents.name = "location_id"
    children_merged = data.sel(
            location_id=children_into_parents.location_id
        ).groupby(children_into_parents).sum("location_id")
    good_locations = avail_locs - too_specific_locs
    good_data = data.sel(location_id=list(good_locations))
    merged_data = sum(broadcast_and_fill(children_merged, good_data,
                                         fill_value=0))
    unchanged_locs = good_locations - set(children_into_parents.values)
    good_locs_didnt_change = (
        data == merged_data.sel(location_id=list(unchanged_locs))).all()
    good_locs_did_change_err_msg = (
           "Error: good locations were changed during the merge.")
    assert good_locs_didnt_change, good_locs_did_change_err_msg
    if not good_locs_didnt_change:
        logger.error(good_locs_did_change_err_msg)
        raise MigrationError(good_locs_did_change_err_msg)
    return merged_data



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", type=str,
        help="Which version of migration to balance.")
    parser.add_argument(
        "--gbd_round_id", type=int, required=True,
        help="Which gbd_round_id to use in file loading and saving")
    args = parser.parse_args()

    subnat_location_ids = get_location_metadata(
                                            location_set_id=LOCATION_SET_ID,
                                            gbd_round_id=args.gbd_round_id,
                                            release_id=RELEASE_ID).\
                                            query("level == 4").\
                                            location_id.tolist()
    # Try to load data, else combine csvs into dataarray
    # Csvs are from if you split in R, xarray from split in Python
    try:
        mig_dir = FBDPath("FILEPATH")
        mig_path = mig_dir / "migration_split.nc"
        mig_da = open_xr(mig_path)
    except: # Data doesn't yet exist
        mig_da = combine_and_save_mig(version=args.version)

    balanced_mig_da = balance_migration(mig_da)

    # add missing locations
    location_ids = get_location_metadata(location_set_id=LOCATION_SET_ID,
                                         gbd_round_id=args.gbd_round_id,
                                         release_id=RELEASE_ID).\
                                         query("level == 4 | level == 3").\
                                         location_id.to_list()
    balanced_mig_da = add_missing_locs(balanced_mig_da, location_ids)
    summary = compute_summary(balanced_mig_da)

    # Save to forecasting directory
    balanced_path = mig_dir / "migration.nc"
    summary_path = mig_dir / "summary.nc"

    save_xr(balanced_mig_da, balanced_path, metric="number", space="identity")
    save_xr(summary, summary_path, metric="number", space="identity")
