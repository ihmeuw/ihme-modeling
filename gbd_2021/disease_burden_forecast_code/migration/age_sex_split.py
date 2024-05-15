"""
This script works to split the migration into separate age-sex groups
by converting the Eurostat data to xarray.

Example:

.. code:: bash

    python FILEPATH/age_sex_split.py
    --migration_version click_20210510_limetr_fixedint
    --pattern_version 20191114_eurostat_age_sex_pattern_w_subnat
    --gbd_round_id 6

"""

import argparse
import pandas as pd
import xarray as xr

from db_queries import get_location_metadata
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_data_transformation.lib.pandas_to_xarray import df_to_xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

# Which location ids to use
WPP_LOCATION_IDS = pd.read_csv(
                         "FILEPATH.csv"
                         )["location_id"].unique().tolist()
# get subnational location ids
PATTERN_ID_VARS = ["age_group_id", "sex_id"]
LOCATION_SET_ID = 39
RELEASE_ID = 6
QATAR_LOCS = [151, 152, 140, 156, 150] #'QAT', 'SAU', 'BHR', 'ARE', 'OMN'

def create_age_sex_xarray(gbd_round_id, pattern_version, subnat_location_ids):
    logger.debug("Creating xarray of age-sex patterns for migration")
    # load patterns
    # location of the pattern for the Qatar-modeled countries
    QATAR_PATTERN = f"FILEPATH/qatar_pattern.csv"
    # Location of the pattern for other countries
    EUROSTAT_PATTERN = f"FILEPATH/eurostat_pattern.csv"
    qatar = pd.read_csv(QATAR_PATTERN)
    eurostat = pd.read_csv(EUROSTAT_PATTERN)
    # convert to xarrays
    qatar = df_to_xr(qatar, dims=PATTERN_ID_VARS)
    eurostat = df_to_xr(eurostat, dims=PATTERN_ID_VARS)
    # create superarray to hold all locs
    all_locs_xr_list = []
    # Put dataframes for each location into a list
    for loc in WPP_LOCATION_IDS + subnat_location_ids:
        if loc in QATAR_LOCS:
            data = qatar
        else:
            data = eurostat
        data = expand_dimensions(data, location_id=[loc])
        all_locs_xr_list.append(data)
    # Concat all locations together
    pattern = xr.concat(all_locs_xr_list, dim='location_id')
    # Save all locs pattern
    logger.debug("Saving age-sex pattern xarray")
    pattern_dir = FBDPath('FILEPATH')
    pattern_path = pattern_dir / f"combined_age_sex_pattern.nc"
    save_xr(pattern, pattern_path, metric="percent", space="identity")
    logger.debug("Saved age-sex pattern xarray")
    return pattern

def main(migration_version, gbd_round_id, pattern_version):
    # load age-sex pattern (loc, draw, age, sex)
    logger.debug("Loading age-sex migration pattern")
    subnat_location_ids = get_location_metadata(gbd_round_id=gbd_round_id,
                                                location_set_id=LOCATION_SET_ID,
                                                release_id=RELEASE_ID).\
                                                query("level == 4").\
                                                location_id.tolist()
    try:
        pattern_dir = FBDPath('FILEPATH')
        pattern_path = pattern_dir / "combined_age_sex_pattern.nc"
        pattern = open_xr(pattern_path)
    except: # Data doesn't yet exist
        pattern = create_age_sex_xarray(gbd_round_id,
                                        pattern_version,
                                        subnat_location_ids)
    # load migration counts (loc, draw, year)
    logger.debug("Loading migration data")
    mig_dir = FBDPath("FILEPATH")
    mig_path = mig_dir / "mig_counts.nc"
    migration = open_xr(mig_path)
    migration = migration.squeeze(drop=True)
    # end up with migration counts with age and sex (loc, draw, year, age, sex)
    split_data = migration * pattern
    # Save it
    logger.debug("Saving age-sex split migration data")

    split_path = mig_dir / "migration_split.nc"
    save_xr(split_data, split_path, metric="number", space="identity")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--migration_version", type=str, required=True,
        help="Which version of migrations to use in WPP directory")
    parser.add_argument(
        "--gbd_round_id", type=int, required=True,
        help="Which gbd_round_id to use in file loading and saving")
    parser.add_argument(
        "--pattern_version", type=str, required=True,
        help="Which age-sex pattern version to use in future migration \
        directory")
    args = parser.parse_args()

    main(migration_version=args.migration_version,
        gbd_round_id=args.gbd_round_id,
        pattern_version=args.pattern_version)
