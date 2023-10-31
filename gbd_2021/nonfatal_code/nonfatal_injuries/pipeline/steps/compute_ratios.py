"""
Computes incidence mortality ratios.

**Context**:

  Some of the injuries that we process have unpredictably large changes. For
  example, disasters related injuries will have a large increase in a certain
  region if that region experiences a natural disaster. These are referred to as
  "shocks" or "non fatal discontinuities". To account for the volatility, shocks
  are processed differently from non shocks. While non shocks use DisMod's 5 
  year age bins (which essentially average results over 5 years), we don't do 
  this for shocks, we attempt to model them for every year to capture the large
  and sudden changes. So for disasters, if there was a hurricane in 2009 our 
  estimates will reflect and increase in disaster related incidence in 2009 
  instead of a smaller increase from 2005-2010.

  We do not have direct data on the incidence of non-fatal injury due to shocks.
  But, we do have shock mortality estimates for different causes. So, we work
  around this by taking related non-shock ecodes (e.g. road injuries, drowning 
  for water related disasters) and computing their incidence mortality ratios 
  (IM ratios or IMR). We then multiply the IM ratios by the shock mortality data
  to get incidence for the shocks.

**Output**:

  The output of this step is a single NetCDF containing IM ratios.

**Overview**:

  1. For each IM ratio ecode,

     2. For each year we need ratios for,

        3. For each sex,

           4. Load the incidence and mortality data for the given ecode, year, 
              sex

           5. Compute the ratio of incidence and mortality to get an IM ratio

           6. Store these ratios and continue to the next sex, year or ecode...

  7. Combine all ratios across ecodes, year and sex into a single object

  8. Save the ratios to a single file.

"""
import os
import pandas as pd
import sys
import time
import xarray as xr

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import demographics, utilities, inj_info, paths
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)


def get_best_ecode_version(ecode):
    versions = [
        int(file.name)
        for file in (FILEPATH)
    ]
    print(FILEPATH)
    return max(versions)


def load_mortality(ecode, version, year_id, sex_id):
    """
    Loads mortality data
    """
    in_dir = FILEPATH
    return xr.open_dataarray(FILEPATH)


def load_incidence(ecode, version, year_id, sex_id):
    """
    Loads incidence data
    """
    in_dir = FILEPATH
    return xr.open_dataarray(FILEPATH)


def compute_ratio(ecode, version, year, sex):
    """Computes incidence mortality ratios for the given ecode, ecode version,
    year and sex.

    Parameters
    ----------
    ecode : Ecode
        An ecode
    version : int
        A ecode version run
    year : int
        The year to compute ratios for.
    sex : int
        The sex_id to compute ratios for.

    Returns
    -------
    xr.DataArray
        Incidence mortality ratios.

    """
    log.debug(f"Creating ratios for {ecode} - {year} - {sex}")
    mortality = load_mortality(ecode, version, year, sex)
    incidence = load_incidence(ecode, version, year, sex)

    return incidence / mortality


def summarize(raw_ratios, super_region):
    """
    Take the average over sex, year, and location (but within a super region)
    """
    super_region_groups = raw_ratios.groupby(super_region)
    return super_region_groups.mean(dim=['location_id', 'sex_id', 'year_id'])


def get_region_map():
    """
    Creates a map between given location_ids and GBD super regions
    """
    reg = db.get_location_metadata(location_set_id=35, gbd_round_id=config.GBD_ROUND)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(demographics.LOCATIONS)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()


def write_results(ratios, version):
    """
    Saves the given ratios for the given version.
    """
    folder = FILEPATH

    log.debug(f"Writing results to {folder}")
    if not folder.exists():
        os.makedirs(folder)
    ratios.to_netcdf(FILEPATH)


def main(version):
    # get location ids indexed by super region
    regmap = get_region_map()

    # Iterate over ecode, year and sex and compute ratios. Then, combine
    # all the ratios into a single xr.dataarray.
    ecode_arrays = []
    for ecode in inj_info.IM_RATIO_ECODES:
        # Use the best ecode version to pull data from
        best_ecode_version = get_best_ecode_version(ecode)
        log.debug(f"Pulling version {best_ecode_version} for {ecode}")
        
        year_arrays = []
        for year in demographics.YEARS_COD:
            if year < 1990: continue
            sex_arrays = []
            for sex in demographics.SEXES:
                sex_arrays.append(
                    compute_ratio(ecode, best_ecode_version, year, sex)
                )

            # Aggregeat by sex
            combined_sexes = xr.concat(sex_arrays,'sex_id')
            year_arrays.append(combined_sexes)

        # Aggregate by year and reduce to GBD super region level
        combined_years = xr.concat(year_arrays, 'year_id')
        summarized = summarize(combined_years, regmap)
        ecode_arrays.append(summarized)

    # Aggregate by ecode
    final_ratios = xr.concat(ecode_arrays,
                             pd.Index(inj_info.IM_RATIO_ECODES, name='ecode'))
    write_results(final_ratios, version)


if __name__ == '__main__':
    log.debug(f"Step Arguments: {sys.argv}")
    version = sys.argv[1]
    log.info("START")
    start = time.time()

    main(version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
