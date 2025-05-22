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

    
**How to run this script**:
    1. Run compute_ratios_launch.py so ratios can be generated for all imr codes. 
    This script generates ratios by ecode and sex.

"""
import os
import pandas as pd
import sys
import time
import xarray as xr
import getpass
import db_queries as db

user = getpass.getuser()
from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import demographics, utilities, inj_info, paths
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

# Initialize logging 
log = logging.getLogger(__name__)
logging.printConfig(log)

def load_mortality(ecode, version, year_id, sex_id):
    """
    Loads mortality data
    """
    in_dir = 'FILEPATH'
    return xr.open_dataarray(in_dir / f"mort_{year_id}_{sex_id}.nc")


def load_incidence(ecode, version, year_id, sex_id):
    """
    Loads incidence data
    """
    in_dir = 'FILEPATH'
    return xr.open_dataarray(in_dir / f"{ecode}_{year_id}_{sex_id}.nc")

"""Load either mortality or incidence data.
    
    Parameters:
    - ecode: the ecode for the injury type
    - version: version of the data
    - year_id: year of data
    - sex_id: male or female
    - data_type: 'mortality' or 'incidence'

    Returns:
    - xarray DataArray of the requested data
    """

def compute_ratio(ecode, version, year, sex_id):
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
    log.debug(f"Creating ratios for {ecode} - {year} - {sex_id}")
    mortality = load_mortality(ecode, version, year, sex_id)
    incidence = load_incidence(ecode, version, year, sex_id)

    return incidence / mortality

def summarize(raw_ratios, super_region_map):
    """
    Compute the average IM ratio by super region to reduce outlier impact.
    
    Parameters:
    - raw_ratios: raw IM ratio data
    - super_region_map: map of location IDs to super region IDs

    Returns:
    - Averaged xarray DataArray
    """
    log.debug("Summarizing ratios by super region")
    raw_ratios = raw_ratios.copy()
    raw_ratios.coords['location_id'] = super_region_map.loc[raw_ratios.coords['location_id']]
    raw_ratios = raw_ratios.rename({'location_id': 'super_region_id'})
    return raw_ratios.groupby('super_region_id').mean(dim=['super_region_id'])



def get_region_map():
    """
    Creates a map between given location_ids and GBD super regions
    """
    reg = db.get_location_metadata(location_set_id=35, release_id=config.RELEASE_ID)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(demographics.LOCATIONS)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()

def write_results(ratios, version, sex_id):
    """
    Saves the given ratios for the given version.
    """
    folder = paths.DATA_DIR / config.DECOMP / 'inc_mortality_ratios' / str(version)

    log.debug(f"Writing results to {folder}")
    if not folder.exists():
        os.makedirs(folder)
    ratios.to_netcdf(folder / f'imr_{ecode}_{sex_id}.nc')


def main(ecode, sex, input_version, output_version):

    # Iterate over year and compute ratios. 
    # Then, combine all the ratios into a single xr.dataarray
    
    year_arrays = []
    for year in demographics.YEARS_COD:
        year_arrays.append(compute_ratio(ecode, input_version, year, sex))

    # Aggregate by year and reduce to GBD super region level
    ratios_by_loc = xr.concat(year_arrays, 'year_id')
    # get location ids indexed by super region
    regmap = get_region_map()
    final_ratios = summarize(ratios_by_loc, regmap)
    
    # Write results
    # Saves the given ratios for the given version.
    folder = paths.DATA_DIR / config.DECOMP / 'inc_mortality_ratios' / str(output_version)
    log.debug(f"Writing results to {folder}")
    if not folder.exists():
        os.makedirs(folder)
    final_ratios.to_netcdf(folder / f'imr_{ecode}_{sex}.nc')
    
    # Save incidence version 
    inc_version_file_path = folder /f"{ecode}_incidence_version.txt"
    if not os.path.exists(inc_version_file_path):
        with open(inc_version_file_path, 'w') as file:
            file.write(str(input_version))
        log.debug(f"Input version {input_version} saved to ecode_incidence_version.txt")
    else:
        log.debug("Version file already exists. Skipping write.")
    

if __name__ == '__main__':
   
    log.debug(f"Step Arguments: {sys.argv}")
    ecode          = sys.argv[1]
    sex            = sys.argv[2]
    input_version  = sys.argv[3]
    output_version = sys.argv[4]

    log.info("START")
    start = time.time()
    main(ecode, sex, input_version, output_version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
    
    
   
