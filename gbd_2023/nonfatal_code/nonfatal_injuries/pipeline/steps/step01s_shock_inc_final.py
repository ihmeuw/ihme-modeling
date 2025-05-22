"""
Computes incidence for shock ecodes.

**Context**:

  The first step in the pipeline is producing incidence estimates for different
  ecodes. Some of our ecodes have unpredictably large changes. For
  example, disasters related injuries will have a large increase in a certain
  region if that region experiences a natural disaster. These are referred to as
  "shocks" or "non fatal discontinuities". To account for the volatility, shocks
  are processed differently from non shocks. While non shocks use dismod's 5 
  year age bins (which essentially average results over 5 years), we don't do 
  this for shocks, we attempt to model them for every year to capture the large
  and sudden changes. So example, if there was a hurricane in 2009 our estimates
  for disaster will reflect the increase in disaster related incidence in 2009
  instead of an increase in incidence from 2005-2010.

  We do not have direct data on the incidence of non-fatal injury due to shocks.
  But, we do have shock mortality estimates for different causes. So, we work
  around this by taking related non-shock ecodes (e.g. road injuries, drowning 
  for water related disasters) and computing their incidence mortality ratios 
  (IM ratios or IMR). We then multiply the IM ratios by the shock mortality 
  data to get incidence for the shocks. 
  
  The shocks mortality estimates (disaster, war_warterror and war_execution are 
  obtained from codem models launched by the cod team and they are produced at 
  each CodCorrect run.

  This script is responsible for combining the IM ratios, mortality estimates 
  and information on when/where shocks have occurred to produce incidence.

  Note:
  - IM ratios used to adjust disasters has a special process. We use road injury
    IM ratios for all age/sex/location/years but for location/years that had
    a water related disaster we also use drowning IM ratios. We do this by
    averaging the two IM ratios. We get water related disaster information
    from the shocks team. Specifically, we get events from the EMDAT database.
  - Incidence is estimated by multiplying mortatlity rates with ratios. 
    The mortality estimates are NOT by platform, but the ratios data are and 
    that's how we get incidence by inpatient and outpatient
  - The IMRatio is by age/sex/years/super region. Super region ratios get 
    applied for all of the most-detailed locs that fall within that super region.

**Output**:

  A netcdf file with shock incidence by ecode, year and sex.

**Overview**:

  1. Load population and mortality counts and compute mortality rates

  2. Load IMRatio data

  3. Compute Incidence: 
     :math:`incidence= Mortality*\frac{Incidence}{Mortality}=Mortality*IMRatio`

  4. Save incidence
"""
import time
import sys
import numpy as np
import pandas as pd
import xarray as xr

import db_queries as db
from get_draws.api import get_draws as gd

from FILEPATH.pipeline import config
from FILEPATH.pipeline.helpers import (demographics,
                                      inj_info,
                                      paths,
                                      converters,
                                      utilities,
                                      input_manager
)
from gbd_inj.FILEPATH.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)

# ---------------------------------CONSTANTS-------------------------------------
WATER_CAUSE_IDS = [988, 989]  # cataclysmic storm and flood. Used by disasters. 

# Disasters also use drowning, see notes above for more info.
RATIO_ECODES = {
    'inj_disaster'      : ['inj_trans_road'], 
    'inj_war_warterror' : ['inj_homicide', 'inj_trans_road'],
    'inj_war_execution' : ['inj_homicide']
}
# -------------------------------------------------------------------------------

def get_loc_ids():
    """
    Returns a df mapping location ids with their super region ids
    """
    df = db.get_location_metadata(location_set_id=config.LOCATION_SET_ID, release_id=config.RELEASE_ID)
    df = df[['location_id', 'super_region_id']]
    df = df.loc[df.location_id.isin(demographics.LOCATIONS)]
    return df
    
def get_shock_mort(ecode, year, sex):
    """Computes mortality count for a given ecode and set of population data"""
    
    log.debug("Getting mort rates")
    draws = gd(
        gbd_id_type  = "cause_id",
        gbd_id       = utilities.get_cause(ecode),
        sex_id       = sex,
        version_id   = config.COD_VERSION, 
        location_id  = demographics.LOCATIONS,
        age_group_id = demographics.AGE_GROUPS,
        year_id      = year,
        source       = "codcorrect",
        measure_id   = config.CODEM_MEASURE_ID,
        metric_id    = 1, #Count is default for codcorrect
        downsample   = config.DOWNSAMPLE,
        n_draws      = config.DRAWS,
        #num_workers  = 15,
        release_id   = config.RELEASE_ID
        )

    # Assumption: Executions don't happen for infants under 1 year of age 
    if ecode == 'inj_war_execution':
        under_1_row_mask = draws.age_group_id.isin(demographics.UNDER_1_AGE_GROUPS)
        draws.append(under_1_row_mask, utilities.drawcols(),0)

    # Format the draws
    draws.drop(['cause_id', 'measure_id', 'metric_id', 'version_id'],
               axis=1, inplace=True)
    
    # insert super_region_ids
    super_region_ids = get_loc_ids()
    draws = draws.merge(super_region_ids, on = 'location_id')
    
    # convert to array
    draws.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id'],
                    inplace=True)
    draws_arr = converters.df_to_xr(draws, wide_dim_name='draw', fill_value=np.nan)
    
    return draws_arr
  
def get_population(year, sex):
    log.debug("Load population data for the given year and sex")
    
    flat_version = str(config.DEM_RUN_ID)
    pop_path = 'FILEPATH'
    pops = xr.open_dataset(pop_path)
    pops = pops.loc[{'year_id': [year], 'sex_id': [sex]}]
    return pops['population']


def get_IM_ratios(ecode, sex, year, water):
    
    IM_folder = 'FILEPATH'
    ratio_ecodes = RATIO_ECODES[ecode]
    
    # inj_disaster
    if water:
        ratio_ecodes += ['inj_drowning']
        log.debug(f'Extending ecode {ratio_ecodes}')
    
    # inj_war_warterror and inj_disaster (only water events)
    if len(ratio_ecodes) > 1:
        IMR_data = []
        for e in ratio_ecodes:
            IM_path = 'FILEPATH.nc"
            imr_arr = xr.open_dataarray(IM_path)
            IMR_data.append(imr_arr)

        IMR_data = xr.concat(IMR_data, dim = 'ecode')
        # Average IM ratios across all ecodes that are used for the current shock
        print(f"Averaging ratios of {ratio_ecodes}")
        IMR_data = IMR_data.mean(dim = 'ecode')

    # inj_war_execution
    else:
        print(f"Getting ratios from {ratio_ecodes}")
        IM_path = f"{IM_folder}/imr_{ratio_ecodes[0]}_{sex}.nc"
        IMR_data = xr.open_dataarray(IM_path)
    
    IMR_data = IMR_data.loc[{'year_id': [year]}]
    
    return IMR_data
    

def main(ecode, year_id, sex_id, version):
  
    # Take mortality counts and divide it by population to get the mortality rate
    mortality_counts  = get_shock_mort(ecode, year_id, sex_id)
    population_counts = get_population(year_id, sex_id)
    # Get mortality rate
    mortality_rate    = mortality_counts / population_counts
    del mortality_counts
    del population_counts
  
    IM_ratios = get_IM_ratios(ecode, sex_id, year_id, water=False)

    if ecode == 'inj_disaster':
        log.debug('Getting water related loc for disaster')
        IM_ratios_water = get_IM_ratios(ecode, sex_id, year_id, water=True)

        # Get all locations for water related disasters for the given year.
        disasters = pd.read_csv(paths.SHOCKS_FILE)
        disaster_locations = disasters.loc[
            (disasters['cause_id'].isin(WATER_CAUSE_IDS)) &
            (disasters['year_id'] == year_id)].location_id.unique()
        log.debug(f"Length of disaster locations {len(disaster_locations)}")
        
        non_disaster_locations = list(set(demographics.LOCATIONS) - set(disaster_locations))

        if len(disaster_locations > 0):
            log.debug('Merging disaster locs by regions')
            incidence = xr.concat([
                mortality_rate.loc[{'location_id': disaster_locations}] * IM_ratios_water,
                mortality_rate.loc[{'location_id': non_disaster_locations}] * IM_ratios
            ], dim="location_id")
        else:
            log.debug("Compute incidence for disaster")
            # If we are predicting values for the future we won't get any water events...
            incidence  = mortality_rate * IM_ratios
    else:
        log.debug("Compute incidence")
        incidence  = mortality_rate * IM_ratios

    # Cap incidence at 0.5. - Sometimes ratios can create a unrealistic
    # incidence if mortality is unusually high
    inc = incidence.copy()
    inc = inc.where(inc < .5, .5)
    inc = inc.drop_vars('super_region_id')
    inc = inc.sel(super_region_id=0) # Hack to drop this dimension and coordinate

    # Save incidence
    folder = (FILEPATH / ecode / str(version)/ ecode /f"shock_ecode_inc")
    if not folder.exists():
        try:
            folder.mkdir(parents=True)
        except:
            pass
    
    filepath = folder / f"{ecode}_{year_id}_{sex_id}.nc"
    log.debug(f"Writing results to {filepath}")
    inc.to_netcdf(filepath, mode='w')
    

if __name__ == '__main__':
    
    log.debug(f"Step Arguments: {sys.argv}")
    ecode   = str(sys.argv[1])
    year_id = int(sys.argv[2])
    sex_id  = int(sys.argv[3])
    version = str(sys.argv[4])

    log.info("START")
    start = time.time()

    main(ecode, year_id, sex_id, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")