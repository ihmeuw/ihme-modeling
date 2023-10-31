"""
Computes incidence for shock ecodes.

**Context**:

  The first step in the pipeline is producing incidence estimates for different
  ecodes. Some of our ecodes have unpredictably large changes. For
  example, disasters related injuries will have a large increase in a certain
  region if that region experiences a natural disaster. These are refered to as
  "shocks" or "non fatal disconitutues". To account for the violatility, shocks
  are processed differently from non shocks. 

  This script is responsible for combining the IM ratios, mortality estimates 
  and information on when/where shocks have occured to produce incidence.


**Ouput**:

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
import get_draws.api as gd

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import (demographics,
                                      inj_info,
                                      paths,
                                      converters,
                                      utilities
)
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)

# ---------------------------------CONSTANTS-------------------------------------
WATER_CAUSE_IDS = [988, 989]  # cataclysmic storm and flood. Used by disasters. 

# Disasters also use drowning, see notes above for me info.
RATIO_ECODES = {
    'inj_disaster'      : ['inj_trans_road'], 
    'inj_war_warterror' : ['inj_homicide', 'inj_trans_road'],
    'inj_war_execution' : ['inj_homicide']
}
# -------------------------------------------------------------------------------


def get_shock_mort(ecode, year_id, sex_id):
    filepath = FILEPATH
    return xr.open_dataset(filepath)


def get_IM_ratios(ecode, water=False):
    # Load IM Ratios
    IM_path = FILEPATH
    IMR_data = xr.open_dataarray(IM_path)

    ecodes = RATIO_ECODES[ecode]
    if water:
        ecodes += ['inj_drowning']
    
    # Average IM ratios across all ecodes that are used for the current shock
    return IMR_data.loc[{'ecode': RATIO_ECODES[ecode]}].mean(dim=['ecode', 'year_id'])

def get_region_map():
    # Map location ids to their super region
    region_map = db.get_location_metadata(
        location_set_id=config.LOCATION_SET_ID,
        gbd_round_id=config.GBD_ROUND
    )
    region_map = region_map[['location_id', 'super_region_id']]
    region_map = region_map.loc[region_map.location_id.isin(demographics.LOCATIONS)]
    return region_map.set_index('location_id')['super_region_id'].to_xarray()
    

def write_results(arr, ecode, version, year_id, sex_id):
    """ Save the incidence results by ecode, year and sex."""
    

def main(ecode, year_id, sex_id, version):
    # Load data
    mortality  = get_shock_mort(ecode, year_id, sex_id)
    IM_ratios  = get_IM_ratios(ecode)
    region_map = get_region_map()

    if ecode == 'inj_disaster':
        IM_ratios_water = get_IM_ratios(ecode, water=True)

        # Get all locations for water related disasters for the given year.
        disasters = pd.read_csv(FILEPATH)
        disaster_locations = disasters.loc[
            (disasters['cause_id'].isin(WATER_CAUSE_IDS)) &
            (disasters['year_id'] == year_id)
        ].location_id.unique()
        non_disaster_locations = list(set(demographics.LOCATIONS) - set(disaster_locations))

        if len(disaster_locations > 0):
            incidence = xr.concat([
                mortality.loc[{'location_id': disaster_locations}].groupby(region_map.loc[{'location_id': disaster_locations}]) * IM_ratios_water,
                mortality.loc[{'location_id': non_disaster_locations}].groupby(region_map.loc[{'location_id': non_disaster_locations}]) * IM_ratios
            ], dim="location_id")
        else:
            # If we are predicting values for the future we won't get any water events...
            incidence  = mortality.groupby(region_map) * IM_ratios
    else:
        incidence  = mortality.groupby(region_map) * IM_ratios

    # Cap incidence at 0.5. - Sometimes ratios can create a unrealistic
    # incidence if mortality is unusally high
    incidence = incidence.where(incidence < .5, .5)
    incidence = incidence.drop('super_region_id')

    # Save incidence
    folder = FILEPATH
    if not folder.exists(): folder.mkdir(parents=True)
    incidence.to_netcdf(FILEPATH)


if __name__ == '__main__':
    log.debug(f"Step Arguments: {sys.argv}")
    ecode   = str(sys.argv[1])
    year_id = int(sys.argv[2])
    sex_id  = int(sys.argv[3])
    version = int(sys.argv[4])

    log.info("START")
    start = time.time()

    main(ecode, year_id, sex_id, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
