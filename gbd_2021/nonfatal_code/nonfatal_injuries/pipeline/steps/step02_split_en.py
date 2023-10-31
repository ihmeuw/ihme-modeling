"""
Apply EN matrices to incidence by ecode to get incidence by ecode and ncode

**Output**:

  NetCDF files saved by ecode, platform, year and sex

**Overview**:

  1. Load a map from location to high/low income

  2. Load incidence data for the given ecode

  3. Load EN matrix

  4. For each Ncode
    
     5. Apply the EN matrix for the current ncode to the incidence data to get
        incidence of the ecode leading to the current ncode.

     6. If the given ecode is a parent ecode then scale the incidence of all 
        child causes to match incidence of the parent ecode.

     7. Save the results.
"""
import os
import sys
import time
import pandas as pd
import xarray as xr

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import (converters, utilities, input_manager,
                                      load_measures, inj_info, paths,
                                      demographics)
from gbd_inj.types import Ecodes

from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)


def get_income_map():
    """ Get a binary variable for high income or not. Just based on the GBD
    super region. This is used for merging on the E-N matrices that are split
    by income."""
    income = db.get_location_metadata(
        location_set_id=config.LOCATION_SET_ID,
        gbd_round_id=config.GBD_ROUND
    )[["location_id", "super_region_name"]]
    
    income.loc[:,"high_income"] = 0
    income.loc[income["super_region_name"] == "High-income", 'high_income'] = 1
    
    income.drop(['super_region_name'], inplace=True, axis=1)
    income = income.loc[income['location_id'].isin(demographics.LOCATIONS)]
    return income.set_index('location_id')['high_income'].to_xarray()


def load_incidence(ecode, year_id, sex_id, platform, version):
    """ Loads incidence data from the previous step. If the ecode is parent ecode,
    then incidence for the parent and the child ecodes are returned.
    """
    ecodes = [ecode]
    if ecode in inj_info.PARENT_ECODES:
        ecodes.extend(inj_info.ECODE_CHILD[ecode])

    # Get the ecode versions
    versions = [
        input_manager.get_input(Ecodes.by_name(ecode)).id
        for ecode in ecodes
    ]

    # Collect all the .nc files for parent and child ecodes
    incidence = []
    for e, version in zip(ecodes, versions):
        # Find the folder to pull data from
        folder = FILEPATH
        folder = FILEPATH
        ecode_incidence = xr.open_dataarray(FILEPATH)
        ecode_incidence = ecode_incidence.loc[{'platform': [platform]}]
        ecode_incidence = ecode_incidence.assign_coords(ecode=e)
        incidence.append(ecode_incidence)
        
    # return all incidence data
    return xr.concat([
        data
        for data in incidence
    ], dim='ecode')
    
def load_en_matrix(ecode, sex_id, platform):
    """
    Returns the EN matrix for the given ecode and sex. 
    """
    # TODO - Remove this when we start using the new EN matrix
    ecode = 'inj_poisoning' if inj_info.ECODE_PARENT[ecode] == 'inj_poisoning' else ecode
    
    platform = 'inp' if platform == "inpatient" else 'otp'

    ecodes = [ecode]
    if ecode in inj_info.PARENT_ECODES:
        ecodes.extend(inj_info.ECODE_CHILD[ecode])
        
    matrix = pd.concat([
        pd.read_csv(FILEPATH)
        for e in ecodes
    ])

    # Format the EN matrix:
    # 1) Subset by sex
    # 2) Convert from normal ages to gbd_age_group_id
    # 3) Expand under 1 and under 5 age groups
    matrix = matrix.loc[matrix['sex'] == sex_id]
    matrix.loc[:,'age_group_id'] = matrix.age.replace(demographics.YEARS_TO_AGE_GROUP_ID)
    matrix = utilities.expand(matrix, "age_group_id", 2, demographics.UNDER_1_AGE_GROUPS)
    matrix = utilities.expand(matrix, "age_group_id", 238, demographics.UNDER_5_AGE_GROUPS)
    
    # Format EN matrix to look like our incidence data so they can be merged
    matrix.rename(columns={
        'n_code': 'ncode',
        'inpatient': 'platform',
        'sex': 'sex_id'
    }, inplace=True)
    matrix.loc[:,'platform'] = matrix.platform.replace({
        1: 'inpatient',
        0: 'outpatient'
    })
    matrix.set_index(['ecode', 'ncode', 'platform', 'high_income','sex_id', 'age_group_id'], inplace=True)

    return converters.df_to_xr(matrix, wide_dim_name='draw', fill_value=0)


def apply_matrix(incidence, matrix, income):
    """ Splits incidence by ecode into incidence by ecode and ncode"""
    split = incidence.groupby(income) * matrix
    return split.drop('high_income')


def scale(incidence, parent_ecode):
    """ Given a datarray of incidence for a parent ecode and it's children,
    the child incidence is scaled such that the total child incidence matches
    the parent incidence
    """
    # child_ecodes is used to select all child ecode incidence
    child_ecodes = list(inj_info.ECODE_CHILD[parent_ecode])

    # Find the scale factor, that is the amount we need to scale each child
    # by to match the parent.
    child_incidence  = incidence.loc[{'ecode': child_ecodes}]
    parent_incidence = incidence.loc[{'ecode': [parent_ecode]}]
    scale_factor     = parent_incidence / child_incidence.sum(dim='ecode')

    # Since parent_incidence was selected using the ecode dimensions it still
    # has an ecode dimension, which is transfered to the scale_factor array.
    # squeeze removes this dimension.
    scale_factor = scale_factor.squeeze() 

    # Scale each child cause
    for child_ecode in child_ecodes:
        incidence.loc[{'ecode': [child_ecode]}] *= scale_factor

    # Some values may be NA due to division in the scale_factor calculation
    return incidence.fillna(0)


def save_netcdf(ds, path, **kargs):
    # There's been this weird issue with saving netCDFs where they
    # ranodomly have locking issues and fail. It's a random bug so we
    # can fix it by trying to save multiple times
    for i in range(10):
        try:
            ds.to_netcdf(path, **kargs)
            return
        except:
            log.debug(f"Error saving to {path}. Trying again...")
            if path.exists(): os.remove(path)
    raise Exception(f"Error saving to {path}")
            

def main(ecode, year_id, sex_id, platform, version):
    # Load data
    income_map = get_income_map()
    log.debug("Loaded map")
    incidence  = load_incidence(ecode, year_id, sex_id, platform, version)
    log.debug("Loaded inc")
    en_matrix  = load_en_matrix(ecode, sex_id, platform)
    log.debug("Loaded en")

    save_mode = 'w'
    # For every n code we have, take our total incidence and multiply it by the
    # % of cases that lead to that ncode, giving total incidence of the e/n pair.
    for ncode in inj_info.get_ncodes(platform):
        log.debug(f"Applying the matrix for {ncode}")

        n_matrix = en_matrix.loc[{'ncode': [ncode]}]
        result = apply_matrix(incidence, n_matrix, income_map)
        log.debug("applied matrix")

        # If the ecode has child causes then scale the child causes to match the
        # parent code.
        if ecode in inj_info.PARENT_ECODES:
            result = scale(result, ecode)
            log.debug("scaled child causes")

        # Save results by ecode
        for e in result.ecode.values:
            ecode_incidence = result.loc[{'ecode': e}].drop('ecode')

            # Get the path to where the ecodes data should be saved
            version = input_manager.get_input(Ecodes.by_name(e)).id
            save_folder = FILEPATH
            if not save_folder.exists(): save_folder.mkdir(parents=True)
            
            save_file = save_folder / FILEPATH
            save_netcdf(ecode_incidence, save_file, mode=save_mode, group=ncode)
        # Swap to append mode after we create the file
        log.debug("saved files")
        save_mode = 'a'


if __name__ == '__main__':
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = str(sys.argv[1])
    year_id  = int(sys.argv[2])
    sex_id   = int(sys.argv[3])
    platform = str(sys.argv[4])
    version  = int(sys.argv[5])

    log.info("START")
    start = time.time()

    main(ecode, year_id, sex_id, platform, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
