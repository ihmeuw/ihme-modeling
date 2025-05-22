"""
Pull DisMod results and compute incidence for non shock ecodes.

**Context**:

  The pipeline begins by pulling incidence, remission, and EMR from bested 
  DisMod models for the current decomp step. Dismod models both incidence for
  fatal and non fatal injuries that requires inpatient care.  We only produce
  estimates for non fatal injuries so we remove fatal incidence with the 
  following formula:

  :math:`inc_{adjusted}=inc_{DisMod}*e^{-\frac{emr}{remission}}` 

  This gives non-fatal e-code incidence that requires inpatient care.

  The pipeline also models injuries requiring outpatient care. (Note: in the
  pipeline inpatient and outpatient are referred to as the "platform". A estimate
  for an injury with platform="inpatient" is an inpatient injury estimate).
  As of GBD 2019, we produce outpatient coefficients (the ratio of outpatient
  cases to inpatient) using a separate process outside the pipeline. For 
  example, a coefficient of 1/2 means there are half as many outpatient cases 
  as there are inpatient. We multiply these coefficients by incidence to 
  produce outpatient incidence estimates.

**Q**: *Why do we interpolate certain non shock ecodes to produce yearly 
estimates instead of using DisMods 5 year binned estimates?*

  In short, interpolated results are used for shocks (injuries that have
  unpredictably large changes in incidence due to things like disasters or war),
  which are modeled yearly instead of in 5 year age bins. DisMod produces
  estimates in 5 year bins (essentially averaging estimates across 5 years). For
  non shock injuries this is fine but for shocks we don't do this, we attempt to
  model them for every year to capture large and sudden changes.

**Output**:

  The output of this step is incidence by ecode. Incidence is split by year, sex
  and ecode

**Overview**:

  1. Pull measures

     2. *Is the ecode an IM ratio?* No:

        2.1. Pull DisMod incidence, EMR and remissions estimates as mort_draws. 
             Requires a bested DisMod model.

     2. *Is the ecode an IM ratio?* Yes:

         2.2. Interpolate incidence, EMR and remissions using Dismod results.
              Requires a bested DisMod model.

  3. At this point we have total incidence for all inpatient injury cases. 
     Now, remove fatal incidence from results. This produces our non-fatal 
     short term incidence estimates for inpatient cases.

  5. Apply outpatient to inpatient coefficients to our inpatient incidence 
     to get outpatient incidence

  6. Save our results, short term incidence by inpatient/outpatient

  7. If our ecode is an IM ratio
 
     7.1 Download mortality rates. These are used to calculate incidence
         to mortality ratios (which are applied to shocks)

"""
import time
import sys
import os
import numpy as np
import pandas as pd
import xarray as xr

import db_queries as db
from get_draws.api import get_draws
from chronos import interpolate

from db_tools.ezfuncs import query
from FILEPATH.pipeline import config
from FILEPATH.helpers import (calculate_measures,
                                      demographics,
                                      inj_info,
                                      paths,
                                      converters,
                                      utilities)

from FILEPATH.pipeline.helpers.pipeline_logging import InjuryLogger as logging
log = logging.getLogger(__name__)

# ---------------------------------CONSTANTS-------------------------------------
INTERPOLATE_THREADS = 45

# Get measure ids for incidence, remission and emr from the central DB
_ids = db.get_ids(table='measure')
INC_ID = int(_ids.loc[_ids["measure_name"] == "Incidence", 'measure_id'].iloc[0])
RMS_ID = int(_ids.loc[_ids["measure_name"] == "Remission", 'measure_id'].iloc[0])
EMR_ID = int(_ids.loc[_ids["measure_name"] == "Excess mortality rate", 'measure_id'].iloc[0])
# -------------------------------------------------------------------------------


def remove_fatal_incidence_cases(data_arr):
    """ Calculates the incidence of non fatal injuries

    Parameters
    ----------
    data_arr : xr.DataArray
        A DataArray with dimensions for incidence, remission
        and emr measures

    Returns
    -------
    xr.DataArray
        Short term non-fatal incidence for inpatient cases

    """
    raw_incidence = data_arr.sel(measure_id=INC_ID)
    remission     = data_arr.sel(measure_id=RMS_ID)
    emr           = data_arr.sel(measure_id=EMR_ID)

    # Output point estimates for quick debugging/validation
    mean_inc = raw_incidence.mean().item()
    mean_rms = remission.mean().item()
    mean_emr = emr.mean().item()
    log.debug(f"Incidence: {mean_inc}")
    log.debug(f"Remission: {mean_rms}")
    log.debug(f"EMR:       {mean_emr}")
    log.debug(f"Adjusted:  {mean_inc * np.exp(-1 * (mean_emr / mean_rms))}")

    adj_inc = raw_incidence * np.exp(-1 * (emr / remission))
    return adj_inc.drop_vars('measure_id')


def apply_outpatient_coeffs(ecode, inpatient):
    """
    Parameters
    ----------
    inpatient : xr.DataArray
        A DataArray with inpatient incidence

    Before the pipeline runs the ratio of outpatient to inpatient is
    calculated for all age groups:
    Outpatient incidence is calculated by multiplying inpatient incidence by
    the outpatient to inpatient ratio.
    """
    # Load and log coefficients
    out_coeff = pd.read_csv(paths.OUTPATIENT_COEFFICIENTS / f"{ecode}.csv")
    #log.debug(f"Outpatient Coefficients\n{out_coeff}")
    # Map age groups to outpatient coefficients
    age_to_out_coeff = dict(zip(out_coeff.age_group_id, out_coeff.out_coeff))

    # Copy the inpatient incidence and multiply it by the coefficients to get
    # outpatient incidence
    outpatient = inpatient.copy()
    for age_group_id in inpatient.coords['age_group_id'].values:
        coeff = age_to_out_coeff[age_group_id]
        outpatient.loc[{'age_group_id': age_group_id}] *= coeff

    return xr.concat([inpatient, outpatient],
                     pd.Index(['inpatient', 'outpatient'], name='platform'))


def best_model_version(me_id):
    try:
        return db.get_best_model_versions(
            entity="modelable_entity", 
            ids=me_id, 
            release_id=config.RELEASE_ID)
    except:
        raise ValueError(f"No bested model exists for {me_id}...")

def get_latest_model_version(meid, release_id):
    
    q = """SELECT 
               modelable_entity_id, 
               model_version_id,
               release_id
           FROM 
               epi.model_version
           WHERE 
               modelable_entity_id IN :meid AND release_id IN :release_id
            ORDER BY model_version_id DESC
           LIMIT 1
        """

    df = query(q,
               conn_def="epi",
               parameters={'meid': [meid], 'release_id': [release_id]})
    latest_version = df['model_version_id'].iloc[0]
    
    return latest_version
    
def get_measures_draws(me_id, sex, year, mvid):
    """
    Gets incidence, remission and emr from the best dismod model
    """
    log.debug(f"Getting draws...")
    draws = get_draws(
        gbd_id_type  = "modelable_entity_id",
        gbd_id       = me_id,
        location_id  = demographics.LOCATIONS,
        year_id      = year,
        sex_id       = sex,
        age_group_id = demographics.AGE_GROUPS,
        source       = "epi",
        measure_id   = [INC_ID, RMS_ID, EMR_ID],
        num_workers  = 4,
        release_id   = config.RELEASE_ID,
        version_id   = mvid
    )
    dropcols = ['modelable_entity_id', 'model_version_id', 'metric_id']
    draws.drop(dropcols, axis=1, inplace=True)

    indexcols = ['measure_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']
    draws.set_index(indexcols, inplace=True)

    return draws

def get_measures_interpolate(me_id, sex, year_start, year_end, mvid):
    """
    Gets interpolated incidence, remission and EMR using the bested DisMod model.
    Note interpolating for years before 1990 (1980 - 1990) is a bit tricky since the 
    function doesn't allow year_end to be 1990, therefore we have to use 1995 as year_end
    We will only save data < 1990
    """
    # Setup interpolate arguments
    log.debug(f'Year {year_start}, year end {year_end}')

    # Interpolate data for incidence
    dfs = []
    for measure_id in [INC_ID, RMS_ID, EMR_ID]:
        log.debug(f"Interpolating draws for measure_id {measure_id}")
        
        start = time.time()
        tmp = interpolate(
            gbd_id_type          = 'modelable_entity_id',
            gbd_id               = me_id,
            measure_id           = measure_id,
            location_id          = demographics.LOCATIONS,
            sex_id               = sex,
            age_group_id         = demographics.AGE_GROUPS,
            reporting_year_start = year_start,
            reporting_year_end   = year_end,
            source               = 'epi',
            release_id           = config.RELEASE_ID,
            num_workers          = INTERPOLATE_THREADS,
            version_id           = mvid
        )
        # Interpolate includes the last year. Since dismod already reports that
        # we can just drop it. (e.g. interpolate has 1990-1995 but dismod
        # estimates 1995 so we drop the interpolated year)
        if year_start == 1980:
            tmp = tmp.loc[tmp.year_id < 1990]
        else:
            tmp = tmp.loc[tmp.year_id < year_end]
        
        log.debug(f"Finished: {time.time() - start:0.2f}")
        dfs.append(tmp)

    # Combine dataframes
    df = pd.concat(dfs)
    del dfs  # Save space

    df = df.loc[df.year_id < year_end]
    dropcols = ['modelable_entity_id', 'model_version_id', 'metric_id']
    df.drop(dropcols, axis=1, inplace=True)

    # Format the data before returning it
    indexcols = ['measure_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']
    df.set_index(indexcols, inplace=True)

    return df

def get_measures(ecode, sex, year, version, folder):
    """
    Gets incidence, remission and EMR data from shared functions. The measures 
    gets interpolated measures if the ecode is an IM ratio and a parent ecode
    there are years we need to interpolate for. If it's not an IM ratio 
    it gets normal dismod draws if the ecode isn't an IM ratio
    """
    me_id = utilities.get_me(ecode)
    log.debug(f'MEID {me_id}')
    bested_mvid = db.get_best_model_versions(entity="modelable_entity", ids=me_id, release_id = 16)
    check_release = bested_mvid['release_id'].iloc[0]
    
    # This is when using models that are out of rotation
    if check_release != config.RELEASE_ID:
        log.debug("Bested mvid does not match current release_id")
        mvid = get_latest_model_version(me_id, config.RELEASE_ID)
        log.debug(f'Use latest model version {mvid} instead')
    else:
        mvid = bested_mvid['model_version_id'].iloc[0]
        log.debug(f"Use bested model version {mvid}")
    
    # Save model version id to a txt file
    mvid_path = folder / "epi_version.txt"
    # Check if the codem_version.txt file already exists
    if not os.path.exists(mvid_path):
        with open(mvid_path, 'w') as file:
            file.write(str(mvid))
        log.debug(f"epi model version {mvid} saved to epi_version.txt")
    
    if ecode in inj_info.IM_RATIO_ECODES and year < config.CURRENT_YEAR and ecode in inj_info.ECODE_PARENT:
        if year == 1980:
            year_end = 1995
        else:
            year_end = demographics.YEARS_EPI[demographics.YEARS_EPI.index(year) + 1]
        if year_end - year > 1:
            log.debug('Ecode is IMR, interpolate')
            draw_df = get_measures_interpolate(me_id, sex, year, year_end, mvid)
        else:
            draw_df = get_measures_draws(me_id, sex, year, mvid)
    
    else:
        draw_df = get_measures_draws(me_id, sex, year, mvid)
        
    return converters.df_to_xr(draw_df, wide_dim_name='draw', fill_value=np.nan)
    
def write_results(arr, ecode, version, sex, year, folder):
    # Save every year of incidence data we have
    for year in arr.year_id.values:
        filepath = folder / f"{ecode}_{year}_{sex}.nc"
        log.debug(f"Writing results to {filepath}")
        arr.loc[{'year_id': [year]}].to_netcdf(filepath, mode='w')

def save_mortality(ecode, sex, year, version):
    """
    Downloads mortality from codem using the bested model.
    The injuries team is responsible for running codem models for road injuries, d
    rowning, and interpersonal violence. These mortality rates are used for 
    generating imr for shocks. The mort rates are produced with CodCorrect.
    """
    log.debug("Pulling Mortality...")
    cause_id = utilities.get_cause(ecode)
    mort_draws = get_draws(
        gbd_id_type  = "cause_id",
        gbd_id       = cause_id,
        location_id  = demographics.LOCATIONS,
        year_id      = year,
        sex_id       = sex,
        age_group_id = demographics.AGE_GROUPS,
        source       = "codem",
        release_id   = config.RELEASE_ID,
    )
    codem_version = mort_draws.model_version_id.unique()

    # Draws are counts of mortality. We need rates. Divide draw (mortality count)
    # by population to get mortality rate.
    mort_draws[utilities.drawcols()] = mort_draws[utilities.drawcols()].divide(mort_draws['population'], axis=0)

    # Cleanup the results
    mort_draws.drop(['population', 'envelope', 'cause_id', 'sex_name', 'measure_id', 
                    'metric_id', 'model_version_id'], axis=1, inplace=True)
    mort_draws.set_index(['location_id','year_id','sex_id','age_group_id'], inplace=True)
    mort = converters.df_to_xr(mort_draws, wide_dim_name='draw', fill_value=np.nan)
    del mort_draws  # Save memory

    # Save mortality data
    log.debug(f"Writing Mortality")
    folder = 'FILEPATH'
    if not folder.exists():
        try:
            folder.mkdir(parents=True)
        except:
            pass
    mort.to_netcdf(folder / f"mort_{year}_{sex}.nc")
    
    codem_version_file_path = 'FILEPATH'
    # Check if the codem_version.txt file already exists
    if not os.path.exists(codem_version_file_path):
        with open(codem_version_file_path, 'w') as file:
            file.write(str(codem_version))
        log.debug(f"Codem version {codem_version} saved to codem_version.txt")
    else:
        log.debug("Codem version file already exists. Skipping write.")
    
    
    
def main(ecode, year_id, sex_id, version):
    
    base_folder = 'FILEPATH'
    if not base_folder.exists():
        try:
            base_folder.mkdir(parents=True)
        except:
            pass

    if year_id in demographics.YEARS_EPI or year_id == 1980:
        data = get_measures(ecode, sex_id, year_id, version, base_folder)
        data = remove_fatal_incidence_cases(data)
        data = apply_outpatient_coeffs(ecode, data)
        write_results(data, ecode, version, sex_id, year_id, base_folder)
    else:
        log.debug(f'{year_id} is not an epi year, skip getting incidence')

    # Download mortality to make IM ratios for IM parent ecodes
    if ecode in inj_info.IM_RATIO_ECODES:
        save_mortality(ecode, sex_id, year_id, version)

 
if __name__ == '__main__':

    #logging.printConfig(log)
    
    log.debug(f"Step Arguments: {sys.argv}")
    # Arguments from sys.argv are all strings. Cast them so they're correct
    # types to prevent errors with shared functions
    ecode   = str(sys.argv[1])
    year_id = int(sys.argv[2])
    sex_id  = int(sys.argv[3])
    version = str(sys.argv[4])

    log.info("START")
    start = time.time()

    main(ecode, year_id, sex_id, version)
    
    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
    