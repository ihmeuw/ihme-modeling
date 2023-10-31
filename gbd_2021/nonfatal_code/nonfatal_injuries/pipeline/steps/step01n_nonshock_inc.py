"""
Pull DisMod results and compute incidence for non shock ecodes.

**Context**:

  The pipeline begins by pulling incidence, remission, and EMR from bested 
  DisMod models for the current decomp step. Dismod models both incidence for
  fatal and non fatal injuries that requires inpatient care.  We only produce
  estimates for non fatal injuries so we remove fatal incidence with the 
  following formulat:

  :math:`inc_{adjusted}=inc_{DisMod}*e^{-\frac{emr}{remission}}` 

  This gives non-fatal e-code incidence that requires inpatient care.

  The pipeline also models injuries requiring outpatient care.

**Output**:

  The output of this step is incidence by ecode. Incidence is split by year, sex
  and ecode

**Overview**:

  1. Pull measures

     2. *Is the ecode an IM ratio?* No:

        2.1. Pull DisMod incidence, EMR and remissions estimates as mort_draws. 
             Requires a bested DisMod model for the current decopm step.

     2. *Is the ecode an IM ratio?* Yes:

         2.2. Interpolate incidence, EMR and remissions using Dismod results.
              Requires a bested DisMod model for the current decomp step.

  3. At this point we have total incidence for all inpatient injury cases. 
     Now, remove fatal incidence from results. The produces our non-fatal 
     short term incidence estimates for inpatient cases.

  5. Apply outpatient to inpatient coeffecients to our inpatient incidence 
     to get outpatient incidence

  6. Save our results, short term incidence by inpatient/outpatient

  7. If our ecode is an IM ratio
 
     7.1 Download mortality rates. These are used to calculate incidence
         to mortality ratios (which are applied to shocks)

"""
import time
import sys

import numpy as np
import pandas as pd
import xarray as xr

import db_queries as db
import get_draws.api as gd
import chronos.interpolate as interpolate

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import (calculate_measures,
                                      demographics,
                                      inj_info,
                                      paths,
                                      converters,
                                      utilities
)
from gbd_inj.pipeline.infrastructure import pipeline_steps
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging
log = logging.getLogger(__name__)
logging.printConfig(log)

# ---------------------------------CONSTANTS-------------------------------------
# Get measure id's for incidence, remission and emr from the central DB
_ids = db.get_ids(table='measure')
INC_ID = int(_ids.loc[_ids["measure_name"] == "Incidence", 'measure_id'].iloc[0])
RMS_ID = int(_ids.loc[_ids["measure_name"] == "Remission", 'measure_id'].iloc[0])
EMR_ID = int(_ids.loc[_ids["measure_name"] == "Excess mortality rate", 'measure_id'].iloc[0])
# -------------------------------------------------------------------------------


def remove_fatal_incidence_cases(data_arr):
    """ Calculates the incidence of non faltal injuries

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
    return adj_inc.drop('measure_id')


def apply_outpatient_coeffs(ecode, inpatient):
    """
    Parameters
    ----------
    inpatient : xr.DataArray
        A DataArray with inpatient incidence

    Outpatient incidence is calculated by multiplying inpatient incidence by
    the outpatient to inpatient ratio.
    """
    # Load and log coefficients
    out_coeff = pd.read_csv(FILEPATH)
    #log.debug(FILEPATH)
    # Map age groups to outpatient coeffecients
    age_to_out_coeff = dict(zip(out_coeff.age_group_id, out_coeff.out_coeff))

    # Copy the inpatient icidence and multiply it by the coefficients to get
    # outpatient incididence
    outpatient = inpatient.copy()
    for age_group_id in inpatient.coords['age_group_id'].values:
        coeff = age_to_out_coeff[age_group_id]
        outpatient.loc[{'age_group_id': age_group_id}] *= coeff

    return xr.concat([inpatient, outpatient],
                     pd.Index(['inpatient', 'outpatient'], name='platform')
    )


def best_model_version(me_id):
    try:
        return db.get_best_model_versions(
            entity="modelable_entity", 
            ids=me_id, 
            gbd_round_id=config.GBD_ROUND, 
            decomp_step=config.DECOMP
        ).model_version_id.iloc[0]
    except:
        raise ValueError(f"No bested model exists for {me_id}...")

def get_measures(ecode, sex, year):
    """
    Gets incidence, remission and emr from the best dismod model for the current
    decomp step.
    """
    me_id = utilities.get_me(ecode)
    bested_model = best_model_version(me_id)
    log.debug(f"Pulling draws for all measures from model {bested_model} ...")
    draws = gd.get_draws(
        gbd_id_type  = "modelable_entity_id",
        gbd_id       = me_id,
        location_id  = demographics.LOCATIONS,
        year_id      = year,
        sex_id       = sex,
        age_group_id = demographics.AGE_GROUPS,
        status       = "best",
        source       = "epi",
        measure_id   = [INC_ID, RMS_ID, EMR_ID],
        downsample   = config.DOWNSAMPLE,
        n_draws      = config.DRAWS,
        num_workers  = pipeline_steps.step_1.resources.threads,
        gbd_round_id = config.GBD_ROUND,
        decomp_step  = config.DECOMP
    )
    model_version = draws.model_version_id.iloc[0]
    log.debug(f"Finished pulling model {model_version}")

    dropcols = ['modelable_entity_id', 'model_version_id', 'metric_id']
    draws.drop(dropcols, axis=1, inplace=True)

    indexcols = ['measure_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']
    draws.set_index(indexcols, inplace=True)

    return converters.df_to_xr(
        draws,
        wide_dim_name='draw',
        fill_value=np.nan
    )

def write_results(arr, ecode, version, year_id, sex_id):
    folder = (FILEPATH)
    if not folder.exists():
        folder.mkdir(parents=True)

    # Save every year of incidence data we have
    for year in arr.year_id.values:
        filepath = FILEPATH
        log.debug(f"Writing results to {filepath}")
        arr.loc[FILEPATH)


def main(ecode, year_id, sex_id, version):
    if year_id in demographics.YEARS_EPI:
        data = get_measures(ecode, sex_id, year_id)
        data = remove_fatal_incidence_cases(data)
        data = apply_outpatient_coeffs(ecode, data)
        write_results(data, ecode, version, year_id, sex_id)

            
if __name__ == '__main__':
    logging.printConfig(log)
    
    log.debug(f"Step Arguments: {sys.argv}")
    # Arguments from sys.argv are all strings. Cast them to they're correct
    # types to prevent erros with shared functions
    ecode   = str(sys.argv[1])
    year_id = int(sys.argv[2])
    sex_id  = int(sys.argv[3])
    version = int(sys.argv[4])
    
    log.info("START")
    start = time.time()

    ecodes = [ecode]
    # If were running a parent ecode run this step for all the child ecodes
    if ecode in inj_info.PARENT_ECODES:
        ecodes.extend(inj_info.ECODE_CHILD[ecode])

    for e in ecodes:
        main(ecode, year_id, sex_id, version)
    
    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
    
