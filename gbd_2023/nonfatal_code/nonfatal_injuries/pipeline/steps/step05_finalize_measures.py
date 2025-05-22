"""
Prepare measures to be uploaded to EPI.

**Context**:

  Finally, in the last step we prepare all the measures for upload. Short-term
  incidence is multiplied by durations to get short-term prevalence and that is
  multiplied by short-term disability weights to get short-term YLDs.
  We also subtract fake long-term prevalence (short-term prevalence times
  long-term probabilities) from the long-term prevalence calculated in the
  previous step to account for the double counting of prevalence in the first 
  year after the injury. There are a couple other adjustments made to long-term
  prevalence as well, including the zeroing out of certain outpatient n-codes 
  (due to expert opinion), saying there's no long-term prevalence for under-1 
  age groups in shocks, and zeroing out some ncodes for the animal ecodes.

*What estimates/measures get uploaded for what?*

  1. Both long-term and short-term:

    - 3  : YLD's
    - 6  : Incidence
    - 35 : Short Term Prevalence
    - 36 : Long Term Prevalence

  2. Short term:

    - 3  : YLDS
    - 6  : Incidence
    - 35 : Short Term Prevalence

  3. Long Term:

    - 6  : Incidence
    - 36 : Long Term Prevalence

*Why don't we upload YLDs for long term ncodes?*

  After uploading our results to EPI, they are run through COMO which computes
  YLDs. These are computed by ncode, which means the EN matrix is then used to
  compute YLDs by ecode/ncode.

**Output**:

  HD5 files ready for upload! 

**Overview**:

  1. Load years from demographics (epi for non shocks, cod for shocks)

  2. *Is the ncode short term?* Yes:

     2.1. Load disability weights (dws).

  3. For every year

     4. Save incidence

     5. *Is the ncode short-term?* Yes:

         5.1. Compute and save prevalence

         5.2 Compute and save YLDS
 
     5. *Is the ncode short-term?* No, it's long-term:

        5.1. Load ODE prevalence

        5.2. *Do we have a short term ncode?* Yes:

              5.2.1. Subtract short term prevalence from our long term 
                     prevalence

        5.3. Do not allow certain outpatient long-term (due to expert opinion)

        5.4. (Assumption) Delete under 1 long-term prevalence of shocks

        5.5. Subtract long term animal contact

        5.6. Save long term prevalence.

"""
import numpy as np
import os
import pandas as pd
import sys
import xarray as xr
import time

import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

import db_queries as db

from FILEPATH.pipeline import config
from FILEPATH.pipeline.helpers import utilities, calculate_measures, load_measures, inj_info, paths, demographics
from FILEPATH.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)

# ---------------------------------CONSTANTS------------------------------------
# Value to cap estimates at. Currently we are only capping prevalence while for 
# incidence we are only doing a spot check
threshold = 0.9
# ------------------------------------------------------------------------------

def read_ode(ecode, ncode, year_id, version, platform='all'):
    """Read results of the shock/non-shock ODE solver."""
    if platform == 'all':
        if ncode in inj_info.LT_OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()

    path_base = 'FILEPATH'
    p_list = []
    for p in platform:
        tmp = "FILEPATH.nc")
        tmp['year_id'] = tmp.year_id.variable.astype(int)
        p_list.append(tmp)

    arr = xr.concat(p_list, dim='platform')
    arr['year_id'] = arr.year_id.variable.astype(int)
    return arr


def get_fake_long_term(ncode, year_id, st_prev):
    """Get fake long-term draws by multiplying HAQI-lt_probs """
    lt_probs = calculate_measures.long_term_probs_combined(ncode=ncode,
                                                           year_id=year_id
    )
    return lt_probs * st_prev


def animal_adjustment(arr, ecode, ncode):
    """Makes adjustments for animal related injuries"""
    animal_df = pd.read_csv(paths.ANIMAL_FILE)
    animal_df = animal_df.loc[animal_df.ecode == ecode]

    # get lists of ncodes we want to remove
    if inj_info.ECODE_PARENT[ecode] == 'inj_animal':
        inp = animal_df.loc[animal_df.inpatient == 1, 'ncode'].unique().tolist()
        otp = animal_df.loc[animal_df.inpatient == 0, 'ncode'].unique().tolist()
    else:
        log.debug("This isn't an animal ecode, no need to make adjustments")
        return arr

    if ncode in inp and ncode in otp:
        arr.loc[{}] = 0
    elif ncode in inp:
        arr.loc[{'platform': ['inpatient']}] = 0
    elif ncode in otp:
        arr.loc[{'platform': ['outpatient']}] = 0
    return arr


def spinal_split(df, ecode, ncode, year_id, version):
    drawdict = {'prop_' + d: d for d in utilities.drawcols()}

    for s in ['a', 'b', 'c', 'd']:
        # load proportion draws
        split_prop = pd.read_csv(paths.SPINAL_SPLIT_FOLDER/f"prop_{s}.csv")
        split_prop.rename(columns=drawdict, inplace=True)
        split_prop.drop('acause', axis=1, inplace=True)

        # split the ncode
        result = df * split_prop.loc[0]
        result.reset_index(inplace=True)

        # save
        out_dir = "FILEPATH"
        if not out_dir.exists():
            try:
                out_dir.mkdir(parents=True)
            except:
                pass

        data_columns = ['location_id',
                        'year_id',
                        'sex_id',
                        'age_group_id']
        result.to_hdf(out_dir/f"36_{year_id}.h5", 'draws', mode='w', format='table',
                      data_columns=data_columns)


def write_results(arr, version, ecode, ncode, year_id, threshold, measure_id):
    """Writes the results for a given df/measure_id in a format ready to upload
    to the epi database."""
    
    out_dir = paths.DATA_DIR/config.DECOMP/inj_info.ECODE_PARENT[ecode]/str(version)/ecode/"upload"/ecode/ncode
    if not out_dir.exists():
        try:
            out_dir.mkdir(parents=True)
        except:
            pass

    filepath = out_dir / f"{measure_id}_{year_id}.h5"
    
    # aggregate by platform before saving
    arr = arr.sum(dim='platform')
    check_for_large_value(arr, 'Platform aggregation', threshold)
    
    # For LT prev it is know that value is still high after platform aggregation
    if arr.max().data >= threshold:
        mean_value = np.mean(arr.values)
        log.warning(f"Cap prev >= {threshold} with mean value {mean_value}")
        arr.values[arr.values >= threshold] = mean_value
    
    df = arr.to_dataset(dim='draw').to_dataframe()
    df.reset_index(inplace=True)
    
    # TODO: Fix for first run of 2020 - delete if not necessary 
    if 'measure_id' in df.columns:
        df.drop(columns=['measure_id'], inplace=True)

    save_columns = ['location_id', 'year_id', 'sex_id', 'age_group_id']

    path = out_dir / f"{measure_id}_{year_id}.h5"
    log.info(f"Saving {path}")
    
    df.to_hdf(
        path_or_buf  = path,
        key          = "draws",
        mode         = "w",
        format       = 'table',
        data_columns = save_columns
    )
    
def check_for_large_value(arr, estimateName, threshold):
    """
    Checks if any values in the array exceed a specified threshold.
    
    Parameters:
    - values (array-like): The array to check for large values.
    - threshold (numeric): The value above which a value is considered large.

    If any values are found to be larger than the threshold, a warning is logged
    with the maximum value found.
    """
    if (arr > threshold).any():
        log.warning(f'{estimateName} greater than {threshold} detected')
        log.warning(f'Max {estimateName} was {arr.max().data}')

def check_n_type(n):
    """
    Logs the categorization of a given ncode as long-term, short-term, or both.
    Parameters:
    - n (str): The ncode to check against long-term and short-term categories.
    """
    
    if n in inj_info.LT_NCODES and n in inj_info.ST_NCODES:
        log.info(f'{n} is both short and long-term')
    elif n in inj_info.LT_NCODES:
        log.info(f"{n} is long-term")
    elif n in inj_info.ST_NCODES:
        log.info(f"{n} is short-term")

def main(ecode, ncode, version):
    
    check_n_type(ncode)
    
    if ncode in inj_info.ST_NCODES:
        if ecode in inj_info.SHOCK_ECODES:
            year_set = 'full'
        else:
            year_set = 'all'

        pct_treated = calculate_measures.pct_treated(year_id=year_set)  # defaults to 10% min treated, 75 haqi cap
        durations = calculate_measures.get_durations(pct_treated, ncode=ncode)
        dws = load_measures.disability_weights_st().loc[{'ncode': ncode}]

    if ecode in inj_info.SHOCK_ECODES:
        dems = db.get_demographics(gbd_team='cod', release_id=config.RELEASE_ID)
        years = [y for y in dems['year_id'] if y >= 1990]
    else:
        dems = db.get_demographics(gbd_team='epi', release_id=config.RELEASE_ID)
        years = dems['year_id']

    for year in years:
        log.info("----------------------------------")
        log.info(f"Working on {year}")
        
        # get incidence, prevalence, and ylds for both in and outpatient, then
        # collapse over platform incidence
        log.info("(1): Get short-term split incidence & write collapsed incidence results.")
        incidence = load_measures.short_term_incidence_split(ecode, version, ncode, year)
        check_for_large_value(incidence, 'Incidence', threshold)
        write_results(incidence, version, ecode, ncode, year, threshold, measure_id=6)

        # only run short term prev and ylds on short term ncodes
        if ncode in inj_info.ST_NCODES:  

            # prevalence - we will also use this for fake long term if it's a long term ncode
            log.info("(2): Get short-term prevalence & write results.")
            st_prevalence = calculate_measures.compute_prevalence(incidence, durations)
            check_for_large_value(st_prevalence, 'ST Prevalence', threshold)
            write_results(st_prevalence, version, ecode, ncode, year, threshold, measure_id=35)

            # ylds
            log.info("(3): Get YLDs & write results.")
            ylds = calculate_measures.short_term_ylds(st_prevalence, dws)
            #check_for_large_value(ylds, 'YLD', threshold)
            write_results(ylds, version, ecode, ncode, year, threshold, measure_id=3)
        
        log.info("(4): Get LT & write results.")  
        if ncode in inj_info.LT_NCODES:
            raw_lt = read_ode(ecode, ncode, year, version)
            
            if ncode in inj_info.ST_NCODES:
                # get the fake long-term draw
                fake_lt = get_fake_long_term(ncode, year, st_prevalence)
                fake_lt = fake_lt.loc[{'ncode': ncode}].drop('ncode')

                if 'ecode' in fake_lt.coords:
                    fake_lt = fake_lt.drop('ecode')

                real_lt = raw_lt - fake_lt
                real_lt.values[real_lt.values < 0] = 0
                real_lt.values[real_lt.values == np.inf] = 0 
                check_for_large_value(real_lt, 'LT fake prevalence ', threshold)
                
                # Hack to cap prevalence so it is not greater than threshold
                max_value = real_lt.max().data
                if max_value >= threshold:
                    real_lt.values[real_lt.values >= threshold] = 0.75
                    log.warning(f'Cap fake prev >= {threshold} at 0.75')
                
            else: 
                real_lt = raw_lt.copy()
              
            # now for a series of "expert" adjustments...
            # 1. Do not allow certain outpatient long-term
            # 2. Delete under 1 lt prevalence of shocks
            # 3. Subtract weird long-term animal contact
            if ncode in ["N48", "N43", "N26", "N25", "N23", "N19", "N11"]:
                real_lt.loc[{'platform': ['outpatient']}] = 0
            if ecode in inj_info.SHOCK_ECODES:
                real_lt.loc[{'age_group_id': demographics.UNDER_1_AGE_GROUPS}] = 0

            if ecode == "inj_animal_nonven" or ecode == "inj_animal_venom":
                real_lt = animal_adjustment(real_lt, ecode, ncode)  
            
            # replace infinity with zero
            real_lt.values[real_lt.values == np.inf] = 0
            check_for_large_value(real_lt, 'LT prevalence before platform aggregation', threshold)

            # Hack to cap prevalence so it is not greater than the threshold
            max_value = real_lt.max().data
            mean_value = np.mean(real_lt.values)
            
            if max_value >= threshold:
                log.warning(f"Cap prev >= {threshold} with mean value {mean_value}")
                real_lt.values[real_lt.values >= threshold] = mean_value
            
            write_results(real_lt, version, ecode, ncode, year, threshold, measure_id=36)


if __name__ == "__main__":
    
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = str(sys.argv[1])
    ncode    = str(sys.argv[2])
    version  = str(sys.argv[3])

    log.info("START")
    start = time.time()

    main(ecode, ncode, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")