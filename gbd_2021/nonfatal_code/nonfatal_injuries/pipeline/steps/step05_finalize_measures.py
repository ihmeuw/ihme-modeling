"""
Prepare measures to be uploaded to EPI.

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


**Output**:

  HD5 files ready for upload! -

  FILEPATH

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

        5.2. *Do we have a short term ncdoe?* Yes:

              5.2.1. Subtract short term prevalence from our long term 
                     prevalence

        5.3. Do not allow certain outpatient long-term (due to expert opinion)

        5.4. (Assumption) Delete under 1 long-term prevalence of shocks

        5.5. Subtract long term animal contact

        5.6. Save long term prevalence

"""
import numpy as np
import os
import pandas as pd
import sys
import xarray as xr
import time

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import utilities, calculate_measures, load_measures, inj_info, paths, demographics
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)


def read_ode(ecode, ncode, year_id, version, platform='all'):
    """Read results of the shock/non-shock ODE solver."""
    if platform == 'all':
        if ncode in inj_info.LT_OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()

    path_base = FILEPATH
    p_list = []
    for p in platform:
        tmp = xr.open_dataarray(FILEPATH)
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
    animal_df = pd.read_csv(FILEPATH)
    animal_df = animal_df.loc[animal_df.ecode == ecode]

    # get lists of ncodes we want to remove
    if inj_info.ECODE_PARENT[ecode] == 'inj_animal':
        inp = animal_df.loc[animal_df.inpatient == 1, 'ncode'].unique().tolist()
        otp = animal_df.loc[animal_df.inpatient == 0, 'ncode'].unique().tolist()
    else:
        print("This isn't an animal ecode, no need to make adjustments")
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
        out_dir = FILEPATH
        if not out_dir.exists():
            out_dir.mkdir(parents=True)

        data_columns = ['location_id',
                        'year_id',
                        'sex_id',
                        'age_group_id']
        result.to_hdf(FILEPATH)


def write_results(arr, version, ecode, ncode, year_id, measure_id):
    """Writes the results for a given df/measure_id in a format ready to upload
    to the epi database."""

    out_dir = FILEPATH
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    filepath = FILEPATH

    arr = arr.sum(dim='platform')
    df = arr.to_dataset(dim='draw').to_dataframe()
    df.reset_index(inplace=True)
    # Fix for first run of 2020 - delete
    if 'measure_id' in df.columns:
        df.drop(columns=['measure_id'], inplace=True)

    save_columns = ['location_id', 'year_id', 'sex_id', 'age_group_id']

    path = out_dir / FILEPATH
    print(f"Saving {path}")
    df.to_hdf(
        path_or_buf  = path,
        key          = "draws",
        mode         = "w",
        format       = 'table',
        data_columns = save_columns
    )
    

def main(ecode, ncode, version):
    if ncode in inj_info.ST_NCODES:
        print("Getting durations, percent treated, and disability weights...")
        if ecode in inj_info.SHOCK_ECODES:
            year_set = 'full'
        else:
            year_set = 'all'
        log.debug("pct_treated")
        pct_treated = calculate_measures.pct_treated(year_id=year_set)  # defaults to 10% min treated, 75 haqi cap
        log.debug("durations")
        durations = calculate_measures.get_durations(pct_treated, ncode=ncode)
        log.debug("dws")
        dws = load_measures.disability_weights_st().loc[{'ncode': ncode}]

    if ecode in inj_info.SHOCK_ECODES:
        dems = db.get_demographics(gbd_team='cod', gbd_round_id=config.GBD_ROUND)
        years = [y for y in dems['year_id'] if y >= 1990]
    else:
        dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
        years = dems['year_id']

    for year in years:
        print("----------------------------------")
        print(f"Working on {year}")
        sys.stdout.flush()

        # get incidence, prevalence, and ylds for both in and outpatient, then
        # collapse over platform incidence
        print("(1): Get short-term split incidence & write collapsed incidence results.")
        incidence = load_measures.short_term_incidence_split(ecode, version, ncode, year)
        write_results(incidence, version, ecode, ncode, year, measure_id=6)

        if ncode in inj_info.ST_NCODES: 

            
            print("(2): Get short-term prevalence & write results.")
            st_prevalence = calculate_measures.compute_prevalence(incidence, durations)
            write_results(st_prevalence, version, ecode, ncode, year, measure_id=35)

            
            print("(3): Get YLDs & write results.")
            ylds = calculate_measures.short_term_ylds(st_prevalence, dws)
            write_results(ylds, version, ecode, ncode, year, measure_id=3)

        if ncode in inj_info.LT_NCODES:
            raw_lt = read_ode(ecode, ncode, year, version)
            
            if ncode in inj_info.ST_NCODES:
              
                fake_lt = get_fake_long_term(ncode, year, st_prevalence)
                fake_lt = fake_lt.loc[{'ncode': ncode}].drop('ncode')

                if 'ecode' in fake_lt.coords:
                    fake_lt = fake_lt.drop('ecode')

                real_lt = raw_lt - fake_lt
                real_lt.values[real_lt.values < 0] = 0

            else:
                real_lt = raw_lt.copy()


            if ncode in ["N48", "N26", "N11", "N19", "N43", "N25", "N23"]:
                real_lt.loc[{'platform': ['outpatient']}] = 0
            if ecode in inj_info.SHOCK_ECODES:
                real_lt.loc[{'age_group_id': demographics.UNDER_1_AGE_GROUPS}] = 0

            if ecode == "inj_animal_nonven" or ecode == "inj_animal_venom":
                real_lt = animal_adjustment(real_lt, ecode,
                                            ncode)  # TODO: outpatient N48, N43, N25, N23, N19, N26 have already been zeroed
                
            write_results(real_lt, version, ecode, ncode, year, measure_id=36)

            sys.stdout.flush()


if __name__ == "__main__":
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = str(sys.argv[1])
    ncode    = str(sys.argv[2])
    version  = int(sys.argv[3])

    log.info("START")
    start = time.time()

    main(ecode, ncode, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
