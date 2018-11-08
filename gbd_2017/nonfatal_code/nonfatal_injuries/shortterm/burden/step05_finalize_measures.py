import db_queries as db
import sys
import os
import pandas as pd
import numpy as np
import xarray as xr
from gbd_inj.inj_helpers import help, calculate_measures, load_measures, inj_info, paths


def read_ode(ecode, ncode, year_id, version, platform='all'):
    """Read results of the shock/non-shock ODE solver."""
    if platform == 'all':
        if ncode in inj_info.LT_OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()
    
    p_list = []
    for p in platform:
        in_dir = os.path.join("FILEPATH")
        filepath = os.path.join(in_dir, "FILEPATH.nc".format(year_id))
        p_list.append(xr.open_dataarray(filepath))  # location_id, year_id, sex_id, age_group_id, platform, draw

    return xr.concat(p_list, dim='platform')


def get_fake_long_term(ncode, year_id, st_prev):
    """Get fake long-term draws by multiplying HAQI-lt_probs """
    
    lt_probs = calculate_measures.long_term_probs_combined(ncode=ncode, year_id=year_id)
    
    return lt_probs * st_prev


def animal_adjustment(arr, ecode, ncode):
    animal_file = "FILEPATH.csv"
    animal_df = pd.read_csv(animal_file)
    # get lists of ncodes we want to remove
    if inj_info.ECODE_PARENT[ecode] == 'inj_animal':
        inp = animal_df.loc[(animal_df['ecode'] == ecode) & (animal_df['inpatient'] == 1), 'ncode'].unique().tolist()
        otp = animal_df.loc[(animal_df['ecode'] == ecode) & (animal_df['inpatient'] == 0), 'ncode'].unique().tolist()
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
    spinal_split_folder = 'FILEPATH'
    drawdict = {'prop_' + d: d for d in help.drawcols()}
    parent = inj_info.ECODE_PARENT[ecode]
    filename = 'FILEPATH.h5'.format(year_id)
    
    for s in ['a', 'b', 'c', 'd']:
        # load proportion draws
        split_prop = pd.read_csv(os.path.join(spinal_split_folder, 'FILEPATH.csv'))
        split_prop.rename(columns=drawdict, inplace=True)
        split_prop.drop('acause', axis=1, inplace=True)
        
        # split the ncode
        result = df * split_prop.loc[0]
        result.reset_index(inplace=True)
        
        # save
        out_dir = os.path.join("FILEPATH")
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir)
            except OSError, e:
                if e.errno != os.errno.EEXIST:
                    raise
                pass
        
        result.to_hdf(os.path.join(out_dir, filename), 'draws', mode='w', format='table',
                      data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id'])


def write_results(arr, version, ecode, ncode, year_id, measure_id):
    """Writes the results for a given df/measure_id. These are saved in a format ready for uploading to the epi database."""
    arr = arr.sum(dim='platform')

    df = arr.to_dataset(dim='draw').to_dataframe()
    out_dir = os.path.join("FILEPATH")
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass

    filename = "FILEPATH.h5".format(measure_id, year_id)
    filepath = os.path.join(out_dir, filename)

    df.reset_index(inplace=True)
    df.to_hdf(filepath, "draws", mode="w", format='table',
              data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id'])


def main(ecode, ncode, version):
    start = help.start_timer()
    
    if ncode in inj_info.ST_NCODES:
        print("Getting durations, percent treated, and disability weights...")
        if ecode in inj_info.SHOCK_ECODES:
            year_set = 'full'
        else:
            year_set = 'all'
        pct_treated = calculate_measures.pct_treated(year_id=year_set)  # defaults to 10% min treated, 75 haqi cap
        durations = calculate_measures.get_durations(pct_treated, ncode=ncode)
        dws = load_measures.disability_weights_st().loc[{'ncode': ncode}]

    if ecode in inj_info.SHOCK_ECODES:
        dems = db.get_demographics(gbd_team='cod', gbd_round_id=help.GBD_ROUND)
        years = [y for y in dems['year_id'] if y >= 1990]
    else:
        dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
        years = dems['year_id']

    for year in years:
        print("----------------------------------")
        print("Working on {}".format(year))
        sys.stdout.flush()
        # get incidence, prevalence, and ylds for both in and outpatient, then collapse over platform
        # incidence
        print("(1): Get short-term split incidence & write collapsed incidence results.")
        incidence = load_measures.short_term_incidence_split(ecode, version, ncode, year)
        write_results(incidence, version, ecode, ncode, year, measure_id=6)
    
        if ncode in inj_info.ST_NCODES:  # only run st prev and ylds on short term ncodes
        
            # prevalence - we will also use this for fake long term if it's a long term ncode
            print("(2): Get short-term prevalence & write results.")
            st_prevalence = calculate_measures.compute_prevalence(incidence, durations)
            write_results(st_prevalence, version, ecode, ncode, year, measure_id=35)
        
            # ylds
            print("(3): Get YLDs & write results.")
            ylds = calculate_measures.short_term_ylds(st_prevalence, dws)
            write_results(ylds, version, ecode, ncode, year, measure_id=3)
        
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
    
            else:
                real_lt = raw_lt.copy()

            # Expert adjustments
            # 1. Do not allow certain outpatient long-term
            # 2. Delete under 1 lt prevalence of shocks
            # 3. Subtract weird long-term animal contact

            if ncode in ["N48", "N26", "N11", "N19", "N43", "N25", "N23"]:
                real_lt.loc[{'platform': ['outpatient']}] = 0
            if ecode in inj_info.SHOCK_ECODES:
                real_lt.loc[{'age_group_id': [2, 3, 4]}] = 0

            if ecode == "inj_animal_nonven" or ecode == "inj_animal_venom":
                real_lt = animal_adjustment(real_lt, ecode,
                                            ncode)

            write_results(real_lt, version, ecode, ncode, year, measure_id=36)
            sys.stdout.flush()
    print('All done!')

    help.end_timer(start)
