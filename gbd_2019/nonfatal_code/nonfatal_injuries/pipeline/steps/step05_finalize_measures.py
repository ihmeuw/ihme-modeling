import db_queries as db
import sys
import os
import time
import pandas as pd
import numpy as np
import xarray as xr
from gbd_inj.inj_helpers import help, calculate_measures, load_measures, inj_info, paths

def read_ode(ecode, ncode, year_id, decomp, version, platform='all'):
    if platform == 'all':
        if ncode in inj_info.LT_OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()

    path_base = (paths.DATA_DIR / decomp / inj_info.ECODE_PARENT[ecode] /
                 str(version) / "ode" / ecode / ncode )
    p_list = []
    for p in platform:
        version = version.rstrip()
        in_dir = path_base / p
        filepath = in_dir / f"prev_{year_id}.nc"
        p_list.append(xr.open_dataarray(filepath))

    return xr.concat(p_list, dim='platform')


def get_fake_long_term(decomp, ncode, year_id, st_prev):
    lt_probs = calculate_measures.long_term_probs_combined(decomp=decomp,
                                                           ncode=ncode,
                                                           year_id=year_id
    )

    return lt_probs * st_prev


def animal_adjustment(arr, ecode, ncode):
    animal_file = ("FILEPATH")
    animal_df = pd.read_csv(animal_file)
    animal_df = animal_df.loc[animal_df.ecode == ecode]
    
    if inj_info.ECODE_PARENT[ecode] == 'inj_animal':
        inp = animal_df.loc[animal_df.inpatient == 1, 'ncode'].unique().tolist()
        otp = animal_df.loc[animal_df.inpatient == 0, 'ncode'].unique().tolist()
    else:
        return arr
    
    if ncode in inp and ncode in otp:
        arr.loc[{}] = 0
    elif ncode in inp:
        arr.loc[{'platform': ['inpatient']}] = 0
    elif ncode in otp:
        arr.loc[{'platform': ['outpatient']}] = 0    
    return arr


def spinal_split(df, ecode, ncode, year_id, decomp, version):
    spinal_split_folder = ("FILEPATH")
    drawdict = {'prop_' + d: d for d in help.drawcols()}
    parent = inj_info.ECODE_PARENT[ecode]
    filename = f"36_{year_id}.h5"
    
    for s in ['a', 'b', 'c', 'd']:
        split_prop = pd.read_csv(spinal_split_folder / f"prop_{s}.csv")
        split_prop.rename(columns=drawdict, inplace=True)
        split_prop.drop('acause', axis=1, inplace=True)
        
        result = df * split_prop.loc[0]
        result.reset_index(inplace=True)
        
        version = version.rstrip()
        out_dir = (paths.DATA_DIR / decomp / parent / str(version) /
                   'upload' / ecode / ncode + s)
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir)
            except OSError as e:
                if e.errno != os.errno.EEXIST:
                    raise
                pass
        
        result.to_hdf(out_dir / filename,
                      'draws',
                      mode='w',
                      format='table',
                      data_columns=['location_id',
                                    'year_id',
                                    'sex_id',
                                    'age_group_id']
        )


def write_results(arr, decomp, version, ecode, ncode, year_id, measure_id):
    version = version.rstrip()
    parent = inj_info.ECODE_PARENT[ecode]
    out_dir = (paths.DATA_DIR /
               f"{decomp}/{parent}/{version}/upload/{ecode}/{ncode}"
    )
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass

    filepath = out_dir / f"{measure_id}_{year_id}.h5"
    
    arr = arr.sum(dim='platform')
    df = arr.to_dataset(dim='draw').to_dataframe()
    df.reset_index(inplace=True)

    i = 0
    while True:
        print(f"Saving {i}")
        i += 1
        try:
            df.to_hdf(filepath, "draws", mode="w", format='table',
                      data_columns=['location_id',
                                    'year_id',
                                    'sex_id',
                                    'age_group_id']
            )
            break
        except:
            os.remove(filepath)


def main(ecode, ncode, decomp, version):
    start_time = time.time()
    
    if ncode in inj_info.ST_NCODES:
        if ecode in inj_info.SHOCK_ECODES:
            year_set = 'full'
        else:
            year_set = 'all'
        pct_treated = calculate_measures.pct_treated(decomp, year_id=year_set) 

        durations = calculate_measures.get_durations(pct_treated, ncode=ncode)
        dws = load_measures.disability_weights_st().loc[{'ncode': ncode}]

    if ecode in inj_info.SHOCK_ECODES:
        dems = db.get_demographics(gbd_team='cod', gbd_round_id=help.GBD_ROUND)
        years = [y for y in dems['year_id'] if y >= 1990]
    else:
        dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
        years = dems['year_id']

    for year in years:
        toc = time.time()
        incidence = load_measures.short_term_incidence_split(ecode, decomp, version, ncode, year)
        write_results(incidence, decomp, version, ecode, ncode, year, measure_id=6)
    
        if ncode in inj_info.ST_NCODES:
            st_prevalence = calculate_measures.compute_prevalence(incidence, durations)
            write_results(st_prevalence, decomp, version, ecode, ncode, year, measure_id=35)
        
            ylds = calculate_measures.short_term_ylds(st_prevalence, dws)
            write_results(ylds, decomp, version, ecode, ncode, year, measure_id=3)
        
        if ncode in inj_info.LT_NCODES:
            raw_lt = read_ode(ecode, ncode, year, decomp, version)

            if ncode in inj_info.ST_NCODES:
                fake_lt = get_fake_long_term(decomp, ncode, year, st_prevalence)
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
                real_lt.loc[{'age_group_id': [2, 3, 4]}] = 0

            if ecode == "inj_animal_nonven" or ecode == "inj_animal_venom":
                real_lt = animal_adjustment(real_lt, ecode,
                                            ncode) 

            write_results(real_lt, decomp, version, ecode, ncode, year, measure_id=36)
        
        tic = time.time()

