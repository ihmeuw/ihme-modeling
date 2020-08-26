import os
import time
import sys

import numpy as np
import pandas as pd

import db_queries as db
import get_draws.api as gd
from fbd_core import etl
import chronos.interpolate as interpolate

from gbd_inj.inj_helpers import help, calculate_measures, inj_info, paths


def get_measures_get_draws(ecode, locs, years, sexes, ages, inc_id, rms_id, emr_id, decomp):
    me_id = help.get_me(ecode)
    
    best_version = db.get_best_model_versions(entity="modelable_entity",
                                              ids=me_id,
                                              status="best",
                                              decomp_step=decomp,
                                              gbd_round_id=help.GBD_ROUND)
    draws = gd.get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=me_id,
        location_id=locs,
        year_id=years,
        sex_id=sexes,
        age_group_id=ages,
        status="best",
        source="epi",
        measure_id=[inc_id, rms_id, emr_id],
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp
    )
    
    dropcols = ['modelable_entity_id', 'model_version_id', 'metric_id']
    draws.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    draws.set_index(indexcols, inplace=True)
    
    incidence = draws.loc[draws['measure_id'] == inc_id].drop('measure_id', axis=1)
    remission = draws.loc[draws['measure_id'] == rms_id].drop('measure_id', axis=1)
    emr = draws.loc[draws['measure_id'] == emr_id].drop('measure_id', axis=1)

    emr = emr.reindex(incidence.index)
    
    return {'incidence': incidence, 'remission': remission, 'emr': emr}


def get_measures_interpolate(ecode, locs, sexes, ages, inc_id, rms_id, emr_id, year_start, year_end, decomp):
    me_id = help.get_me(ecode)
    best_version = db.get_best_model_versions(entity="modelable_entity",
                                              ids=me_id,
                                              status="best",
                                              decomp_step=decomp,
                                              gbd_round_id=help.GBD_ROUND)

    me_id = int(me_id)
    inc_id = int(inc_id)
    rms_id = int(rms_id)
    emr_id = int(emr_id)
    n_workers = 30

    start = time.time()
    inc = interpolate.interpolate(
        gbd_id_type='modelable_entity_id',
        gbd_id=me_id,
        measure_id=inc_id,
        location_id=locs,
        sex_id=sexes,
        age_group_id=ages,
        reporting_year_start=year_start,
        reporting_year_end=year_end,
        status='best',
        source='epi',
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp,
        num_workers=n_workers,  # Make sure we have 40 threads
    )
    
    start = time.time()
    rms = interpolate.interpolate(
        gbd_id_type='modelable_entity_id',
        gbd_id=me_id,
        measure_id=rms_id,
        location_id=locs,
        sex_id=sexes,
        age_group_id=ages,
        reporting_year_start=year_start,
        reporting_year_end=year_end,
        status='best',
        source='epi',
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp,
        num_workers=n_workers  # Make sure we have 40 threads
    )
    
    start = time.time()
    emr = interpolate.interpolate(
        gbd_id_type='modelable_entity_id',
        gbd_id=me_id,
        measure_id=emr_id,
        location_id=locs,
        sex_id=sexes,
        age_group_id=ages,
        reporting_year_start=year_start,
        reporting_year_end=year_end,
        status='best',
        source='epi',
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp
    )

    inc = inc.loc[inc['year_id'] < year_end]
    rms = rms.loc[rms['year_id'] < year_end]
    emr = emr.loc[emr['year_id'] < year_end]
    
    dropcols = ['measure_id', 'metric_id', 'model_version_id',
                'modelable_entity_id']
    inc.drop(dropcols, axis=1, inplace=True)
    rms.drop(dropcols, axis=1, inplace=True)
    emr.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    inc.set_index(indexcols, inplace=True)
    rms.set_index(indexcols, inplace=True)
    emr.set_index(indexcols, inplace=True)

    emr = emr.reindex(inc.index)
    
    m_dict = {'incidence': inc, 'remission': rms, 'emr': emr}
    
    return m_dict


def save_mortality(ecode, year_id, sex_id, locs, ages, decomp, version):
    cause_id = help.get_cause(ecode)
    draws = gd.get_draws(
        gbd_id_type="cause_id",
        gbd_id=cause_id,
        location_id=locs,
        year_id=year_id,
        sex_id=sex_id,
        age_group_id=ages,
        status="best",
        source="codem",
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp
    )

    draws[help.drawcols()] = draws[help.drawcols()].divide(draws['pop'], axis=0)
    draws.drop(['pop', 'envelope', 'cause_id', 'sex_name', 'measure_id', 'metric_id'], axis=1, inplace=True)
    draws.set_index(['location_id','year_id','sex_id','age_group_id'], inplace=True)
    mort = etl.df_to_xr(draws, wide_dim_name='draw', fill_value=np.nan)

    filename = "mort_{}_{}.nc".format(str(year_id), str(sex_id))
    version = version.rstrip()
    folder = os.path.join(paths.DATA_DIR, decomp, inj_info.ECODE_PARENT[ecode], version, 'mortality_for_shocks')
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    filepath = os.path.join(folder, filename)
    mort.to_netcdf(filepath)


def get_measures(ecode, year_id, sex_id, decomp, version):
    ids = db.get_ids(table='measure')
    inc_id = ids.loc[ids["measure_name"] == "Incidence", 'measure_id'].iloc[0]
    rms_id = ids.loc[ids["measure_name"] == "Remission", 'measure_id'].iloc[0]
    emr_id = ids.loc[ids["measure_name"] == "Excess mortality rate", 'measure_id'].iloc[0]

    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)
    location_ids = dems["location_id"]
    age_group_ids = dems["age_group_id"]
    
    if ecode in inj_info.IM_RATIO_ECODES and year_id < help.LAST_YEAR:
        print(f"{ecode} uses IM ratio. Interpolating.")
        if year_id < 2015:
            year_end = year_id + 5
            mort_year_end = year_end - 1
        elif year_id == 2015:
            year_end = 2019
            mort_year_end = year_end - 1
        else:
            year_end = help.LAST_YEAR
            mort_year_end = year_end
        measure_dict = get_measures_interpolate(ecode,
                                                location_ids,
                                                sex_id,
                                                age_group_ids,
                                                inc_id,
                                                rms_id,
                                                emr_id,
                                                year_id,
                                                year_end,
                                                decomp
        )
        for year in range(year_id, mort_year_end+1):
            save_mortality(ecode,
                           year,
                           sex_id,
                           location_ids,
                           age_group_ids,
                           decomp,
                           version
            )

    else:
        measure_dict = get_measures_get_draws(ecode,
                                              location_ids,
                                              year_id,
                                              sex_id,
                                              age_group_ids,
                                              inc_id,
                                              rms_id,
                                              emr_id,
                                              decomp
        )
    
    return measure_dict


def write_results(arr, ecode, decomp, version, year_id, sex_id):
    version = version.rstrip()
    folder = paths.DATA_DIR / f"{decomp}/{inj_info.ECODE_PARENT[ecode]}/{version}/nonshock_ecode_inc"
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
        
    for year in arr.year_id.values:
        filename = '{}_{}_{}.nc'.format(ecode, str(year), str(sex_id))
        filepath = folder / filename
        print(f'Writing netCDF: {filepath}')
        arr.loc[{'year_id': [year]}].to_netcdf(filepath, mode='w')
        

        
def main(ecode, year_id, sex_id, decomp, version):
    tic = time.time()

    m_draws = get_measures(ecode, year_id, sex_id, decomp, version)
    
    x_inc = etl.df_to_xr(m_draws['incidence'],
                         wide_dim_name='draw',
                         fill_value=np.nan
    )
    x_rem = etl.df_to_xr(m_draws['remission'],
                         wide_dim_name='draw',
                         fill_value=np.nan
    )
    x_emr = etl.df_to_xr(m_draws['emr'],
                         wide_dim_name='draw',
                         fill_value=np.nan
    )

    out_coeff = pd.read_csv(paths.OUTPATIENT_COEFFICIENTS / f"{ecode}.csv")
    age_to_out_coeff = dict(zip(out_coeff.age_group_id, out_coeff.out_coeff))

    sys.stdout.flush()
    adjusted_inc = calculate_measures.short_term_incidence_unsplit(
        x_inc,
        x_rem,
        x_emr,
        age_to_out_coeff
    )

    write_results(adjusted_inc, ecode, decomp, version, year_id, sex_id)
    toc = time.time()
    total = toc - tic

