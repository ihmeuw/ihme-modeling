####################################################################
## Author: USERNAME
## Date: 08/15/17
## Purpose: Child Script for NonShock Incidence
####################################################################

# SETUP ----------------------------------------------- #

# import packages
from __future__ import division
import pandas as pd
import numpy as np
import db_queries as db
import get_draws.api as gd
import time
import os
from gbd_inj.inj_helpers import help, calculate_measures, inj_info, paths
from fbd_core import etl
import chronos.interpolate as interpolate


# COMPUTATION ---------------------------------------- #

def outpatient_cov(me_id, drawcols):
    """ This function returns a single scalar value
    for the outpatient covariate """
    # get the best model version from DisMod panda cascade
    model_version = db.get_best_model_versions(
        entity='modelable_entity',
        ids=me_id,
        status='best',
        gbd_round_id=help.GBD_ROUND)['model_version_id'].iloc[0].astype(str)
    filepath = os.path.join(
        'FILEPATH',
        model_version,
        'FILEPATH.csv.gz')
    covars = pd.read_csv(filepath)['beta_incidence_x_s_outpatient']
    covars = np.exp(covars)[-1000:]
    covars = pd.DataFrame(covars)
    covars['draw'] = drawcols
    covars.set_index('draw', inplace=True)
    x_outcov = etl.df_to_xr(covars)
    return x_outcov


def get_measures_get_draws(me_id, locs, years, sexes, ages, inc_id, rms_id, emr_id):
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
        gbd_round_id=help.GBD_ROUND)
    
    dropcols = ['modelable_entity_id', 'model_version_id', 'metric_id']
    draws.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    draws.set_index(indexcols, inplace=True)
    
    incidence = draws.loc[draws['measure_id'] == inc_id].drop('measure_id', axis=1)
    remission = draws.loc[draws['measure_id'] == rms_id].drop('measure_id', axis=1)
    emr = draws.loc[draws['measure_id'] == emr_id].drop('measure_id', axis=1)
    
    m_dict = {'incidence': incidence, 'remission': remission, 'emr': emr}
    return m_dict


def get_measures_interpolate(me_id, locs, sexes, ages, inc_id, rms_id, emr_id, year_start, year_end):
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
        gbd_round_id=help.GBD_ROUND)
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
        gbd_round_id=help.GBD_ROUND)
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
        gbd_round_id=help.GBD_ROUND)
    
    
    inc = inc.loc[inc['year_id'] < year_end]
    rms = rms.loc[rms['year_id'] < year_end]
    emr = emr.loc[emr['year_id'] < year_end]
    
    dropcols = ['measure_id', 'metric_id', 'model_version_id', 'modelable_entity_id']
    inc.drop(dropcols, axis=1, inplace=True)
    rms.drop(dropcols, axis=1, inplace=True)
    emr.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    inc.set_index(indexcols, inplace=True)
    rms.set_index(indexcols, inplace=True)
    emr.set_index(indexcols, inplace=True)
    
    m_dict = {'incidence': inc, 'remission': rms, 'emr': emr}
    
    return m_dict


def save_mortality(ecode, year_id, sex_id, locs, ages, version):
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
        gbd_round_id=help.GBD_ROUND
    )

    draws[help.drawcols()] = draws[help.drawcols()].divide(draws['pop'], axis=0)
    draws.drop(['pop', 'envelope', 'cause_id', 'sex_name', 'measure_id', 'metric_id'], axis=1, inplace=True)
    draws.set_index(['location_id','year_id','sex_id','age_group_id'], inplace=True)
    mort = etl.df_to_xr(draws, wide_dim_name='draw', fill_value=np.nan)

    filename = "FILEPATH.nc".format(str(year_id), str(sex_id))
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    filepath = os.path.join(folder, filename)
    print("Writing mortality")
    mort.to_netcdf(filepath)


def get_measures(ecode, me_id, year_id, sex_id, version):
    ids = db.get_ids(table='measure')
    inc_id = ids.loc[ids["measure_name"] == "Incidence", 'measure_id'].iloc[0]
    rms_id = ids.loc[ids["measure_name"] == "Remission", 'measure_id'].iloc[0]
    emr_id = ids.loc[ids["measure_name"] == "Excess mortality rate", 'measure_id'].iloc[0]

    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)
    location_ids = dems["location_id"]
    age_group_ids = dems["age_group_id"]

    if ecode in inj_info.IM_RATIO_ECODES and year_id < help.LAST_YEAR:
        if year_id < 2010:
            year_end = year_id + 5
            mort_year_end = year_end - 1
        else:
            year_end = help.LAST_YEAR
            mort_year_end = year_end
        measure_dict = get_measures_interpolate(me_id, location_ids, sex_id, age_group_ids, inc_id, rms_id, emr_id,
                                                year_id, year_end)

        for year in range(year_id,mort_year_end+1):
            save_mortality(ecode,year,sex_id,location_ids,age_group_ids, version)

    else:
        measure_dict = get_measures_get_draws(me_id, location_ids, year_id, sex_id, age_group_ids,
                                              inc_id, rms_id, emr_id)
    
    return measure_dict
        

def write_results(arr, ecode, version, year_id, sex_id):
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    for year in arr.year_id.values:
        filename = 'FILEPATH.nc'.format(ecode, str(year), str(sex_id))
        filepath = os.path.join(folder, filename)
        print('Writing netCDF')
        arr.loc[{'year_id': [year]}].to_netcdf(filepath)

    
def main(ecode, year_id, sex_id, version):
    tic = time.time()
    me_id = help.get_me(ecode)
    
    m_draws = get_measures(ecode, me_id, year_id, sex_id, version)
    
    x_inc = etl.df_to_xr(m_draws['incidence'], wide_dim_name='draw', fill_value=np.nan)
    x_rem = etl.df_to_xr(m_draws['remission'], wide_dim_name='draw', fill_value=np.nan)
    x_emr = etl.df_to_xr(m_draws['emr'], wide_dim_name='draw', fill_value=np.nan)
    
    otp_cov = outpatient_cov(me_id, help.drawcols())
    
    adjusted_inc = calculate_measures.short_term_incidence_unsplit(x_inc, x_rem, x_emr, otp_cov)

    write_results(adjusted_inc, ecode, version, year_id, sex_id)
    toc = time.time()
    total = toc - tic
    print("Total time was {} seconds".format(total))


if __name__ == '__main__':
    ecode = 'inj_falls'
    year_id = 1995
    sex_id = 1
    version = "1"
    repo = "FILEPATH"
    
    main(ecode, year_id, sex_id, version, repo)
