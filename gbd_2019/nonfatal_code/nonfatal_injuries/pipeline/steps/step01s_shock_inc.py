import pandas as pd
import numpy as np
import db_queries as db
import get_draws.api as gd
import time
import os
import sys
from gbd_inj.inj_helpers import help, versions, inj_info, paths
import xarray as xr
from fbd_core import etl


def get_shock_mort(ecode, pops, locs, ages, year_id, sex_id, decomp):
    cause_id = help.get_cause(ecode)

    draws = gd.get_draws(
        gbd_id_type="cause_id",
        gbd_id=cause_id,
        version_id=model_versions[ecode][sex_id],
        location_id=locs,
        year_id=year_id,
        age_group_id=ages,
        measure_id=1,
        source="codem",
        gbd_round_id=help.GBD_ROUND,
        decomp_step=model_versions['decomp']
    )
    
    if ecode == 'inj_war_execution':

        draws = draws.loc[(draws.age_group_id != 2) & (draws.age_group_id != 3),
                          ]
        
        sub = draws[draws['age_group_id'] == 4]

        oth_cols = [col for col in sub.columns if 'draw_' not in col]
        sub.set_index(oth_cols, inplace=True)
        sub[:] = 0
        sub = sub.reset_index()
        sub['age_group_id'] = 2

        draws = draws.append(sub)

        sub = draws[draws['age_group_id'] == 4]

        oth_cols = [col for col in sub.columns if 'draw_' not in col]
        sub.set_index(oth_cols, inplace=True)
        sub[:] = 0
        sub = sub.reset_index()
        sub['age_group_id'] = 3

        draws = draws.append(sub)

    draws.drop(['cause_id', 'measure_id', 'metric_id', 'sex_name'],
               axis=1,
               inplace=True)
    
    draws.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id'],
                    inplace=True)
    
    mort = etl.df_to_xr(draws, wide_dim_name='draw', fill_value=np.nan)
    mort = mort / pops['population']  # gets it into rate space
    return mort

def get_ratios(ratio_file, ecode, water=False):
    if ecode == "inj_war_warterror":
        sub = ["inj_trans_road",'inj_homicide']
    elif ecode == "inj_war_execution":
        sub = ["inj_homicide"]
    elif ecode == 'inj_disaster':
        if water:
            sub = ["inj_trans_road", "inj_drowning"]
        else:
            sub = ["inj_trans_road"]
    else:
        raise ValueError('Invalid ecode. Must be one of inj_war_warterror, inj_war_execution, or inj_disaster')
    
    return ratio_file.loc[{'ecode': sub}].mean(dim='ecode')
    

def aqua(locs, year_id):
    disasters = pd.read_csv("FILEPATH")
    locations = disasters.loc[disasters['year_id'] == year_id, 'location_id'].unique()
    return {'water': [x for x in locs if x in locations], 'no_water': [x for x in locs if x not in locations]}


def get_shock_inc(ecode, pops, dems, year_id, sex_id, decomp, ratio_file, regmap):
    mort = get_shock_mort(ecode, pops, dems['location_id'], dems['age_group_id'], year_id, sex_id, decomp)
    
    if ecode == 'inj_disaster':
        dis_type = aqua(dems['location_id'], year_id)
        
        if len(dis_type['water']) > 0 and len(dis_type['no_water']) > 0:
            aqua_ratio = get_ratios(ratio_file, ecode, water=True)
            noaq_ratio = get_ratios(ratio_file, ecode, water=False)
            
            incidence = xr.concat([mort.loc[{'location_id': dis_type['water']}].groupby(regmap.loc[{'location_id': dis_type['water']}]) * aqua_ratio,
                                  mort.loc[{'location_id': dis_type['no_water']}].groupby(regmap.loc[{'location_id': dis_type['no_water']}]) * noaq_ratio], dim='location_id')
        elif len(dis_type['no_water']) > 0:
            noaq_ratio = get_ratios(ratio_file, ecode, water=False)
            incidence = mort.groupby(regmap) * noaq_ratio
        elif len(dis_type['water']) > 0:
            aqua_ratio = get_ratios(ratio_file, ecode, water=True)
            incidence = mort.groupby(regmap) * aqua_ratio
        else:
            raise ValueError('Something went wrong, there\'s nothing in dis_type')
    else:
        ratio = get_ratios(ratio_file, ecode)
        incidence = mort.groupby(regmap) * ratio
    
    incidence = incidence.drop('super_region_id')
    incidence = incidence.where(incidence < .5, .5)
    
    return incidence
    
def get_region_map(location_ids):
    reg = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(location_ids)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()


def write_results(arr, ecode, decomp, version, year_id, sex_id):
    version = version.rstrip()
    
    folder = (paths.DATA_DIR / decomp / inj_info.ECODE_PARENT[ecode] /
              version / 'shock_ecode_inc')
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    filepath = folder / f"{ecode}_{year_id}_{sex_id}.nc"

    if filepath.exists():
        os.remove(filepath)
    arr.to_netcdf(filepath)


def main(ecode, year_id, sex_id, decomp, version):
    tic = time.time()
    version = version.rstrip()
    
    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)
    
    env_version = versions.get_env(ecode, version)
    pop_path = paths.DATA_DIR / f"flats/{env_version}/pops.nc"
    pops = xr.open_dataset(pop_path)
    pops = pops.loc[{'year_id': [year_id], 'sex_id': [sex_id]}]
    
    version = version.rstrip()
    ratio_file = xr.open_dataarray(
        os.path.join(paths.DATA_DIR, decomp, 'inc_mortality_ratios', str(versions.get_crv(ecode, version)), 'ratios.nc'))
    
    regmap = get_region_map(dems['location_id'])
    incidence = get_shock_inc(ecode, pops, dems, year_id,
                              sex_id, decomp, ratio_file, regmap)

    write_results(incidence, ecode, decomp, version, year_id, sex_id)
