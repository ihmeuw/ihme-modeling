import pandas as pd
import db_queries as db
import os
import sys
from gbd_inj.inj_helpers import help, versions, inj_info, paths
import xarray as xr


def get_mortality(ecode, decomp, version, year_id, sex_id):
    in_dir = os.path.join(paths.DATA_DIR, decomp, ecode, str(version), 'mortality_for_shocks')
    filename = "mort_{}_{}.nc".format(str(year_id), str(sex_id))
    mortality = xr.open_dataarray(os.path.join(in_dir, filename))
    return mortality


def get_incidence(ecode, decomp, version, year_id, sex_id):
    in_dir = os.path.join(paths.DATA_DIR, decomp, ecode, str(version), 'nonshock_ecode_inc')
    filename = "{}_{}_{}.nc".format(ecode, str(year_id), str(sex_id))
    incidence = xr.open_dataarray(os.path.join(in_dir,filename))
    return incidence


def compute_ratio(ecode, decomp, version, year, sex):
    mortality = get_mortality(ecode, decomp, version, year, sex)
    incidence = get_incidence(ecode, decomp, version, year, sex)
    ratio = incidence / mortality
    return ratio


def summarize(raw_ratios, super_region):
    return raw_ratios.groupby(super_region).mean(dim=['location_id', 'sex_id', 'year_id'])


def get_region_map(location_ids):
    reg = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(location_ids)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()


def write_results(ratios, decomp, version):
    folder = os.path.join(paths.DATA_DIR, decomp, 'inc_mortality_ratios', str(version))
    if not os.path.exists(folder):
        os.makedirs(folder)
    filepath = os.path.join(folder, "ratios.nc")
    ratios.to_netcdf(filepath)
    

def main(decomp, version):
    start = help.start_timer()

    ratio_ecodes = inj_info.IM_RATIO_ECODES
    dems = db.get_demographics(gbd_team='cod', gbd_round_id=help.GBD_ROUND)
    sexes = dems['sex_id']
    mortyears = list([x for x in dems['year_id'] if x >= 1990])

    regmap = get_region_map(dems['location_id'])
    final_list = []
    for ecode in ratio_ecodes:
        year_arr_list = []
        for year in mortyears:
            sex_arr_list = []
            for sex in sexes:
                ratios = compute_ratio(ecode, decomp, str(versions.get_best_version(ecode)), year, sex)
                sex_arr_list.append(ratios)
            combined_sexes = xr.concat(sex_arr_list,'sex_id')
            year_arr_list.append(combined_sexes)
        combined_years = xr.concat(year_arr_list, 'year_id')
        print(('Summarize {}'.format(ecode)))
        summarized = summarize(combined_years, regmap)
        final_list.append(summarized)
    final_ratios = xr.concat(final_list, pd.Index(ratio_ecodes, name='ecode'))
    
    write_results(final_ratios, decomp, version)
    
    help.end_timer(start)
