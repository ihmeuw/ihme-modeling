####################################################################
## Author: USERNAME
## Date: 08/15/17
####################################################################

# SETUP ----------------------------------------------- #

# import packages
from __future__ import division
import pandas as pd
import db_queries as db
import os
import sys
from gbd_inj.inj_helpers import help, versions, inj_info, paths
import xarray as xr


def get_mortality(ecode, version, year_id, sex_id):
    in_dir = os.path.join('FILEPATH')
    filename = "FILEPATH.nc".format(str(year_id), str(sex_id))
    mortality = xr.open_dataarray(os.path.join(in_dir, filename))
    return mortality


def get_incidence(ecode, version, year_id, sex_id):
    in_dir = os.path.join('FILEPATH')
    filename = "FILEPATH.nc".format(ecode, str(year_id), str(sex_id))
    incidence = xr.open_dataarray(os.path.join(in_dir,filename))
    return incidence


def compute_ratio(ecode, version, year, sex):
    print "Getting mortality..."
    mortality = get_mortality(ecode, version, year, sex)
    print('Getting incidence')
    incidence = get_incidence(ecode, version, year, sex)
    
    ratio = incidence / mortality
    return ratio


def summarize(raw_ratios, super_region):
    # collapse over sex, year, and location (but within a super region)
    return raw_ratios.groupby(super_region).mean(dim=['location_id', 'sex_id', 'year_id'])


def get_region_map(location_ids):
    reg = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(location_ids)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()


def write_results(ratios, version):
    folder = os.path.join('FILEPATH')
    if not os.path.exists(folder):
        os.makedirs(folder)
    filepath = os.path.join(folder, "FILEPATH.nc")
    ratios.to_netcdf(filepath)
    

def main(version):
    start = help.start_timer()
    # pull identifiers
    ratio_ecodes = inj_info.IM_RATIO_ECODES
    dems = db.get_demographics(gbd_team='cod', gbd_round_id=help.GBD_ROUND)
    sexes = dems['sex_id']
    mortyears = list(filter(lambda x: x >= 1990, dems['year_id']))
    regmap = get_region_map(dems['location_id'])
    # iterate over year sex and ecode to get the ratios all in one data frame
    final_list = []
    for ecode in ratio_ecodes:
        year_arr_list = []
        for year in mortyears:
            sex_arr_list = []
            for sex in sexes:
                print('{}, {}, sex {}'.format(ecode, year, sex))
                sys.stdout.flush()  # write to log file
                ratios = compute_ratio(ecode, str(versions.get_best_version(ecode)), year, sex)
                sex_arr_list.append(ratios)
            combined_sexes = xr.concat(sex_arr_list,'sex_id')
            year_arr_list.append(combined_sexes)
        combined_years = xr.concat(year_arr_list, 'year_id')
        print('Summarize {}'.format(ecode))
        summarized = summarize(combined_years, regmap)
        final_list.append(summarized)
    final_ratios = xr.concat(final_list, pd.Index(ratio_ecodes, name='ecode'))
    
    print('Write results')
    write_results(final_ratios, version)
    
    help.end_timer(start)


if __name__ == '__main__':
    version = 16
    repo = "FILEPATH"

    main(version, repo)
