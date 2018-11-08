"""
Author: USERNAME
Last modified: 05/03/2018
Purpose: Splits the E-code Incidence to E-N Incidence
"""
import pandas as pd
import xarray as xr
import db_queries as db
import os
from gbd_inj.inj_helpers import help, load_measures, inj_info, paths


def get_income_map(location_ids):
    """ Get a binary variable for high income or not. Just based on the GBD super region. This
    is used for merging on the E-N matrices that are split by income."""
    income = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)[["location_id", "super_region_name"]]
    income["high_income"] = 0
    income.loc[income["super_region_name"] == "High-income", 'high_income'] = 1
    income.drop(['super_region_name'], inplace=True, axis=1)
    income = income.loc[income['location_id'].isin(location_ids)]
    return income.set_index('location_id')['high_income'].to_xarray()


def split_ncodes(incidence, matrix, income):
    split = incidence.groupby(income) * matrix
    return split.drop('high_income')


def prep_file(ecode, year_id, sex_id, platform, version):
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.mkdir(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    if ecode in inj_info.PARENT_ECODES:
        ecodes = inj_info.ECODE_CHILD[ecode]
    else:
        ecodes = [ecode]
    for e in ecodes:
        filename = "FILEPATH.nc".format(e, platform, year_id, sex_id)
        filepath = os.path.join(folder, filename)
        if os.path.exists(filepath):
            os.remove(filepath)


def write_results(incidence, ecode, year_id, sex_id, platform, version, mode='w', group=None):
    folder = os.path.join("FILEPATH")
    if ecode in inj_info.PARENT_ECODES:
        for e in inj_info.ECODE_CHILD[ecode]:
            filename = "FILEPATH.nc".format(e, platform, year_id, sex_id)
            incidence.loc[{'ecode': e}].drop('ecode').to_netcdf(os.path.join(folder, filename), mode=mode, group=group)
    else:
        filename = "FILEPATH.nc".format(ecode, platform, year_id, sex_id)
        incidence.to_netcdf(os.path.join(folder, filename), mode=mode, group=group)


def main(ecode, year_id, sex_id, platform, version):
    start = help.start_timer()
    
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    income = get_income_map(dems['location_id'])
    
    # if this is a parent e-code, we now want to use the parent incidence to scale the child incidence
    # and we don't want to save the parent incidence at all.
    if ecode in inj_info.PARENT_ECODES:
        print("This is a parent e-code, so now we are going to split and scale the children.")
        # get parent info
        parent_inc = load_measures.short_term_incidence_unsplit(ecode, version, year_id, sex_id, platform)
        parent_mat = load_measures.en_matrix(ecode, sex_id, platform)
        
        # get child info
        children_inc = []
        children_mat = []

        for child in inj_info.ECODE_CHILD[ecode]:
            child_inc = load_measures.short_term_incidence_unsplit(child, version, year_id, sex_id, platform)
            child_mat = load_measures.en_matrix(child, sex_id, platform)
            children_inc.append(child_inc)
            children_mat.append(child_mat)
        child_inc = xr.concat(children_inc, pd.Index(inj_info.ECODE_CHILD[ecode], name='ecode'))
        child_mat = xr.concat(children_mat, pd.Index(inj_info.ECODE_CHILD[ecode], name='ecode'))
        
        # split into ncodes, scale the children, and save
        prep_file(ecode, year_id, sex_id, platform, version)
        mode = 'w'
        for n in inj_info.get_ncodes(platform):
            parent_n_mat = parent_mat.loc[{'ncode': [n]}]
            child_n_mat = child_mat.loc[{'ncode': [n]}]
            parent_split_inc = split_ncodes(parent_inc, parent_n_mat, income)
            child_split_inc = split_ncodes(child_inc, child_n_mat, income)
            
            scaled_child = child_split_inc * (parent_split_inc / child_split_inc.sum(dim='ecode'))
            scaled_child = scaled_child.fillna(0)
            write_results(scaled_child, ecode, year_id, sex_id, platform, version, mode=mode, group=n)
            mode = 'a'  # after first time, change to 'a' so it appends other ncodes to the same file
    else:  # non-parent ecode
        print("This is a single e-code so we are just splitting it, no scaling.")
        inc = load_measures.short_term_incidence_unsplit(ecode, version, year_id, sex_id, platform)
        matx = load_measures.en_matrix(ecode, sex_id, platform)
        prep_file(ecode, year_id, sex_id, platform, version)
        mode = 'w'
        for n in inj_info.get_ncodes(platform):
            print(n)
            n_matx = matx.loc[{'ncode': [n]}]
            split_inc = split_ncodes(inc, n_matx, income)
            write_results(split_inc, ecode, year_id, sex_id, platform, version, mode=mode, group=n)
            mode = 'a'  # after first time, change to 'a' so it appends other ncodes to the same file
    help.end_timer(start)


if __name__ == '__main__':
    ecode = 'inj_trans_road'
    year_id = 1990
    sex_id = 1
    platform = "inpatient"
    version = "5"

    main(ecode, year_id, sex_id, platform, version)
