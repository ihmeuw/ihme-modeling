from __future__ import division
import os

import db_queries as db

from gbd_inj.inj_helpers import help, versions, calculate_measures, load_measures, inj_info, paths
import sys
import xarray as xr
import numpy as np


def make_age_dictionary(age_type):
    """Makes an age dictionary out of either age data or age group ID mapped to age lower and
    age upper. Useful for the interpolation.

    Args:
        age_type (string): do you want age-data (i.e. 10, 15, 20, ...95+) or age_group_id?

    Returns:
        result (dict): dictionary of either age_group_id or age_start to age lower and upper

    """
    if age_type == 'age':
        age_dict = {a: [a, a + 5] for a in range(5, 100, 5)}
        age_dict.update({0: [0, 7 / 365], .01: [7 / 365, 28 / 365], .1: [28 / 365, 1], 1: [1, 5]})
    elif age_type == 'age_group':
        age_dict = {ag: [(ag - 5) * 5, (ag - 5) * 5 + 5] for ag in range(6, 21)}
        age_dict.update({30: [80, 85], 31: [85, 90], 32: [90, 95], 235: [95, 100],
                         2: [0, 7 / 365], 3: [7 / 365, 28 / 365], 4: [28 / 365, 1], 5: [1, 5]})
    else:
        raise ValueError('age_type must be either "age" or "age_group"')
    return age_dict


def aggregate_inc(lt_probs, ecode, ncode, platform, sex, years, version):
    allyears = []
    for year in years:
        inc = calculate_measures.long_term_incidence(ecode, version, ncode, platform, year, sex, lt_probs)
        allyears.append(inc)
    
    master = xr.concat(allyears, dim='year_id')
    return master


def aggregate_emr(smr_df, sex, years, flat_version):
    allyears = []
    for year in years:
        emr = calculate_measures.emr(smr_df, year, sex, flat_version)
        allyears.append(emr)
    
    master = xr.concat(allyears, dim='year_id')
    return master


def get_grp_population(sex_id, pop_dir):
    """ Get df of group populations with age data instead of age_group_id
    """
    
    master = xr.open_dataset(os.path.join('FILEPATH.nc'))
    master = master.loc[{'sex_id': [sex_id]}]
    master = master.to_dataframe().reset_index()
    
    # convert to age-data
    master = help.convert_from_age_group_id(master)
    return master


def get_sy_population(grp_pop, sex_id, pop_dir):
    """ Get single-year population (i.e. single-year ages) data for all years.

    :param grp_pop: group-population df because we need 95+
    :param sex_id (int)
    :param location_id (int)
    :param pop_dir (str): directory where population flat-files are saved
    :return: single-year population data-frame for all COD years
    """
    terminal_age = grp_pop.loc[grp_pop["age"] == 95]
    under1 = grp_pop.loc[grp_pop.age < 1]
    under1 = under1.groupby(['location_id', 'sex_id', 'year_id']).sum().reset_index()
    under1["age"] = 0
    
    master = xr.open_dataset(os.path.join('FILEPATH.nc'))
    master = master.loc[{'sex_id': [sex_id]}]
    master = master.to_dataframe().reset_index()
    
    # merge on single-year age names
    age_ids = db.get_ids(table="age_group")
    master = master.merge(age_ids, on=['age_group_id'])
    master.rename(columns={'age_group_name': 'age'}, inplace=True)
    master.drop('age_group_id', inplace=True, axis=1)
    master.loc[master.age == "95 plus", 'age'] = 95
    master["age"] = master["age"].astype(float)
    master = master.append(terminal_age)
    master = master.append(under1)
    
    return master


def get_pop_total(sy, grp):
    """Take Under 1 age groups and add on their population to the single-year age population. This creates
    a DF that has under 1 pops and single year age groups above 1.
    """
    
    sy = sy.loc[sy['age'] >= 1]
    grp = grp.loc[grp['age'] < 1]
    
    result = grp.append(sy)
    return result


def interpolate_ages(arr, sy_pop, grp_pop):
    """Given an array with regular GBD age_group_id dimension, returns an interpolated array with values for each single
    year, indexed by the single year age_group_ids, together with aggregated under-1 and 95+ groups
    
    :param arr: DataArray with standard GBD age groups in the age_group_id dimension. Values must be rates
    :param sy_pop: Dataset with a single array ('population') containing single year populations
    :param grp_pop: Dataset with a single array ('population') containing age group populations
    """
    # Get single year ages to interpolate to, and append them (with NaN for value) to original age_group values
    single_years = sy_pop.coords['age_group_id'].values
    sy_ages = xr.DataArray([0] * len(single_years), dims='age_group_id', coords=[single_years])
    interp, _ = xr.broadcast(arr, sy_ages)
    
    # Add a coordinate with the midpoint of each age group, so 5.5 for single year age 5, but 7.5 for age group age 5
    interp = help.append_age_mdpt(interp, single_years=True)
    
    # Interpolate to fill in the single year values, using the midpoint as the time of the age_group value
    interp = interp.sortby('age_mid').interpolate_na(dim='age_group_id', use_coordinate='age_mid')
    interp = interp.drop('age_mid')
    
    # Squeeze the interpolated values to match the initial age group rates, when considering count space
    interpolated = interp.loc[{'age_group_id': single_years}]
    interpolated = help.append_age_group(interpolated)
    ratio = ((arr * grp_pop['population']).rename({'age_group_id': 'age_group'}) /
             (interpolated * sy_pop['population']).groupby('age_group').sum('age_group_id')
             ).fillna(0)
    squeezed = ratio * interpolated.groupby('age_group')
    
    # Append the collapsed under-1 data from the original array, along with the 95+ age group
    under1 = (arr.loc[{'age_group_id': [2, 3, 4]}] * grp_pop['population']).sum('age_group_id') / \
             grp_pop['population'].loc[{'age_group_id': [2, 3, 4]}].sum('age_group_id')
    age_group_dim = xr.DataArray([0], dims='age_group_id', coords=[[28]])
    under1, _ = xr.broadcast(under1, age_group_dim)
    under1['age_group'] = 28
    over95 = arr.loc[{'age_group_id': [235]}]
    over95['age_group'] = 235
    
    final = xr.concat([under1, squeezed, over95], dim='age_group_id')
    return final


def integrate(prev, inc, mort, t):
    new_prev = -(((1 - prev) * inc - prev * mort) * np.exp(-(mort + inc) * t) - inc) / (mort + inc)
    return new_prev.fillna(prev)


def progress_half_year(prev, inc, mort, sy_pop, grp_pop):
    new_prev = integrate(prev, inc, mort, .5)
    sy_pop_w_over95 = xr.concat([sy_pop['population'], grp_pop['population'].loc[{'age_group_id': [235]}]],
                                dim='age_group_id')
    # Drop under-1 then collapse to age_group
    new_prev = new_prev.drop(28, dim='age_group_id')
    new_prev = (new_prev * sy_pop_w_over95).groupby('age_group').sum('age_group_id').rename({'age_group': 'age_group_id'}) / grp_pop['population']
    
    # Then add back in the disaggregated under-1 age groups, all set to zero
    under_1_ages = xr.DataArray([0,0,0], dims='age_group_id', coords=[[2,3,4]])
    new_prev, _ = xr.broadcast(new_prev, under_1_ages)
    new_prev = new_prev.fillna(0)
    
    return new_prev
    

def progress_one_year(prev, inc, mort):
    new_prev = integrate(prev, inc, mort, 1)
    new_prev.coords['year_id'].values = new_prev.coords['year_id'].values + 1
    new_prev = new_prev.shift(age_group_id=1)
    new_prev = new_prev.fillna(0)
    
    return new_prev


def write_results(arr, ecode, ncode, platform, year, version):
    out_dir = os.path.join("FILEPATH")
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    filename = "FILEPATH.nc".format(year)
    filepath = os.path.join(out_dir, filename)
    arr.to_netcdf(filepath)


def main(ecode, ncode, platform, version):
    start = help.start_timer()
    
    parent = inj_info.ECODE_PARENT[ecode]
    flat_version = versions.get_env(parent, version)
    
    # need the cod demographics because
    dems = db.get_demographics(gbd_team="cod", gbd_round_id=help.GBD_ROUND)
    
    # get dfs used for long-term incidence and EMR
    lt_probs = calculate_measures.long_term_probs_combined(ncode, year_id='full')
    
    print "Working on {}".format(ncode)
    if ncode in inj_info.EMR_NCODES:
        smr = load_measures.smr(ncode)
    
    sy_pop = load_measures.population(flat_version, single_year=True)
    grp_pop = load_measures.population(flat_version)
    
    prev = xr.DataArray([0], dims='ncode', coords=[[ncode]])
    for year in dems['year_id']:
        print(year)
        inc_list = []
        emr_list = []
        print('Getting incidence and emr if applicable')
        sys.stdout.flush()  # write to log
        for sex in dems['sex_id']:
            sex_inc = calculate_measures.long_term_incidence(ecode, version, ncode, platform, year, sex, lt_probs)
            inc_list.append(sex_inc)
            if ncode in inj_info.EMR_NCODES:
                sex_emr = calculate_measures.emr(smr, year, sex, flat_version)
                emr_list.append(sex_emr)
        
        incidence = xr.concat(inc_list, dim='sex_id')
        
        print('Interpolating')
        sys.stdout.flush()  # write to log
        inc_interp = interpolate_ages(incidence, sy_pop.loc[{'year_id': [year]}], grp_pop.loc[{'year_id': [year]}])
        if ncode in inj_info.EMR_NCODES:
            emr = xr.concat(emr_list, dim='sex_id')
            emr_interp = interpolate_ages(emr, sy_pop.loc[{'year_id': [year]}], grp_pop.loc[{'year_id': [year]}])
        else:
            emr_interp = xr.DataArray([0], dims='year_id', coords=[[year]])
        
        print('Running ODE/incrementing process')
        sys.stdout.flush()  # write to log
        # progress half year and save, for 1990 and on
        if year >= 1990:
            year_result = progress_half_year(prev, inc_interp, emr_interp,
                                             sy_pop.loc[{'year_id': [year]}], grp_pop.loc[{'year_id': [year]}])
            write_results(year_result, ecode, ncode, platform, year, version)
        # then progress full year and increment, for all years but the last
        if year != help.LAST_YEAR:
            prev = progress_one_year(prev, inc_interp, emr_interp)
    
    help.end_timer(start)

if __name__ == '__main__':
    ecode = 'inj_disaster'
    ncode = 'N19'
    platform = 'outpatient'
    version = '2'
    main(ecode, ncode, platform, version)