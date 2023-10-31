"""


**Output**:

  Post ODE prevalence estimates as NetCDFs in:

  FILEPATH

**Overview**:

  1. Load long term probabilties, population data and SMR (if applicable)

  2. Create a variable prev to store our prevalence in.

  3. For every year

     4. Load long term incidence for both sexes

     5. *Does the ncode use EMR* Yes:

        5.1. Load EMR for both sexes

    6. Interpolate incidence for every year

    7. Interpolate EMR (or set EMR to 0 if the ncode isn't an EMR one)

    8. Compute prevalence for the current year

    9. Save the current year's prevalence

    10. Update our prev variable for the next year

"""

import pandas as pd
import numpy as np
import os
import sys
import time
import xarray as xr

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import calculate_measures, demographics, load_measures, inj_info, paths, input_manager, monitor
from gbd_inj.pipeline.infrastructure import pipeline_steps
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)

def append_age_mdpt(arr, coord_name='age_mid', single_years=False):
    age_dict = demographics.AGE_GROUP_ID_TO_MIDPOINT
    # Under 5 age group - just in case
    age_dict[28] = 0.5
    if single_years:
        # adjust standard age groups down slightly so that all values are
        # unique, a requirement for xarray's interpolate
        age_dict.update({k: v - .00001 for k, v in list(age_dict.items())
                         if v % 0.5 == 0})
        # single year ages
        age_dict.update({a+48: a+.5 for a in range(1,100)})

    age_map = pd.Series(age_dict).rename_axis('age_group_id').to_xarray()
    new_arr = arr.copy()
    new_arr[coord_name] = age_map
    return new_arr


def append_age_group(arr, coord_name='age_group'):
    """Appends GBD age groups onto an xr.Array

    Parameters
    ----------
    arr : xr.Array
        An xr.Array with
    coord_name : str
        The name of new coordinate

    Returns
    -------
    xr.Array
        An array with a new age groups coordinate

    """
    age_to_age_group = demographics.YEARS_TO_AGE_GROUP_ID

    # Map from single age to age groups (do 1-5 and 5-95+ age groups seperately)
    age_to_age_midpoint = lambda a: (((a - 3) // 5) - 9) * 5
    age_dict = {a+53: age_to_age_group[age_to_age_midpoint(a+53)]
                for a in range(95)}
    # Add 1-5 age groups
    age_dict.update({
        49: 238,
        50: 34,
        51: 34,
        52: 34,
    })
    
    age_map = pd.Series(age_dict).rename_axis('age_group_id').to_xarray()
    new_arr = arr.copy()
    new_arr[coord_name] = age_map
    return new_arr


def interpolate_ages(arr, sy_pop, grp_pop):
    """Interpolates estimates for five year age bins into single age estimates

    Parameters
    ----------
    arr : xr.DataArray
        DataArray with standard GBD age groups in the age_group_id dimension.
        Values must be rates
    sy_pop : xr.Dataset
        Dataset with a single array ('population') containing single year
        populations
    grp_pop : xr.Dataset
        Dataset with a single array ('population') containing age group
        populations

    Returns
    -------
    xr.DataArray
        An array with regular GBD age_group_id dimension, returns an
        interpolated array with values for each single year, indexed by the
        single year age_group_ids, together with aggregated under-1 and 95+
        groups

    """
    # Get single year ages to interpolate to, and append them (with NaN for value) to original age_group values
    single_years = sy_pop.coords['age_group_id'].values
    sy_ages = xr.DataArray([0] * len(single_years), dims='age_group_id', coords=[single_years])
    interp, _ = xr.broadcast(arr, sy_ages)

    # Add a coordinate with the midpoint of each age group, so 5.5 for single year age 5, but 7.5 for age group age 5
    interp = append_age_mdpt(interp, single_years=True)

    # Interpolate to fill in the single year values, using the midpoint as the time of the age_group value
    interp = interp.sortby('age_mid').interpolate_na(dim='age_group_id', use_coordinate='age_mid')
    interp = interp.drop('age_mid')

    # Squeeze the interpolated values to match the initial age group rates, when considering countspace
    interpolated = interp.loc[{'age_group_id': single_years}]
    interpolated = append_age_group(interpolated)
    ratio = ((arr * grp_pop['population']).rename({'age_group_id': 'age_group'}) /
             (interpolated * sy_pop['population']).groupby('age_group').sum('age_group_id')
             ).fillna(0)
    squeezed = ratio * interpolated.groupby('age_group')

    # Append the collapsed under-1 data from the original array, along with the 95+ age group
    under1 = (arr.loc[{'age_group_id': demographics.UNDER_1_AGE_GROUPS}] * grp_pop['population']).sum('age_group_id') / \
             grp_pop['population'].loc[{'age_group_id': demographics.UNDER_1_AGE_GROUPS}].sum('age_group_id')
    age_group_dim = xr.DataArray([0], dims='age_group_id', coords=[[28]])
    under1, _ = xr.broadcast(under1, age_group_dim)
    under1['age_group'] = 28
    over95 = arr.loc[{'age_group_id': [235]}]
    over95['age_group'] = 235

    final = xr.concat([under1, squeezed, over95], dim='age_group_id')
    return final


def integrate(prev, inc, mort, t):
    """
    Closed form solution to the ODE

    $\frac{((1 - \text{prev})*\text{inc}-\text{prev}*\text{inc})e^{-(\text{mort}+\text{inc})t-\text{inc}}}{\text{mort}*\text{inc}}$
    """
    new_prev = -(((1 - prev) * inc - prev * mort) * np.exp(-(mort + inc) * t) - inc) / (mort + inc)
    return new_prev.fillna(prev)


def progress_half_year(prev, inc, mort, sy_pop, grp_pop):
    "Takes prevalence information and moves it half a year forward"
    # Integrate
    new_prev = integrate(prev, inc, mort, .5)


    sy_pop_w_over95 = xr.concat([sy_pop['population'], grp_pop['population'].loc[{'age_group_id': [235]}]],
                                dim='age_group_id')
    # Drop under-1 then collapse to age_group
    new_prev = new_prev.drop(28, dim='age_group_id')
    new_prev = (new_prev * sy_pop_w_over95).groupby('age_group').sum('age_group_id').rename({'age_group': 'age_group_id'}) / grp_pop['population']

    # Then add back in the disaggregated under-1 age groups, all set to zero
    under_1_ages = xr.DataArray(
        [0] * len(demographics.UNDER_1_AGE_GROUPS),
        dims='age_group_id',
        coords=[demographics.UNDER_1_AGE_GROUPS]
    )
    new_prev, _ = xr.broadcast(new_prev, under_1_ages)
    new_prev = new_prev.fillna(0)

    return new_prev


# TODO: need to account for 95+ age group. Currently just replacing their rate with the 94 rate when incrementing ages..
def progress_one_year(prev, inc, mort):
    """Takes prevalence information and moves it forward a year"""
    new_prev = integrate(prev, inc, mort, 1)
    new_prev = new_prev.assign_coords({'year_id': new_prev.coords['year_id'].values + 1})
    new_prev = new_prev.shift(age_group_id=1)
    new_prev = new_prev.fillna(0)

    return new_prev


def write_results(arr, ecode, ncode, platform, year, version):
    """Saves prevalence information"""
    out_dir = FILEPATH
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    arr.to_netcdf(FILEPATH)


def main(ecode, ncode, platform, version):
    print(f"Working on {ncode}")

    parent = inj_info.ECODE_PARENT[ecode]
    input = input_manager.input_with_id(version)
    flat_version = str(364)

    # need the cod demographics because its shocks (epi demographics go by the
    # 5 year age bins.
    dems = db.get_demographics(gbd_team="cod", gbd_round_id=config.GBD_ROUND)

    # Load long term probabilities, population data and SMR if applicable
    lt_probs = calculate_measures.long_term_probs_combined(ncode=ncode, year_id='full')
    sy_pop = load_measures.population(flat_version, single_year=True)
    grp_pop = load_measures.population(flat_version)
    if ncode in inj_info.EMR_NCODES:
        smr = load_measures.smr(ncode)

    prev = xr.DataArray([0], dims='ncode', coords=[[ncode]])

    mon = monitor.PipelineMonitor(len(dems['year_id']), pipeline_steps.shock_4, 10)
    for year in dems['year_id']:
        mon.start_iteration()
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
        if year != demographics.YEARS_EPI[-1]:
            prev = progress_one_year(prev, inc_interp, emr_interp)

        mon.end_iteration()
        if mon.should_restart(): sys.exit()

if __name__ == '__main__':
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = sys.argv[1]    
    ncode    = sys.argv[2]
    platform = sys.argv[3]
    version  = sys.argv[4]

    log.info("START")
    start = time.time()
    
    main(ecode, ncode, platform, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
