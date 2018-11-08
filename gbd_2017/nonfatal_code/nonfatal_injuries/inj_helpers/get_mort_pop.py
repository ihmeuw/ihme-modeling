"""
Save flat-files used in the shock and non-shock modeling process -- populations and mortality.
"""

import db_queries as db
import os
import pandas as pd
import xarray as xr
from gbd_inj.inj_helpers import help, paths


def get_best_version():
    temp = db.get_envelope(gbd_round_id=help.GBD_ROUND, year_id=help.LAST_YEAR, with_hiv=True, with_shock=False)
    return temp.loc[0,'run_id']


def write_results(df, filepath, indexcols):
    """Writes results for the df, keyed by location."""
    df = df.copy()
    df.drop('run_id',inplace=True,axis=1)
    df.set_index(indexcols, inplace = True)
    ds = df.to_xarray()
    ds.to_netcdf(filepath)


def get_mortality(dems, shock):
    """Pull all-cause mortality."""
    print("Getting mortality")
    df = db.get_envelope(age_group_id=dems["age_group_id"], location_id=dems["location_id"], year_id=dems['year_id'],
                         sex_id=dems['sex_id'], with_hiv=1, with_shock=shock, rates=1, gbd_round_id=help.GBD_ROUND)
    return df


def get_populations(dems):
    """Pull populations for GBD age-groups."""
    print("Getting populations")
    df = db.get_population(year_id=dems['year_id'], sex_id=dems['sex_id'], location_id=dems["location_id"],
                           age_group_id=dems["age_group_id"], gbd_round_id=help.GBD_ROUND)
    return df


def get_sy_populations(dems):
    """Pull populations for single-year age-groups without under 1 age groups (already contained in get_populations()) This does not include 95+ because it is already
    in the get_populations call. Will be appended in scripts if necessary."""
    print("Getting single-year populations")
    df = db.get_population(year_id=dems['year_id'], sex_id=dems['sex_id'], location_id=dems["location_id"],
                           age_group_id=list(range(49, 143)), single_year_age=True, gbd_round_id=help.GBD_ROUND)
    return df


def rate_in_baseline():
    """Makes the portion of the rate_in files that don't vary by loc/year/sex"""
    iota = pd.DataFrame({'age': [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 90, 100],
                         'type': 'iota', 'mean': '5', 'lower': '0', 'upper': '10', 'std': 'inf'})
    diota = pd.DataFrame({'age': [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 90],
                          'type': 'diota', 'mean': '0', 'lower': '_inf', 'upper': 'inf', 'std': 'inf'})
    # rho/drho is remission -- always set to 0
    rho = pd.DataFrame({'age': [0, 0, 100], 'type': ['drho', 'rho', 'rho'],
                        'mean': '0', 'lower': '0', 'upper': '0', 'std': 'inf'})
    domega = pd.DataFrame(
        {'age': [0, 3.5 / 365, 17.5 / 365, 196.5 / 365, 3, 7.5, 12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5,
                 47.5, 52.5, 57.5, 62.5, 67.5, 72.5, 77.5, 82.5, 87.5, 92.5, 97.5],
         'type': 'domega', 'mean': '0', 'lower': '_inf', 'upper': 'inf', 'std': 'inf'})
    # chi is excess mortality
    # chi for non-emr
    no_emr_chi = pd.DataFrame({'age': [0, 100, 0], 'type': ['chi', 'chi', 'dchi'],
                               'mean': '0', 'lower': '0', 'upper': '0', 'std': 'inf'})
    # chi for emr
    emr_chi = iota.copy()
    emr_chi['type'] = 'chi'
    emr_dchi = diota.copy()
    emr_dchi['type'] = 'dchi'
    
    return {'emr': pd.concat([iota, diota, rho, emr_chi, emr_dchi, domega]),
            'no_emr': pd.concat([iota, diota, rho, no_emr_chi, domega])}


def rate_in_omega(mort_df, rate_dict, years, outdir):
    print('Making rate_in files for every location, year, and sex')
    mort = mort_df.loc[mort_df['year_id'].isin(years)]
    mort = help.convert_to_age_midpoint(mort)
    mort["std"] = (mort["upper"] - mort["lower"]) / 3.92
    
    young = mort.loc[mort['age'] == mort['age'].min()]
    young['age'] = 0
    young['std'] = 'inf'
    young['lower'] = '0'
    young['upper'] = 'inf'
    
    old = mort.loc[mort['age'] == mort['age'].max()]
    old.set_index(['location_id','year_id','sex_id'], inplace=True)
    old['age'] = 100
    old['std'] = mort.groupby(['location_id','year_id','sex_id'])['std'].mean()
    old['lower'] = '0'
    old['upper'] = 'inf'
    old.reset_index(inplace=True)
    
    print('making omega')
    # create omega -- all-cause mortality
    omega = young.append(mort).append(old)
    omega["type"] = "omega"
    omega.drop(['run_id'], axis=1, inplace=True)
    omega.set_index(['location_id','year_id','sex_id'], inplace=True)
    
    metaloc = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)
    print('starting loop')
    for locn in omega.index.get_level_values('location_id').unique():
        ihme_loc_id = str(metaloc.loc[metaloc['location_id'] == locn, 'ihme_loc_id'].reset_index(drop=True)[0])
        for y in omega.index.get_level_values('year_id').unique():
            for s in omega.index.get_level_values('sex_id').unique():
                folder = os.path.join(outdir, 'rate_in', str(y), str(s), ihme_loc_id)
                if not os.path.exists(folder):
                    os.makedirs(folder)
                    
                emr = pd.concat([rate_dict['emr'], omega.loc[locn,y,s].reset_index(drop=True)]).reset_index(drop=True)
                no_emr = pd.concat([rate_dict['no_emr'], omega.loc[locn,y,s].reset_index(drop=True)]).reset_index(drop=True)
                emr.to_csv(os.path.join(folder, 'FILEPATH.csv'), index=False)
                no_emr.to_csv(os.path.join(folder, 'FILEPATH.csv'), index=False)


def main(version):

    # pull the demographics for COD because we want every year
    dems = db.get_demographics(gbd_team = "cod", gbd_round_id=help.GBD_ROUND)
    rate_years = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)['year_id']
    indexcols = ['location_id', 'sex_id', 'year_id', 'age_group_id']

    # make directory for these files for a given version
    outdir = os.path.join("FILEPATH")
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    raw_rate_in = rate_in_baseline()
    
    # (1) Mortality
    mort = get_mortality(dems, shock=0)
    write_results(df=mort, filepath=os.path.join(outdir, "mortality.nc"),
                  indexcols=indexcols)

    rate_in_omega(mort, raw_rate_in, rate_years, outdir)

    # (2) Populations
    pops = get_populations(dems)
    write_results(df=pops, filepath=os.path.join(outdir, "FILEPATH.nc"), indexcols=indexcols)

    # (3) Single-Year Populations
    sy_pops = get_sy_populations(dems)
    write_results(df=sy_pops, filepath=os.path.join(outdir, "FILEPATH.nc"), indexcols=indexcols)


if __name__ == '__main__':
    version = get_best_version()
    main(version)
