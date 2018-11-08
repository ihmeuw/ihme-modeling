import os
from gbd_inj.inj_helpers import help, inj_info, paths
import xarray as xr
import pandas as pd
import numpy as np
from fbd_core import etl


def short_term_incidence_unsplit(ecode, version, year_id, sex_id, platform='all'):
    """Gets incidence for a given ecode from the either shock or nonshock step."""
    if ecode in inj_info.SHOCK_ECODES:
        in_dir = os.path.join('FILEPATH')
    else:
        in_dir = os.path.join('FILEPATH')
    filename = "FILEPATH.nc".format(ecode, year_id, sex_id)

    # read the incidence and subset to the platform that we want
    incidence = xr.open_dataarray(os.path.join(in_dir, filename))
    if platform == 'all':
        platform = ['inpatient', 'outpatient']
    else:
        platform = np.atleast_1d(platform).tolist()
    return incidence.loc[{'platform': platform}]


def short_term_incidence_split(ecode, version, ncode, year_id, sex_id='all', platform='all', location_id='all'):
    """Pulls incidence from the E-N split step. Will automatically grab inpatient incidence and check for outpatient,
    based on the help.get...() function. If it's not there, it won't append anything."""
    in_dir = os.path.join('FILEPATH')
    
    if sex_id == 'all':
        sex_id = [1, 2]
    sex_id = np.atleast_1d(sex_id).tolist()
    if platform == 'all':
        if ncode in inj_info.OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()
    if location_id != 'all':
        location_id = np.atleast_1d(location_id).tolist()
    
    s_list = []
    for s in sex_id:
        p_list = []
        for p in platform:
            filename = 'FILEPATH.nc'.format(ecode, p, year_id, s)
            incidence = xr.open_dataarray(os.path.join(in_dir, filename), group=ncode)
            if location_id != 'all':
                incidence = incidence.loc[{'location_id': location_id}]
            p_list.append(incidence)
        s_list.append(xr.concat(p_list, dim='platform'))
    return xr.concat(s_list, dim='sex_id')


def en_matrix(ecode, sex_id, platform):
    """Returns e/n matrix, used to split an ecode into the 47 ncodes.
    
    Matrices vary by ecode, platform, sex, age and high income/not high income (the super region) countries.
    """
    if platform == "inpatient":
        short_plat = "inp"
    else:
        short_plat = "otp"
    # read the matrix file for the specified platform
    if inj_info.ECODE_PARENT[ecode] == 'inj_poisoning':
        e = 'inj_poisoning'
    else:
        e = ecode
    
    en_mat_dir = "FILEPATH"
    matrix = pd.read_csv(os.path.join(en_mat_dir, "FILEPATH.csv"))
    
    # subset the matrix to requested sex, and convert from ages to age_group_ids (copying values for under 1 age groups)
    matrix = matrix.loc[matrix['sex'] == sex_id]
    matrix = help.convert_to_age_group_id(matrix, collapsed_0=True)
    matrix = help.expand_under_1(matrix)
    
    # format matrix for incidence merge
    matrix.drop(['ecode'], inplace=True, axis=1)
    matrix.rename(columns={'n_code': 'ncode', 'inpatient': 'platform', 'sex': 'sex_id'}, inplace=True)
    plat_dict = {1: 'inpatient', 0: 'outpatient'}
    matrix['platform'] = matrix['platform'].replace(plat_dict)
    
    matrix.set_index(['ncode', 'platform', 'high_income','sex_id', 'age_group_id'], inplace=True)
    x_matr = etl.df_to_xr(matrix, wide_dim_name='draw', fill_value=0)
    
    return x_matr


def disability_weights_st():
    folder = 'FILEPATH'
    return etl.df_to_xr(pd.read_csv(os.path.join(folder, 'FILEPATH.csv')).set_index('ncode'),
                        wide_dim_name='draw', fill_value=np.nan)


def smr(ncode):
    smr = pd.read_csv(os.path.join(paths.INPUT_DIR, 'FILEPATH.csv')).drop('name', axis=1)

    if ncode == "N48":
        smr = smr.loc[smr["ncode"] == "N9"]
    else:
        smr = smr.loc[smr["ncode"] == ncode]
    smr["se"] = (smr["UL"] - smr["LL"]) / 3.92

    smr["ncode"] = ncode
    
    # generate draws of SMR
    smr.reset_index(drop=True, inplace=True)  # need to reset index so that the random draws will line up
    np.random.seed(659177)
    smr[help.drawcols()] = pd.DataFrame(np.random.normal(smr['SMR'], smr['se'], size=(1000, len(smr))).T)
    smr.drop(['SMR', 'UL', 'LL', 'se'], inplace=True, axis=1)

    smr = help.convert_to_age_group_id(smr, collapsed_0=False)
    smr.set_index(['ncode','age_group_id'], inplace=True)
    smr[smr < 1] = 1
    return etl.df_to_xr(smr, wide_dim_name='draw', fill_value=np.nan)


def mortality(flat_version, year_id, sex_id, location_id='all'):
    mort = xr.open_dataset(os.path.join(paths.DATA_DIR, 'FILEPATH.nc'))
    if location_id == 'all':
        subset = {'year_id': [year_id], 'sex_id': [sex_id]}
    else:
        location_id = np.atleast_1d(location_id).tolist()
        subset = {'year_id': [year_id], 'sex_id': [sex_id], 'location_id': location_id}
    mort = mort.loc[subset]
    mort['std'] = (mort['upper'] - mort['lower']) / 3.92

    np.random.seed(7182009)
    mortdraws = xr.DataArray(data=np.random.normal(mort['mean'], mort['std'], size=(1000,)+mort['mean'].shape),
                             dims=('draw',)+mort['mean'].dims,
                             coords=mort['mean'].coords.merge({'draw': ['draw_'+str(n) for n in range(1000)]}).coords)
    mortdraws.values[mortdraws > 1] = 1
    mortdraws.values[mortdraws < 0] = 0
    return mortdraws


def population(flat_version, single_year=False, year_id='all', sex_id='all', location_id='all'):
    filename = 'FILEPATH.nc' if single_year else 'pops.nc'
    pops = xr.open_dataset(os.path.join(paths.DATA_DIR, 'FILEPATH', filename))
    subset = {}
    if year_id != 'all':
        year_id = np.atleast_1d(year_id).tolist()
        subset.update({'year_id': year_id})
    if sex_id != 'all':
        sex_id = np.atleast_1d(sex_id).tolist()
        subset.update({'sex_id': sex_id})
    if location_id != 'all':
        location_id = np.atleast_1d(location_id).tolist()
        subset.update({'location_id': location_id})
    return pops.loc[subset]
