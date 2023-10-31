"""
This file holds functions that load data that needs to be used in the pipeline.

"""

import numpy as np
import os
import pandas as pd
import xarray as xr

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import demographics, inj_info, input_manager, paths, converters, utilities
from gbd_inj.types import Ecodes


def short_term_incidence_split(ecode, version, ncode, year_id, sex_id='all',
                               platform='all', location_id='all'):
    """Loads short term incidence for any given ecode/ncode combo

    Parameters
    ----------
    ecode : Ecode
        The ecode to get incidence for
    version : Int
        The ecode version
    ncode : Ncode
        The ncode to get incidence for
    year_id : Int
        The year to get incidence for
    sex_id : Int
        The sex to get incidence for. Default is 'all'
    platform : Platform
        The platform to get incidene for. Default is 'all'.
    location_id : Int
        The location to get incidence for. If `location_id='all'` then all
        locations will be loaded.

    Returns
    -------
    xr.Array
        An xr.Array of incidence data.

    """
    in_dir = (paths.DATA_DIR / FILEPATH)
    # Parse sex
    if sex_id == 'all':
        sex_id = [1, 2]
    sex_id = np.atleast_1d(sex_id).tolist()
    # Parse platform
    if platform == 'all':
        if ncode in inj_info.OTP_NCODES:
            platform = ['inpatient', 'outpatient']
        else:
            platform = ['inpatient']
    platform = np.atleast_1d(platform).tolist()
    # Parse locations
    if location_id != 'all':
        location_id = np.atleast_1d(location_id).tolist()

    s_list = []
    for s in sex_id:
        p_list = []
        for p in platform:
            file_path = FILEPATH
            incidence = xr.open_dataarray(file_path, group=ncode)
            if location_id != 'all':
                incidence = incidence.loc[{'location_id': location_id}]
            p_list.append(incidence)
        s_list.append(xr.concat(p_list, dim='platform'))
    return xr.concat(s_list, dim='sex_id')


def disability_weights_st():
    """ Loads short term disability weights

    Returns
    -------
    xr.Array
        An array of short term disability weights

    """
    return converters.df_to_xr(
        pd.read_csv(paths.DISABILITY_WEIGHT_FILE).set_index('ncode'),
        wide_dim_name='draw', fill_value=np.nan
    )


def smr(ncode):
    """Gets standardized mortality ratio.

    Parameters
    ----------
    ncode : Ncode
        The Ncode to load.

    Returns
    -------
    xr.DataArray
        An array of smr

    """
    smr = pd.read_csv(paths.INPUT_DIR / FILEPATH)
\
    if ncode == "N48":
        smr = smr.loc[smr["ncode"] == "N9"]
    else:
        smr = smr.loc[smr["ncode"] == ncode]
    smr["se"] = (smr["UL"] - smr["LL"]) / 3.92

    smr["ncode"] = ncode

    # generate draws of SMR
    smr.reset_index(drop=True, inplace=True)
    np.random.seed(659177)
    smr[utilities.drawcols()] = pd.DataFrame(np.random.normal(smr['SMR'], smr['se'], size=(config.DRAWS, len(smr))).T)
    smr.drop(['SMR', 'UL', 'LL', 'se'], inplace=True, axis=1)

    smr.loc[smr.age == 0.01, "age"] = demographics.AGE_GROUP_ID_TO_YEARS[3]
    expand_years = [demographics.AGE_GROUP_ID_TO_YEARS[id] for id in (388, 389)]
    smr = utilities.expand(smr, "age", 0.1, expand_years)
    smr = smr.loc[smr.age != 0.1]
    smr = utilities.expand(smr, "age", 1, [2])

    # Convert ages to age groups
    smr['age_group_id'] = smr.age.replace(demographics.YEARS_TO_AGE_GROUP_ID)
    smr.drop(columns=['age'], inplace=True)
    
    smr.set_index(['ncode','age_group_id'], inplace=True)
    smr[smr < 1] = 1
    return converters.df_to_xr(smr, wide_dim_name='draw', fill_value=np.nan)


def mortality(flat_version, year_id, sex_id, location_id='all'):
    """Loads downloaded mortality data as draws from previously downloaded
    data (flat files)

    Parameters
    ----------
    flat_version : Int
        The version of flat files to load
    year_id : Int
        The year to load.
    sex_id : Int
        The sex to load
    location_id : int
        The location to load. Default is 'all'

    Returns
    -------
    xr.DataArray
        An array of mortality data.

    """
    mort = xr.open_dataset(paths.DATA_DIR / FILEPATH)
    if location_id == 'all':
        subset = {'year_id': [year_id], 'sex_id': [sex_id]}
    else:
        location_id = np.atleast_1d(location_id).tolist()
        subset = {'year_id': [year_id], 'sex_id': [sex_id], 'location_id': location_id}
    mort = mort.loc[subset]
    mort['std'] = (mort['upper'] - mort['lower']) / 3.92

    np.random.seed(7182009)
    mortdraws = xr.DataArray(
        data=np.random.normal(mort['mean'], mort['std'], size=(config.DRAWS,)+mort['mean'].shape),
        dims=('draw',)+mort['mean'].dims,
        coords=mort['mean'].coords.merge({'draw': utilities.drawcols()}).coords
    )
    return mortdraws
    mortdraws.values[mortdraws.values > 1] = 1
    mortdraws.values[mortdraws.values < 0] = 0
    return mortdraws


def population(flat_version, single_year=False, year_id='all', sex_id='all', location_id='all'):
    """Loads population data from previously download data (flat files)

    Parameters
    ----------
    flat_version : Int
        The flat files version to use.
    single_year : Bool
        Load a single year? Default is `False`
    year_id : Int
        The year to load. Default is 'all'
    sex_id : Int
        The sex to load. Default is 'all'
    location_id : Int
        The location to load. Default is 'all'

    Returns
    -------
    xr.DataArray
        An array of population data.

    """
    filename = FILEPATH if single_year else FILEPATH
    pops = xr.open_dataset(paths.DATA_DIR/FILEPATH)
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
