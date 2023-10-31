"""
This file contains code used to make calculations in the pipeline.
"""

import os
import numpy as np
import pandas as pd
import xarray as xr

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import (demographics,
                                      load_measures,
                                      inj_info,
                                      paths,
                                      converters,
                                      utilities
)


def load_covariate(covariate_id):
    """Loads GBD covariate data

    Parameters
    ----------
    covariate_id : int
        The GBD id for the covariate to load

    Returns
    -------
    pd.DataFrame
        Covariate data

    """
    path = paths.COVARIATES / FILEPATH

    if not path.parent.exists(): path.mkdir(parents=True)
    # If we have already downloaded the covariate then load it
    if path.exists():
        return pd.read_csv(path)

    # Download the covariate
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
    locations = dems['location_id']
    df = db.get_covariate_estimates(covariate_id=covariate_id,
                                    location_id=locations,
                                    gbd_round_id=config.GBD_ROUND,
                                    decomp_step=config.DECOMP
    )
    # Save the dataframe and return it
    df.to_csv(path)
    return df

def pct_treated(min_treat=0.1, max_haqi=75, year_id='all'):
    """Pull the covariate value for HAQI Index.

    Used in both short-term durations and the long-term disability weights.

    Parameters
    ----------
    min_treat : type

    max_haqi : type
 
    year_id : int
        The year

    Returns
    -------
    xr.DataArray
        Percent treated for the given year.

    """
    dems = db.get_demographics(gbd_team='epi',
                               gbd_round_id=config.GBD_ROUND
    )
    locations = dems['location_id']
    haqi = load_covariate(1099)

    # get mean and SE from upper and lower
    haqi["se"] = (haqi["upper_value"] - haqi["lower_value"]) / 3.92

    # generate draws of HAQI
    np.random.seed(10191948)
    haqi[utilities.drawcols()] = pd.DataFrame(
        np.random.normal(haqi['mean_value'], haqi['se'], size=(config.DRAWS, len(haqi))).T
    )

    # drop unneeded columns
    haqi.drop(['mean_value', 'se', 'lower_value', 'upper_value',
               'model_version_id', 'covariate_id', 'covariate_name_short',
               'location_name', 'age_group_id', 'age_group_name', 'sex_id',
               'sex'], axis=1, inplace=True)

    # Transform the draws so that it is the % treated:
    haqi.set_index(['location_id', 'year_id'], inplace=True)
    haqi[haqi > max_haqi] = max_haqi
    xhaq = converters.df_to_xr(haqi, wide_dim_name='draw', fill_value=np.nan)
    dims = ['location_id', 'year_id']
    p_treated = min_treat + ((1-min_treat) * (xhaq - xhaq.min(dim=dims)) / (xhaq.max(dim=dims) - xhaq.min(dim=dims)))

    if year_id == 'all':
        return p_treated.loc[{'year_id': dems['year_id']}]
    elif year_id == 'full':
        return p_treated
    else:
        return p_treated.loc[{'year_id': [year_id]}]


def get_durations(pct_treated, ncode='all'):
    """Gets short-term duration for a specific ncode, platform, and year.

    Parameters
    ----------
    pct_treated : xr.DataArray
        DataArray with coordinates location_id, year_id, and draw.
    ncode : Ncode
        An Ncode

    Returns
    -------
    xr.DataArray
        DataArray with coordinates location_id, year_id, draw, ncode, platform

    """

    treated_path = paths.INPUT_DIR / FILEPATH
    treated_dur = converters.df_to_xr(
        pd.read_csv(treated_path).set_index(['ncode','platform']),
        wide_dim_name='draw', fill_value=np.nan
    )

    untreated_path = paths.INPUT_DIR / FILEPATH
    untreat_dur = converters.df_to_xr(
        pd.read_csv(untreated_path).set_index(['ncode','platform']),
        wide_dim_name='draw', fill_value=np.nan
    )

    if ncode != 'all':
        ncode = np.atleast_1d(ncode).tolist()
        treated_dur = treated_dur.loc[{'ncode': ncode}]
        untreat_dur = treated_dur.loc[{'ncode': ncode}]

    durations = (pct_treated * treated_dur) + ((1 - pct_treated) * untreat_dur)
    return durations


def compute_prevalence(incidence, durations):
    """Computes prevalence from incidence and durations.

    The following forumula is used:
      :math:`prevalence = \frac{incidence*durations}{1+(incidence*durations)}`


    Parameters
    ----------
    incidence : xr.DataArray
        A DataArray of incidence
    durations : xr.DataArray
        A DataArray of durations

    Returns
    -------
    xr.DataArray
        A DataArray of prevalence

    """
    return (incidence * durations) / (1 + (incidence * durations))


def short_term_ylds(prevalence, dws):
    """Computes YLDs from prevalence and disability weights.

    The following formula is used:

    prevalence * dws

    Parameters
    ----------
    prevalence : xr.DataArray
        A DataArray of prevalence
    dws : xr.DataArray
        A DataArray of disability weights

    Returns
    -------
    type
        Short Term YLDS

    """
    return prevalence * dws

def create_lt_grid(platform, ages):
    """Creates a grid of probabilities for N-codes that have 100% long-term
    probabilities.

    Parameters
    ----------
    platform : Platform
        The platform to create a grid for
    ages : [Int]
        A list of ages

    Returns
    -------
    pd.DataFrame
        A Dataframe of probabilities by ncode and age group

    """
    ncodes = [x for x in inj_info.get_lt_ncodes(platform) if x not in inj_info.ST_NCODES]
    grid = pd.DataFrame(utilities.expandgrid(ncodes, ages))
    grid.columns = ["ncode", "age_gr"]
    grid["platform"] = platform
    for draw in utilities.drawcols():
        grid[draw] = 1
    return grid


def long_term_probs_treated(ncode):
    """ Gets the raw long-term probabilities from a previous step output
    (not re-run here) and formats them to be merged with the short-term draws.

    Parameters
    ----------
    ncode : Ncode
        The ncode to get long term probablities for

    Returns
    -------
    xr.DataArray
        Long term probabilities of being treated as draws.

    """
  
    probs = pd.read_csv(paths.INPUT_DIR / FILEPATH)

    long_term_mask = probs["n_code"].isin(
        [x for x in inj_info.LT_NCODES if x not in inj_info.ST_NCODES]
    )

    probs = probs[~long_term_mask]
    probs.inpatient = probs.inpatient.astype(str)
    plat_dict = {'1': "inpatient", '0': "outpatient"}
    probs.inpatient = probs["inpatient"].replace({
        '1': "inpatient",
        '0': "outpatient"
    })
    probs.rename(columns={"inpatient": "platform", "n_code": "ncode"}, inplace=True)

    # get the ages used
    ages = probs.age_gr.unique()

    inp_grid = create_lt_grid("inpatient", ages)
    otp_grid = create_lt_grid("outpatient", ages)
    grid = inp_grid.append(otp_grid)

    # now put all of the probabilities together
    allprobs = probs.append(grid, sort=False)

    # drop this random column that keeps showing up from the probs csv
    allprobs.drop("draw_", axis=1, inplace=True)

    # now drop n-codes that have 0 long-term probability for a given platform
    allprobs.set_index(['ncode', 'platform', 'age_gr'], inplace=True)
    allprobs["draw_sum"] = allprobs.sum(axis=1)
    allprobs.reset_index(inplace=True)
    allprobs["total_sum"] = allprobs.groupby(['ncode', 'platform'])["draw_sum"].transform('sum')
    finalprobs = allprobs.loc[allprobs["total_sum"] != 0].reset_index()
    finalprobs.drop(['total_sum', 'draw_sum'], inplace=True, axis=1)

    # Convert to age groups
    # Ages are 0-1, 1-5, 5-10,...
    finalprobs.rename(columns={"age_gr": "age"}, inplace=True)
    finalprobs.loc[:,'age_group_id'] = finalprobs.age.replace(demographics.YEARS_TO_AGE_GROUP_ID)
    finalprobs = utilities.expand(finalprobs, "age_group_id", 2, demographics.UNDER_1_AGE_GROUPS)
    finalprobs = utilities.expand(finalprobs, "age_group_id", 238, demographics.UNDER_5_AGE_GROUPS)
    finalprobs.drop(columns=['age'], inplace=True)
    
    finalprobs = finalprobs.loc[finalprobs['ncode']==ncode]
    # finalprobs.rename(columns={"age_group_id": "collapsed_age_group_id"}, inplace=True)
    finalprobs.set_index(['ncode', 'platform', 'age_group_id'], inplace=True)

    x_probs = converters.df_to_xr(finalprobs, wide_dim_name='draw', fill_value=np.nan)

    return x_probs


def long_term_probs_untreated_multiplier():
    """Gets the expert-driven multipliers for long-term *untreated* probabilities.

    Parameters
    ----------

    Returns
    -------
    xr.DataArray
        ...

    """

    df = pd.read_excel(FILEPATH
    )
    df = df[['N-code', 'mean', 'LL', 'UL']]
    df.rename(columns={'N-code': 'ncode'}, inplace=True)

    df.loc[df["mean"] == "same", 'mean'] = 1
    df.loc[df["mean"] == 1, 'UL'] = 1
    df.loc[df["mean"] == 1, 'LL'] = 1
    df = df.loc[-df["mean"].isna()]
    df.loc[df["mean"] == "unlikely to survive", 'mean'] = "all"

    # create list of all-lt probs
    all = df.loc[df["mean"] == "all"].ncode.unique().tolist()
    all = [str(x) for x in all]

    df_mults = df.loc[np.logical_not(df["ncode"].isin(all))].copy()
    df_mults.loc[:,"se"] = (df_mults["UL"] - df_mults["LL"]) / 3.92
    df_mults.drop(['UL', 'LL'], inplace=True, axis=1)
    df_mults.loc[:,"mean"] = df_mults["mean"].astype(float)
    df_mults.loc[:,"se"] = df_mults["se"].astype(float)

    df_mults.reset_index(drop=True, inplace=True)  # reset index so random draws line up
    np.random.seed(541916)
    df_mults[utilities.drawcols()] = pd.DataFrame(np.random.normal(df_mults['mean'], df_mults['se'], size=(config.DRAWS, len(df_mults))).T)

    df_mults.drop(['mean', 'se'], inplace=True, axis=1)

    df_all = pd.DataFrame(columns=df_mults.columns)
    for i in range(len(all)):
        df_all.loc[i, 'ncode'] = all[i]

    df = df_mults.append(df_all)
    df['ncode'] = df['ncode'].astype(str)

    df.set_index('ncode', inplace=True)
    arr = converters.df_to_xr(df, wide_dim_name='draw', fill_value=np.nan)

    return arr


def long_term_probs_combined(ncode, year_id='all'):
    """Merge the multipliers onto the long-term probabilities.

    Parameters
    ----------
    ncode : Ncode
        The ncode to load
    year_id : type
        The year to load.

    Returns
    -------
    xr.DataArray
        Description of returned object.

    """
    ut_mults = long_term_probs_untreated_multiplier()
    t_probs = long_term_probs_treated(ncode)

    # grab percent treated
    print("Getting HAQI pct treated values...")
    pct_t = pct_treated( min_treat=0.1, max_haqi=75, year_id=year_id)

    ut_probs = t_probs * ut_mults
    if 'outpatient' in ut_probs.platform.values:
        ut_probs.loc[{'platform': 'outpatient'}] = ut_probs.loc[{'platform': 'outpatient'}].fillna(
            t_probs.loc[{'platform': 'outpatient'}])

    ut_probs = ut_probs.fillna(1)

    agg_prob = t_probs * pct_t + ut_probs * (1 - pct_t)
    agg_prob.values[agg_prob.values > 1] = 1

    return agg_prob


def long_term_incidence(ecode, version, ncode, platform, year_id, sex_id, lt_probs, location_id='all'):
    """Calculates long term incidence

    Parameters
    ----------
    ecode : Ecode
        The ecode to calculate
    version : int
        The ecode version
    ncode : Ncode
        The ncode to run
    platform : Platform
        The platform to run
    year_id : int
        The year to run
    sex_id : int
        The sex_id to run
    lt_probs : xr.DataArray
        Long term probabilities
    location_id : int
        A location_id

    Returns
    -------
    type
        Long term incidence.

    """
    st_inc = load_measures.short_term_incidence_split(ecode,
                                                      version,
                                                      ncode,
                                                      year_id,
                                                      sex_id,
                                                      platform,
                                                      location_id
    )
    return st_inc * lt_probs.loc[{'ncode': [ncode]}]


def emr(smr, year_id, sex_id, flat_version, location_id='all'):
    """Loads excess mortality ratios.

    Parameters
    ----------
    smr : xr.DataArray
        Standard mortality ratio
    year_id : type
        The year to run
    sex_id : int
        The GBD sex_id to run
    flat_version : int
        The current version of flat files (current demographics run_id)
    location_id : type
        The location_id to run

    Returns
    -------
    xr.DataArray
        Excess mortality ratios.

    """
    mort = load_measures.mortality(flat_version, year_id, sex_id, location_id)
    return mort * (smr - 1)


def ccc(x, y):
    """ Calculates ccc
    Arguments
    ---------
        x: np.Array
        An array of n numbers
        
        y: np.Array
        An array of n numbers
        
    Returns
    -------
        float:

    
    """
    assert len(x) == len(y), f"Inputs must be the same length: x={len(x)}, y={len(y)}"
    std_x   = np.std(x)
    std_y   = np.std(y)
    corr_xy = np.corrcoef(x, y)[0][1]
    diff    = np.mean(x) - np.mean(y)

    return (
        (2 * corr_xy * std_x * std_y)
        /
        (np.square(std_x) + np.square(std_y) + np.square(diff))
    )
