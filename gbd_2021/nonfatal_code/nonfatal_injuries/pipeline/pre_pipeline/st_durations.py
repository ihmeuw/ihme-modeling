"""Creates draws for treated and untreated short term durations.

Uses expert estimates and results of Netherlands ISS analysis for mean, upper, and lower durations of each ncode,
 inpatient and outpatient, and the multiplier for duration if untreated.

To run, simply call main(). Does not need to be updated between GBD rounds unless we get new empirical duration data."""

import pandas as pd
import numpy as np
from gbd_inj.pipeline.helpers import utilities
import os


def main():
    # load file
    filepath = FILEPATH
    raw = pd.read_excel(filepath, sheet_name='short-term durations', header=None, skiprows=9, index_col=0)

    # subset to the right data
    inpatient = raw[[2, 3, 4, 5]]
    outpatient = raw[[6, 7, 8, 9]]
    mults = raw.reset_index()[[0, 10, 11, 12]]

    inpatient.rename(columns={2: 'mean', 3: 'se', 4: 'll', 5: 'ul'}, inplace=True)
    outpatient.rename(columns={6: 'mean', 7: 'se', 8: 'll', 9: 'ul'}, inplace=True)
    mults.rename(columns={0: 'ncode', 10: 'mean', 11: 'll', 12: 'ul'}, inplace=True)

    treated = pd.concat([inpatient, outpatient], keys=['inpatient', 'outpatient'], names=['platform', 'ncode'])

    # create SE where it doesn't already exist
    treated['se'] = treated['se'].fillna((treated['ul']-treated['ll'])/3.92)
    mults['se'] = (mults['ul'] - mults['ll']) / 3.92

    # convert to years
    treated = treated/365.25

    # make draws
    treated.reset_index(inplace=True) 
    np.random.seed(81112)
    treated[utilities.drawcols()] = pd.DataFrame(np.random.normal(treated['mean'], treated['se'], size=(1000, len(treated))).T)
    mults[utilities.drawcols()] = pd.DataFrame(np.random.normal(mults['mean'], mults['se'], size=(1000, len(mults))).T)

    # format
    treated.drop(['mean', 'se', 'll', 'ul'], axis=1, inplace=True)
    treated.set_index(['ncode', 'platform'], inplace=True)
    mults.drop(['mean', 'se', 'll', 'ul'], axis=1, inplace=True)
    mults.set_index(['ncode'], inplace=True)

    # make untreated
    treated[treated<0] = 0 
    treated[treated>1] = 1  
    untreated = treated * mults
    untreated[untreated>1] = 1

    outdir = FILEPATH
    treated.sort_index().to_csv(FILEPATH)
    untreated.sort_index().to_csv(FILEPATH)


def lognormal():
    # load file
    filepath = FILEPATH
    raw = pd.read_excel(filepath, sheet_name='short-term durations', header=None, skiprows=9, index_col=0)

    # subset to the right data
    inpatient = raw[[2, 3, 4, 5]]
    outpatient = raw[[6, 7, 8, 9]]
    mults = raw.reset_index()[[0, 10, 11, 12]]

    inpatient.rename(columns={2: 'mean', 3: 'se', 4: 'll', 5: 'ul'}, inplace=True)
    outpatient.rename(columns={6: 'mean', 7: 'se', 8: 'll', 9: 'ul'}, inplace=True)
    mults.rename(columns={0: 'ncode', 10: 'mean', 11: 'll', 12: 'ul'}, inplace=True)

    treated = pd.concat([inpatient, outpatient], keys=['inpatient', 'outpatient'], names=['platform', 'ncode'])

    # create SE where it doesn't already exist
    treated['se'] = treated['se'].fillna((treated['ul'] - treated['ll']) / 3.92)
    mults['se'] = (mults['ul'] - mults['ll']) / 3.92

    # convert to years
    treated = treated / 365.25

    treated['mu'] = np.log((treated['mean']**2)/np.sqrt(treated['se']**2 + treated['mean']**2))
    treated['sig'] = np.sqrt(np.log(1 + (treated['se'] / treated['mean'])**2))

    # make draws
    treated.reset_index(inplace=True) 
    np.random.seed(81112)
    treated[utilities.drawcols()] = pd.DataFrame(np.random.lognormal(treated['mu'], treated['sig'], size=(1000, len(treated))).T)
    mults[utilities.drawcols()] = pd.DataFrame(np.random.normal(mults['mean'], mults['se'], size=(1000, len(mults))).T)

    # format
    treated.drop(['mean', 'se', 'll', 'ul', 'mu', 'sig'], axis=1, inplace=True)
    treated.set_index(['ncode', 'platform'], inplace=True)
    mults.drop(['mean', 'se', 'll', 'ul'], axis=1, inplace=True)
    mults.set_index(['ncode'], inplace=True)

    # make untreated
    mults[mults < 0] = 0 
    treated[treated > 1] = 1  
    untreated = treated * mults
    untreated[untreated > 1] = 1

    outdir = FILEPATH
    treated.sort_index().to_csv(FILEPATH)
    untreated.sort_index().to_csv(FILEPATH)
