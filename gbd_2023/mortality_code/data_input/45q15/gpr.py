'''
Description: Defines the GPR model for adults and several helper functions
'''

from __future__ import division
from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

import pdb
import scipy.stats.mstats as sp
import pylab as pl
import os
import sys
import numpy as np


'''
Define Helper Functions

'''

def get_unique(seq):
    """
    gets unique tuples from a list
    """
    checked = []
    for e in seq:
        if e not in checked:
            checked.append(e)
    return checked


def pmodel_func(x, pyear, pmort):
    """
    Interpolate input to get mean function prior
    """
    all = zip(pyear,pmort)
    all = get_unique(all)
    all = sorted(all,key=lambda x: x[0])
    return pl.interp(x,[i[0] for i in all],[i[1] for i in all])


def collapse_sims(mort_draws):
    """
    draws to med lower upper with std
    """
    med = pl.mean(mort_draws, axis=0)
    lower = pl.array(sp.mquantiles(mort_draws,axis=0,prob=.025))[0]
    upper = pl.array(sp.mquantiles(mort_draws,axis=0,prob=.975))[0]
    std = pl.std(mort_draws,axis=0)
    return pl.np.core.records.fromarrays([med,lower,upper,std], [('med','<f8'),('lower','<f8'),('upper','<f8'),('std','<f8')]) 


def logit(p):
    return np.log((p)/(1-p))


def inv_logit(p):
    return 1/(1+np.exp(-p))


'''
Define Models
'''

def gpmodel_nodata(pyear, pmort, scale, predictionyears, sim, amp2x, mse):
    """
    Gaussian process model, no data
    
    Notes:
    uses pymc.gp methods
    """
    # Mean
    M = Mean(pmodel_func, pyear=pyear, pmort=pmort)

    # Covariance
    C = Covariance(matern.euclidean, diff_degree=1., amp=pl.sqrt(mse*amp2x), scale=scale)

    return M, C


def gpmodel(ihme_loc_id, region_name, year, log10_mort, log10_var,
            category, pyear, pmort, mse, scale, amp2x, predictionyears):

    """
    Gaussian Process model, for countries with multiple data sources

    Arguments:
    ihme_loc_id
    region_name - gbd region
    year - year of estimate
    log10_mort - log10 of data
    log10_var - log10 of variance
    category - data source category
    pyear - year of prior for log10 estimate
    pmort - prior for log10 estimate
    mse - mse*amp2x = amplitude**2
    scale - scale to be used
    amp2x - amplitude multiplier
    predictionyears - years to predict estimates for
    """

    ###########################
    # Gaussian process priors #
    ###########################

    # set mean prior as the values from the prediction model
    M = Mean(pmodel_func, pyear=pyear, pmort=pmort)

    # set covariance
    C = Covariance(matern.euclidean, diff_degree=1., amp=pl.sqrt(mse*amp2x), scale=scale)


    ##################
    # Sampling Model #
    ##################

    # Split up data into categories 
    year_cat = {}
    mort_cat = {}
    var_cat = {}
    bias_vr_cat = {}

    allyear = []
    allmort = []

    # These are all the possible categories
    allcategory = []
    allcategory.append('complete')
    allcategory.append('ddm_adjust')
    allcategory.append('gb_adjust')
    allcategory.append('sibs')
    allcategory.append('no_adjust')
    allcategory.append('dss')

    for acat in allcategory:
        year_cat[acat] = []
        mort_cat[acat] = []
        var_cat[acat] = []
    
    # category is a vector of data$category[data$ihme_loc_id == cc & data$data == 1]
    # outside loop: all possible categories, inside loop: all observations 
    # year_cat, etc. become vectors of all years (etc) w/ specific category of data
    for ucat in pl.unique(category):
        count = 0
        for cat in category:
            if cat == ucat:
                year_cat[ucat].append(year[count])
                var_cat[ucat].append(log10_var[count])
                if ((ihme_loc_id in ['DOM' , 'PER' , 'MAR' , 'MDG']) & (cat in ["sibs","ddm_adjust","gb_adjust"])):
                    mort_cat[ucat].append(log10_mort[count]  - rnormal(mu=0., tau=.01**-2))
                else:
                    mort_cat[ucat].append(log10_mort[count])
            count = count + 1
    
    # create vectors of all years/values in dataset
    for acat in allcategory:
        for ucat in pl.unique(category):
            if acat == ucat:
                allyear = allyear + year_cat[ucat]
                allmort = allmort + mort_cat[ucat]

    # make these vectors into pylabs arrays
    for ucat in pl.unique(category):
        year_cat[ucat] = pl.array(year_cat[ucat])
        mort_cat[ucat] = pl.array(mort_cat[ucat])
        var_cat[ucat] = pl.array(var_cat[ucat])

    allyear = pl.array(allyear)
    allmort = pl.array(allmort)

    # set up inputs to observe function
    # dimensions for predictions
    allobs = np.zeros(len(log10_mort))
    allvar = np.zeros(len(log10_mort))
    allyear = np.zeros(len(log10_mort))
    j = 0

    for cat in mort_cat.keys():
        for i in range(len(mort_cat[cat])):
            allobs[j] = mort_cat[cat][i]
            allvar[j] = var_cat[cat][i] 
            allyear[j] = year_cat[cat][i]
            j += 1

    # use observe method to get predictions from M and C
    observe(M, C, allyear, allobs, allvar)

    return M, C