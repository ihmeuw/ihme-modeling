'''
Description: Defines the GPR model for adults and several helper functions
'''

from __future__ import division
from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

import scipy.stats.mstats as sp
import pylab as pl
import numpy as np
import os
import sys


'''
Define models
'''

def pmodel_func(x, pyear, pmort):
    all = pl.unique(zip(pyear,pmort))
    return pl.interp(x,all[:,0],all[:,1])


def gpmodel(prior_mort, prior_year, data_mort, data_year, data_stderr, data_category, scale, amp2, cc):

    #### Priors ####

    # Mean
    M = Mean(pmodel_func,pyear=prior_year,pmort=prior_mort)

    # Differentiability
    only_vr = (sum(data_category != 'complete VR') == 0)
    if only_vr == True:
        diff_degree = .8
    else:
        diff_degree = 2.

    # Covariance
    C = Covariance(matern.euclidean, diff_degree=diff_degree, amp=pl.sqrt(amp2), scale=scale)

    # GPR
    gpmort = GP('gpmort', M, C, mesh = pl.unique(prior_year))

    # Bias parameters
    if cc == 'DOM' or cc == 'PER' or cc == 'MAR' or cc == 'MDG':
        bias_vr = Normal('bias_vr', mu=0., tau=.01**-2)
        bias_sibs = Normal('bias_sibs',mu=0., tau=.01**-2)
    else:
        bias_vr = pl.array([0.])
        bias_sibs = pl.array([0.])

    #### Sampling Model ####

    # Complete VR
    index = (data_category == 'complete')
    if sum(index) > 0:
        @observed
        @stochastic
        def obs_completevr(mu=gpmort, tau=(1./((data_stderr[index])**2)), years=data_year[index], value=data_mort[index]):
            return normal_like(value, mu(years), tau)

    else:
        obs_completevr = []

    # Adjusted
    index = ((data_category == 'ddm_adjust') | (data_category == 'gb_adjust'))
    if sum(index) > 0:
        @observed
        @stochastic
        def obs_ddmadjusted(mu=gpmort, tau=(1./((data_stderr[index])**2)), years=data_year[index], value=data_mort[index], bias=bias_vr):
            return normal_like(value, mu(years) + bias, tau)

    else:
        obs_ddmadjusted = []

    # Sibs
    index = (data_category == 'sibs')
    if sum(index) > 0:
        @observed
        @stochastic
        def obs_siblinghistories(mu=gpmort, tau=(1./((data_stderr[index])**2)), years=data_year[index], value=data_mort[index], bias=bias_sibs):
            return normal_like(value, mu(years) + bias, tau)

    else:
        obs_siblinghistories = []

    # No adjust
    index = ((data_category == 'no_adjust') | (data_category == 'dss'))
    if sum(index) > 0:
        @observed
        @stochastic
        def obs_other(mu=gpmort, tau=(1./((data_stderr[index])**2)), years=data_year[index], value=data_mort[index]):
            return normal_like(value, mu(years), tau)

    else:
        obs_other = []

    # All data
    @deterministic
    def allobs(obs1=obs_completevr, obs2=obs_ddmadjusted, obs3=obs_siblinghistories, obs4=obs_other, bias2=bias_vr, bias3=bias_sibs):
        if len(obs2) != 0:
            obs2 = obs2 - bias2
        if len(obs3) != 0:
            obs3 = obs3 - bias3
        return pl.array(list(obs1) + list(obs2) + list(obs3) + list(obs4))

    allyear = pl.array(list(data_year[data_category == 'complete']) + list(data_year[(data_category == 'ddm_adjust') | (data_category == 'gb_adjust')]) + list(data_year[data_category == 'sibs']) + list(data_year[(data_category == 'no_adjust') | (data_category == 'dss')]))
    allvar = pl.array(list(data_stderr[data_category == 'complete']) + list(data_stderr[(data_category == 'ddm_adjust') | (data_category == 'gb_adjust')]) + list(data_stderr[data_category == 'sibs']) + list(data_stderr[(data_category == 'no_adjust') | (data_category == 'dss')]))**2

    return gpmort, allobs, allyear, allvar, bias_vr, bias_sibs


def gpmodel_all(prior_mort, prior_year, scale, amp2):
    # Mean
    M = Mean(pmodel_func, pyear=prior_year, pmort=prior_mort)

    # Covariance
    C = Covariance(matern.euclidean, diff_degree=2., amp=pl.sqrt(amp2), scale=scale)

    return M, C


'''
Define prediction functions
'''

def results(d):
    med = pl.mean(d,axis=0)
    lower = pl.array(sp.mquantiles(d,axis=0,prob=.025))[0]
    upper = pl.array(sp.mquantiles(d,axis=0,prob=.975))[0]
    std = pl.std(d,axis=0)
    return pl.np.core.records.fromarrays([med,lower,upper,std], [('med','<f8'),('lower','<f8'),('upper','<f8'),('std','<f8')])


def gpmodel_pred(gpmort, allyear, allvar, allobs, bias_vr, bias_sibs, sim, burn, thin, predictionyears):
    mc = MCMC([gpmort, allyear, allvar, allobs, bias_vr, bias_sibs])
    mc.use_step_method(GPNormal, gpmort, allyear, allvar, allobs)
    mc.sample(sim,burn,thin)
    d = pl.array([m(predictionyears) for m in gpmort.trace()])
    return(d)


def gpmodel_all_pred(M,C,sim,predictionyears):
    d = []
    gpr_seeds = [x+123456 for x in range(1,sim+1)]
    for ii in range(sim):
        np.random.seed(gpr_seeds[ii])
        x = Realization(M,C)
        d.append(x(predictionyears))
    d = pl.array(d)
    return(d)
