'''
Description: Defines the GPR models for kids 
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
   #gets unique tuples from a list
   checked = []
   for e in seq:
       if e not in checked:
           checked.append(e)
   return checked



def pmodel_func(x, pyear, pmort):
                all = zip(pyear,pmort)
                all = get_unique(all)
                all = sorted(all,key=lambda x: x[0])
                return pl.interp(x,[i[0] for i in all],[i[1] for i in all])


def collapse_sims(mort_draws):
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
# GPR model for non-data countries
def gpmodel_nodata(pyear,pmort,scale,predictionyears,sim,amp2x,mse):
	# Mean
	M = Mean(pmodel_func, pyear=pyear, pmort=pmort)

	# Covariance
	C = Covariance(matern.euclidean, diff_degree=2., amp=pl.sqrt(mse*amp2x), scale=scale)

	return M, C

# GPR model for countries with multiple data sources
def gpmodel(ihme_loc_id,region_name,year,log10_mort,log10_var,category,pyear,pmort,mse,scale,amp2x,predictionyears):


	"""
	Arguments:
	log10_mort - log10(5q0) data
	year - year of 5q0 est.
	log10_stderr - log10 standard errors for 5q0 est.
	category - data source category
	region_name - gbd region
	pyear - year of prior log10(5q0) est.
	pmort - prior log10(5q0) est.
	scale - scale to be used
	amp2x - amplitude multiplier
	mse - mse*amp2x = amplitude**2  
	predictionyears - years to predict estimates for
	"""

	# Split up data into categories 
	year_cat = {}
	mort_cat = {}
	var_cat = {}
	bias_vr_cat = {}

	allyear = []
	allmort = []

	# These are all the possible categories
	allcategory = []
	allcategory.append('dhs')
	allcategory.append('census')
	allcategory.append('mics')
	allcategory.append('cdc')
	allcategory.append('other')
	allcategory.append('vr_biased')
	allcategory.append('vr_unbiased')
	allcategory.append('vr_no_overlap')
	allcategory.append('papfam')
	allcategory.append('wfs')

	for acat in allcategory:
		year_cat[acat] = []
		mort_cat[acat] = []
		var_cat[acat] = []
	
	#category is a vector of data$category[data$ihme_loc_id == cc & data$data == 1]
	#outside loop: all possible categories, inside loop: all observations 
	#year_cat, etc. become vectors of all years (etc) w/ specific category of data
	for ucat in pl.unique(category):
		count = 0
		for cat in category:
			if cat == ucat:
				year_cat[ucat].append(year[count])
				mort_cat[ucat].append(log10_mort[count])
				var_cat[ucat].append(log10_var[count])
			count = count + 1
	
	#create vectors of all years/5q0 values in dataset
	for acat in allcategory:
		for ucat in pl.unique(category):
			if acat == ucat:
				allyear = allyear + year_cat[ucat]
				allmort = allmort + mort_cat[ucat]

	#make these vectors into pylabs arrays
	for ucat in pl.unique(category):
		year_cat[ucat] = pl.array(year_cat[ucat])
		mort_cat[ucat] = pl.array(mort_cat[ucat])
		var_cat[ucat] = pl.array(var_cat[ucat])

	allyear = pl.array(allyear)
	allmort = pl.array(allmort)

	# assign degree of differentiability based on VR-only/mixed source (with excepetions for 3 gbd regions and mauritius)
	vr_only = pl.unique(category).shape[0] & ('vr_unbiased' in category)
	if vr_only and region_name != 'Caribbean' and region_name != 'CaribbeanI' and region_name != 'Oceania' and ihme_loc_id != 'MUS':
		diff_degree = .8
	else:
		diff_degree = 2.

	###########################
	# Gaussian process priors #
	###########################

	# set mean prior as the values from the prediction model 
	M = Mean(pmodel_func, pyear=pyear, pmort=pmort)

	# set covariance
	C = Covariance(matern.euclidean, diff_degree=diff_degree, amp=pl.sqrt(mse*amp2x), scale=scale)

	###################
	# Sampling Models 
	###################

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

	observe(M, C, allyear, allobs, allvar)

	return M, C
