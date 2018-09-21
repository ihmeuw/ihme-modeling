'''
Description: Runs GPR on training data and calculates loss functions for testing data
'''

import scipy.stats.mstats as sp
import pylab as pl
import os
import sys
import numpy as np

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

user = sys.argv[1]
os.chdir('FILEPATH')
import gpr; reload(gpr)


'''
Get GPR settings
'''

rr = sys.argv[2]
cc = sys.argv[3]
ho = int(sys.argv[4])

'''
Import data
'''
if os.name == 'posix':
	os.chdir('FILEPATH')
else:
	os.chdir('FILEPATH')

data = pl.csv2rec('prediction_model_results_all_stages_%s_%i.txt' % (rr, ho), missing='NA')

# training data
index = (data['iso3'] == cc) & (data['data'] == 1) & (data['include'] == 'TRUE')
train_year = pl.array(data['year'][index])
train_mort = pl.array(data['logit_mort'][index])
train_var = pl.array(data['logit_var'][index])
train_category = pl.array(data['category'][index])


# testing data
index = (data['iso3'] == cc) & (data['data'] == 1) & (data['include'] == 'FALSE')
test_year = pl.array(data['year'][index])
test_mort = pl.array(data['logit_mort'][index])
test_var = pl.array(data['logit_var'][index])


# prior
index = (data['iso3'] == cc)
prior_year = pl.array(data['year'][index])
prior_mort = gpr.logit(pl.array(data['pred2final'][index])) # this is to convert the prior to logit space

# prediction years & mse 
predictionyears = pl.array(range(int(min(data['year'])),int(max(data['year']))+1)) + 0.5
mse = data['mse'][index][0] 

'''
Test all parameter combinations
'''

amp2x_list = [0.2,0.6,1.,3.,5.]
scale_list = [5.,10.,15.,20.,25.,30.]


if (len(test_mort) > 0):
	all_err = [] # set up holder for holdout results 
	all_est = [] # set up holder for predictions
	
	for amp2x in amp2x_list: # loop through amp
		for scale in scale_list: 
			print('amp2x %f and scale %f' % (amp2x, scale))
			
			## fit model 
			if (len(train_year) == 0): # no data model 
				[M,C] = gpr.gpmodel_nodata(pyear=prior_year,pmort=prior_mort,scale=scale,predictionyears=predictionyears,sim=100,amp2x=amp2x,mse=mse)
			else: # data model
				[M,C] = gpr.gpmodel(cc,rr,train_year,train_mort,train_var,train_category,prior_year,prior_mort,mse,scale,amp2x,predictionyears)
			
			## find mean and standard error, drawing from M and C
			draws = 1000
			#not setting seed here because the holdouts are random anyway
			mort_draws = np.zeros((draws, len(predictionyears)))
			for draw in range(draws):
				mort_draws[draw,:] = Realization(M, C)(predictionyears)
			
			# collapse across draws
			# note: space transformations need to be performed at the draw level
			logit_est = gpr.collapse_sims(mort_draws)
			unlogit_est = gpr.collapse_sims(gpr.inv_logit(mort_draws))
			
			## save the predictions
			for i in range(len(predictionyears)):
				all_est.append((rr, cc, ho, scale, amp2x, mse*amp2x, predictionyears[i], unlogit_est['med'][i], unlogit_est['std'][i]))
			
			## calculate error and save this too 
			for year, mort, var in zip(test_year, test_mort, test_var):
				pred_index = (predictionyears == year)
				re = (gpr.inv_logit(mort) - unlogit_est['med'][pred_index])/(gpr.inv_logit(mort))
				total_var = var + logit_est['std'][pred_index]**2
				coverage = int((logit_est['med'][pred_index] - 1.96*pl.sqrt(total_var)) < mort < (logit_est['med'][pred_index] + 1.96*pl.sqrt(total_var)))
				all_err.append((rr, cc, ho, scale, amp2x, mse*amp2x, year, mort, re, coverage))


## write files
if os.name == 'posix':
	os.chdir('FILEPATH')
else:
	os.chdir('FILEPATH') 

all_est = pl.array(all_est, [('gbd_region', '|S64'), ('iso3', '|S32'), ('ho', '<f8'), 
							 ('scale', '<f8'), ('amp2x', '<f8'), ('amp2', '<f8'), ('year', '<f8'), 
							 ('mort', '<f8'), ('std', '<f8')])
pl.rec2csv(all_est, 'gpr_%s_%i.txt' %(cc, ho))

if os.name == 'posix':
	os.chdir('FILEPATH')
else:
	os.chdir('FILEPATH') 

all_err = pl.array(all_err, [('gbd_region', '|S64'), ('iso3', '|S32'), ('ho', '<f8'), 
							 ('scale', '<f8'), ('amp2x', '<f8'), ('amp2', '<f8'), ('year', '<f8'), 
							 ('mort', '<f8'), ('re', '<f8'), ('coverage', '<f8')])
pl.rec2csv(all_err, 'loss_%s_%i.txt' %(cc, ho))

