'''
Description: Runs GPR on all data using selected parameters
'''

import pylab as pl
import os
import sys
import numpy as np

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

user = sys.argv[5]
sys.path.append('FILEPATH')
import gpr; reload(gpr)

os.chdir('FILEPATH')


'''
Get GPR settings
'''

rr = sys.argv[1]
cc = sys.argv[2]
rnum = sys.argv[3]
hivsims = int(sys.argv[4])


'''
Import data
'''


if hivsims == 1:
	os.chdir('FILEPATH')
	data = pl.csv2rec('gpr_5q0_input_' + rnum +'.txt', missing='NA')
else:
	os.chdir('FILEPATH')
	data = pl.csv2rec('gpr_5q0_input.txt', missing='NA')

# data
index = (data['ihme_loc_id'] == cc) & (data['data'] == 1)
data_year = pl.array(data['year'][index])
data_mort = pl.array(data['logit_mort'][index])
data_var = pl.array(data['logit_var'][index])
data_category = pl.array(data['category'][index])

# prior
index = (data['ihme_loc_id'] == cc)
prior_year = pl.array(data['year'][index])
prior_mort = gpr.logit(data['pred2final'][index]) # this is to convert the prior to logit space

# prediction years 
predictionyears = pl.array(range(int(min(data['year'])),int(max(data['year']))+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0]) # mse NOT incorporated in 07_select_parameter

'''
Import best parameters 
'''
os.chdir('FILEPATH')
data = pl.csv2rec('selected_parameters.txt', missing='NA')

best_scale = float(data['scale'][(data['best']==1) & (data['ihme_loc_id']==cc)][0])
best_amp2x = float(data['amp2x'][(data['best']==1) & (data['ihme_loc_id']==cc)][0])

# create exception for MNE
if (cc == "MNE"):
	best_scale = 10


'''
Fit model with best parameters
'''

# fit model, get predictions 
if (len(data_year) == 0): # no data model 
	[M,C] = gpr.gpmodel_nodata(pyear=prior_year,pmort=prior_mort,scale=best_scale,predictionyears=predictionyears,sim=1000,amp2x=best_amp2x,mse=mse)
else: # data model 
	[M,C] = gpr.gpmodel(cc,rr,data_year,data_mort,data_var,data_category,prior_year,prior_mort,mse,best_scale,best_amp2x,predictionyears)

	
## find mean and standard error, drawing from M and C
draws = 1000
mort_draws = np.zeros((draws, len(predictionyears)))
gpr_seeds = [x+123456 for x in range(1,1001)]
for draw in range(draws):
	np.random.seed(gpr_seeds[draw])
	mort_draws[draw,:] = Realization(M, C)(predictionyears)

# collapse across draws
# note: space transformations need to be performed at the draw level
logit_est = gpr.collapse_sims(mort_draws)
unlogit_est = gpr.collapse_sims(gpr.inv_logit(mort_draws))

if hivsims == 0:
	os.chdir('FILEPATH/')
	all_est = []
	for i in range(len(predictionyears)):
		all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
	all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('med', '<f8'), ('lower', '<f8'), ('upper', '<f8')])
	pl.rec2csv(all_est, 'gpr_%s.txt' %cc)

# save the sims 
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((cc, predictionyears[i], s, gpr.inv_logit(mort_draws[s][i])))


all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])

if hivsims == 1: 
	os.chdir('FILEPATH')
	pl.rec2csv(all_sim, 'gpr_%s_%s_sim.txt' %(cc,rnum))
else:
	pl.rec2csv(all_sim, 'gpr_%s_sim.txt' %cc)
