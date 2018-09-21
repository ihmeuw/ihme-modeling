'''
Description: Runs GPR on all data using selected parameters 
Modified to run for fertility 
'''

import pylab as pl
import os
import sys
import numpy as np
import math
import getpass
import re

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern


user = getpass.getuser()
sys.path.append('FILEPATH' % (user))
import gpr; reload(gpr)

os.chdir('FILEPATH' % (user))

#mod_id
mod_id = str(sys.argv[3])
tfr_bound = 9.5

'''
Get GPR settings
'''


rr = sys.argv[1]
cc = sys.argv[2]

manual_parameters = True

'''
Import data
'''



os.chdir('FILEPATH')
data = pl.csv2rec('FILEPATH)

# data
index = (data['ihme_loc_id'] == cc)

data_year = pl.array(data['year'][index])

data_fert = pl.array(data['logit_bound_tfr'][index])
data_var = pl.array(data['logit_bound_var'][index])
data_category = pl.array(data['category'][index])

# prior
data = pl.csv2rec("FILEPATH")


index = (data['ihme_loc_id'] == cc)
prior_year = pl.array(data['year'][index])
prior_fert = pl.array(data['logit_bound_tfr_pred_smooth'][index])


# prediction years 
predictionyears = pl.array(range(int(min(data['year'])),int(max(data['year']))+1)) + 0.5
mse = pl.array(data['mse'][index])
print(mse)
mse = float(mse[0]) 

'''
Fit model with best parameters
'''

if(manual_parameters==True):
	best_scale= int(sys.argv[5])
	best_amp2x = int(sys.argv[4])

print(best_scale)
print(best_amp2x)

# fit model, get predictions 
if (len(data_year) == 0): # no data model 
	[M,C] = gpr.gpmodel_nodata(pyear=prior_year,pfert=prior_fert,scale=best_scale,predictionyears=predictionyears,sim=1000,amp2x=best_amp2x,mse=mse)
	print('used')
  
else: # data model 
	[M,C] = gpr.gpmodel(cc,rr,data_year,data_fert,data_var,data_category,prior_year,prior_fert,mse,best_scale,best_amp2x,predictionyears)

	

## find mean and standard error, drawing from M and C
draws = 1000
tfr_draws = np.zeros((draws, len(predictionyears)))
gpr_seeds = [x+123456 for x in range(1,1001)]
for draw in range(draws):
	np.random.seed(gpr_seeds[draw])
	tfr_draws[draw,:] = Realization(M, C)(predictionyears)


# collapse across draws
# note: space transformations need to be performed at the draw level
logit_est = gpr.collapse_sims(tfr_draws)
unlogit_est = gpr.collapse_sims(np.exp(tfr_draws)*tfr_bound/(1+np.exp(tfr_draws))) # get the inverse logit



os.chdir('FILEPATH')

all_est = []
for i in range(len(predictionyears)):
	all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('med', '<f8'), ('lower', '<f8'), ('upper', '<f8')])
pl.rec2csv(all_est, 'gpr_%s.txt' %(cc+'_'+ str(best_amp2x) + '_' + str(best_scale)))

# save the sims 
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((cc, predictionyears[i], s, np.exp(tfr_draws[s][i])*tfr_bound/ (1+np.exp(tfr_draws[s][i])) ))


all_sim = pl.array(all_sim, [('ihme_loc', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('fert', '<f8')])


pl.rec2csv(all_sim, 'gpr_%s_sim.txt' %(cc+ '_' + str(best_amp2x) + '_' + str(best_scale)))

