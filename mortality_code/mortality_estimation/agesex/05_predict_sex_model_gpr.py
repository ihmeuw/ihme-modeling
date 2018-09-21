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

print(sys.argv[1])
print(sys.argv[2])
print(sys.argv[3]) 

user = sys.argv[3]
sys.path.append('PATH' % (user))
import gpr; reload(gpr)

os.chdir('PATH' % (user))


'''
Get GPR settings
'''

rr = sys.argv[1]
cc = sys.argv[2]
rnum = int(1)
hivsims = int(0)

'''
Import data
'''
os.chdir('PATH')
data = pl.csv2rec('gpr_input_file.csv', missing='NA')

# data
index = (data['ihme_loc_id'] == cc) & (data['data'] == 1)
data_year = pl.array(data['year_id'][index])
data_mort = pl.array(data['logit_q5_sexratio'][index]) 
data_var = pl.array(data['data_var'][index])
data_category = pl.array(data['category'][index])

# prior
index = (data['ihme_loc_id'] == cc)
prior_year = pl.array(data['year_id'][index])
prior_mort = data['pred_logitratio_s2'][index]

# prediction years 
predictionyears = pl.array(range(int(min(data['year_id'])),int(max(data['year_id']))+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0]) 

'''
Import best parameters 
'''

os.chdir('PATH' % ('PATH' if os.name=='posix' else 'PATH'))
data = pl.csv2rec('sex_model_params.txt', missing='NA')

best_scale = float(data['scale'][(data['best']==1) & (data['ihme_loc_id']==cc)][0])
best_amp2x = float(data['amp2x'][(data['best']==1) & (data['ihme_loc_id']==cc)][0])

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


logit_est = gpr.collapse_sims(mort_draws)
unlogit_est = gpr.collapse_sims(mort_draws)

os.chdir('FILEPATH')
all_est = []
for i in range(len(predictionyears)):
	all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('med', '<f8'), ('lower', '<f8'), ('upper', '<f8')])
pl.rec2csv(all_est, 'gpr_%s.txt' %cc)

# save the sims 
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((cc, predictionyears[i], s, mort_draws[s][i]))


all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])
pl.rec2csv(all_sim, 'gpr_%s_sim.txt' %cc)

