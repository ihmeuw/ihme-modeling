##############################
## Purpose: Run GPR
##############################

import pylab as pl
import os
import sys
import numpy as np
import pandas as pd

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

param = pd.read_csv(FILEPATH + '/param_map.csv')
task_id = int(os.environ['SGE_TASK_ID'])-1

rr = str(param.region_name[task_id])
cc = str(param.model_locs[task_id])
user = str(param.username[task_id])
age = str(param.model_ages[task_id])
version = str(param.version[task_id])
loop = str(param.loop[task_id])
gbd_year = str(param.gbd_year[task_id])

print(rr)
print(cc)
print(user)
print(age)
print(version)
print(loop)

sys.path.append(FILEPATH)

print('sys.path.append')

import gpr

os.chdir(FILEPATH)

loc_map = pd.read_csv(FILEPATH + 'loc_map.csv')

print('loc_map')

'''
Get GPR settings
'''

rnum = int(1)
hivsims = int(0)


'''
Import data
'''

os.chdir(FILEPATH)


# df = pd.read_csv('age_' + age + '_results.csv', encoding='latin1')
# df.to_csv('age_' + age + '_results.csv', encoding='utf-8', index=False)

data = pd.read_csv('age_' + age + '_results.csv')

os.chdir(FILEPATH + '/stage1and2/')

bounds = pd.read_csv('logit_bounds.csv')


# data
index = (data['ihme_loc_id'] == cc) & (data['data'] == 1)
data_year = pl.array(data['year_id'][index])
data_mort = pl.array(data['adjusted_logit_asfr_data'][index])
data_var = pl.array(data['data_var'][index])
data_category = pl.array(data['category'][index])

# prior
index = (data['ihme_loc_id'] == cc)
prior_year = pl.array(data['year_id'][index])
prior_mort = data['stage2_pred'][index]

# prediction years
predictionyears = pl.array(range(int(min(data['year_id'])),int(max(data['year_id']))+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0])

'''
Import best parameters
'''

os.chdir(FILEPATH)
data = pd.read_csv('asfr_params_age_' + age + '.txt')

best_scale = float(data['scale'][(data['best']==1) & (data['ihme_loc_id']==cc)].iloc[0])
best_amp2x = float(data['amp2x'][(data['best']==1) & (data['ihme_loc_id']==cc)].iloc[0])

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
logit_est = gpr.collapse_sims(mort_draws)
mort_draws = gpr.inv_logit(mort_draws)

mort_draws = pd.DataFrame(mort_draws)
mort_draws.columns = predictionyears
mort_draws['ihme_loc_id'] = cc
mort_draws['sim'] = list(range(1000))
mort_draws = pd.melt(mort_draws, id_vars = ['ihme_loc_id','sim'], var_name = 'year', value_name = 'mort')

# Unscale backtransformed draws
upper_logit_bound = bounds[bounds['age'] == int(age)]['upper_bound'].iloc[0]
lower_logit_bound = bounds[bounds['age'] == int(age)]['lower_bound'].iloc[0]
mort_draws['mort'] = mort_draws['mort'] * (upper_logit_bound - lower_logit_bound) + lower_logit_bound
mort_draws = mort_draws.rename(index = str, columns = {"mort" : "val"})

# Collapse unscaled and backtransformed draws
meandf = mort_draws[['ihme_loc_id', 'year', 'val']].groupby(by = ['ihme_loc_id','year']).mean()
meandf = meandf.reset_index()
meandf['lower'] = mort_draws[['ihme_loc_id', 'year', 'val']].groupby(by = 'year').quantile(q = .025).reset_index()['val']
meandf['upper'] = mort_draws[['ihme_loc_id', 'year', 'val']].groupby(by = 'year').quantile(q = .975).reset_index()['val']
meandf = meandf.rename(index = str, columns = {"val": "mean"})


# save the predictions
os.chdir(FILEPATH)

raked_parents = ['CHN', 'USA', 'KEN', 'IND', 'IDN', 'ETH', 'IRN', 'MEX', 'ZAF', 'GBR', 'JPN', 'NZL', 'NOR', 'UKR', 'SWE', 'ITA', 'POL',
                  'PAK', 'NGA', 'PHL', 'RUS', 'BRA']

if cc[0:3] in raked_parents and cc not in ['CHN_354', 'CHN_361']:
  os.chdir(FILEPATH)

mort_draws.to_csv('gpr_' + cc + '_' + age + '_sim.csv')
meandf.to_csv('gpr_' + cc + '_' + age +'.csv')
