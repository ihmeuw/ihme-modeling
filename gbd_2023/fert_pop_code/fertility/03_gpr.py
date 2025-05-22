##############################
## Purpose: Gaussian Process Regression
## Details: Calculate data density
##          Choose space-time parameters 
##          Smooth predictions
###############################

import argparse
import pylab as pl
import os
import sys
import numpy as np
import pandas as pd
import getpass

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

user = getpass.getuser()

if sys.argv[0] == '':
  loc = 'LOCATION INPUT'
  model_age = '20'
  version_id = 'RUN ID'
  loop = '1'
  year_start = '1950'
  year_end = '2024'
else:
  parser = argparse.ArgumentParser()
  parser.add_argument('--loc', type=str, required=True,
                      action='store', help='The ihme_loc_id for GPR')
  parser.add_argument('--version_id', type=str, required=True,
                      action='store', help='The version_id for GPR')
  parser.add_argument('--year_start', type=int, required=True,
                      action='store', help='Starting year')
  parser.add_argument('--year_end', type=int, required=True,
                      action='store', help='Ending year')
  parser.add_argument('--loop', type=str, required=True,
                      action='store', help='Current loop')
  parser.add_argument('--model_age', type=str, required=True,
                      action='store', help='The age for GPR')
  args = parser.parse_args()
  
  version_id = args.version_id
  loc = args.loc
  year_start = args.year_start
  year_end = args.year_end
  loop = args.loop
  model_age = args.model_age

print(loc, user, model_age, version_id, loop)
os.linesep

sys.path.append('FILEPATH')
print('FILEPATH')

import gpr

loc_map = pd.read_csv('FILEPATH/loc_map.csv')

###############################################
###############   Import Data   #######################################################################################################
###############################################

os.chdir('FILEPATH')

data = pd.read_csv('age_' + model_age + '_gpr_input.csv')

## Data
index = (data['ihme_loc_id'] == loc) & (data['data'] == 1)
data_year = pl.array(data['year_id'][index])
data_fert = pl.array(data['adjusted_asfr_data'][index]) 
data_var = pl.array(data['logit_variance'][index])
data_category = pl.array(data['category'][index])

## Prior
index = (data['ihme_loc_id'] == loc)
prior_year = pl.array(data['year_id'][index])
prior_fert = data['stage2_pred'][index]

## Prediction years 
predictionyears = pl.array(range(int(year_start),int(year_end)+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0]) 

## Data categories
allcategory = []
allcategory.append('vr_other')
allcategory.append('vr_unbiased')
allcategory.append('other')

###############################################
############ Import best parameters ###################################################################################################
###############################################

os.chdir('FILEPATH')
data = pd.read_csv('age_' + model_age + '_params.csv')

best_scale = float(data['scale'][(data['ihme_loc_id']==loc)])
best_amp2x = 1
	
###############################################
############## Fit Model ##############################################################################################################
###############################################

## Fit model, get predictions 
if (len(data_year) == 0): # no data model 
	[M,C] = gpr.gpmodel_nodata(pyear=prior_year,pval=prior_fert,scale=best_scale,amp2x=best_amp2x,mse=mse,diff_degree=1)
else: # data model 
	[M,C] = gpr.gpmodel(loc,data_year,data_fert,data_var,data_category,allcategory,prior_year,prior_fert,mse,best_scale,best_amp2x,diff_degree=1,process='other')

	
## Find mean and standard error, drawing from M and C
draws = 1000
fert_draws = np.zeros((draws, len(predictionyears)))
gpr_seeds = [x+123456 for x in range(1,1001)]
for draw in range(draws):
	np.random.seed(gpr_seeds[draw])
	fert_draws[draw,:] = Realization(M, C)(predictionyears)

## Collapse across draws
# note: space transformations need to be performed at the draw level
fert_draws = gpr.inv_logit(fert_draws)

fert_draws = pd.DataFrame(fert_draws)
fert_draws.columns = predictionyears
fert_draws['ihme_loc_id'] = loc
fert_draws['sim'] = range(1000)
fert_draws = pd.melt(fert_draws, id_vars = ['ihme_loc_id','sim'], var_name = 'year', value_name = 'fert')

## Unscale backtransformed draws
bounds = pd.read_csv('FILEPATH/logit_bounds.csv')
upper_logit_bound = bounds.loc[bounds.age==int(model_age)].upper_bound.unique()
lower_logit_bound = bounds.loc[bounds.age==int(model_age)].lower_bound.unique()

fert_draws['fert'] = fert_draws['fert'] * (upper_logit_bound - lower_logit_bound) + lower_logit_bound
fert_draws['age'] = model_age

## Collapse unscaled and backtransformed draws
meandf = fert_draws[['ihme_loc_id', 'year', 'fert']].groupby(by = ['ihme_loc_id','year']).mean()
meandf = meandf.reset_index()
meandf['lower'] = fert_draws[['ihme_loc_id', 'year', 'fert']].groupby(by = 'year').quantile(q = .025).reset_index()['fert']
meandf['upper'] = fert_draws[['ihme_loc_id', 'year', 'fert']].groupby(by = 'year').quantile(q = .975).reset_index()['fert']
meandf = meandf.rename(index = str, columns = {"fert": "mean"})
meandf['age'] = model_age

## Save the predictions
os.chdir('FILEPATH')

raked_parents = loc_map[loc_map.ihme_loc_id.str.contains('_')]['parent_id']
raked_parents = loc_map[loc_map['location_id'].isin(raked_parents)]
raked_parents = raked_parents[raked_parents['level']==3]
raked_parents = raked_parents['ihme_loc_id']

if loc[0:3] in raked_parents.values and loc not in ['CHN_354', 'CHN_361']:
  os.chdir('FILEPATH')

with open('gpr_' + loc + '_' + model_age + '_sim.csv', mode='w', newline='\n') as f:
  fert_draws.to_csv(f)
with open('gpr_' + loc + '_' + model_age + '.csv', mode='w', newline='\n') as f:
  meandf.to_csv(f)


