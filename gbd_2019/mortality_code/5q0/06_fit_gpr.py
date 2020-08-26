'''
Description: Runs GPR on all data using selected parameters
'''
import pylab as pl
import os
import sys
import numpy as np
import getpass
import argparse
import pandas as pd

from pymc import *
  from pymc.gp import *
  from pymc.gp.cov_funs import matern

# Load GPR library
user = getpass.getuser()
code_dir = "FILEPATH""
sys.path.append(code_dir)
import gpr


# Get GPR settings
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--location_id', type=int, required=True,
                    action='store', help='The location_id for GPR')
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='The ihme_loc_id for GPR')
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id for GPR')
args = parser.parse_args()

location_id = args.location_id
ihme_loc_id = args.ihme_loc_id
version_id = args.version_id

version_dir = "FILEPATH/{}".format(version_id)
input_dir = "{}/model".format(version_dir)
output_dir = "{}/model".format(version_dir)


# Get data
input_file = "{}/gpr_input.csv".format(input_dir)
data = pd.read_csv(input_file)

# Create vectors of data
all_location_index = (data['ihme_loc_id'] == ihme_loc_id)
index = (data['ihme_loc_id'] == ihme_loc_id) & (data['data'] == 1)
region_name = pl.array(data['region_name'][all_location_index])[0]
print(region_name)
data_year = pl.array(data['year'][index])
data_mort = pl.array(data['logit_mort'][index])
data_var = pl.array(data['logit_var'][index])
data_category = pl.array(data['category'][index])

# Prior
index = (data['ihme_loc_id'] == ihme_loc_id)
prior_year = pl.array(data['year'][index])
prior_mort = gpr.logit(data['pred2final'][index]) # this is to convert the prior to logit space

# Prediction years
predictionyears = pl.array(range(int(min(data['year'])),int(max(data['year']))+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0])

'''
Import best parameters
'''
spacetime_parameter_file = "{}/data/assigned_hyperparameters.csv".format(version_dir)
data = pd.read_csv(spacetime_parameter_file)

best_scale = float(data['scale'][(data['best']==1) & (data['location_id']==location_id)].iloc[0])
best_amp2x = 1



'''
Fit model with best parameters
'''

# fit model, get predictions
if (len(data_year) == 0): # no data model
  [M,C] = gpr.gpmodel_nodata(pyear=prior_year,pmort=prior_mort,scale=best_scale,predictionyears=predictionyears,sim=1000,amp2x=best_amp2x,mse=mse)
else: # data model
  [M,C] = gpr.gpmodel(ihme_loc_id,region_name,data_year,data_mort,data_var,data_category,prior_year,prior_mort,mse,best_scale,best_amp2x,predictionyears)


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
# the difference of the mean of the antilogited draws from the antilogit of the mean of the draws 
mean_diff = np.subtract(unlogit_est,gpr.inv_logit(logit_est))


# save the sims
all_sim = []
for i in range(len(predictionyears)):
  for s in range(draws):
  all_sim.append((ihme_loc_id, predictionyears[i], s, gpr.inv_logit(mort_draws[s][i])-mean_diff[i]))

all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])
all_sim = pd.DataFrame(all_sim)
all_sim['ihme_loc_id'] = all_sim['ihme_loc_id'].str.decode('utf-8')


all_sim.to_csv("{}/gpr_{}.csv".format(output_dir, location_id), index = False)
