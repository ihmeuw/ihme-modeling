'''
Description: Runs GPR on all data using selected parameters
'''

import pylab as pl
import os
import sys
import numpy as np
import pandas as pd
import getpass
import argparse

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

user = getpass.getuser()
task_id = int(os.getenv("SLURM_ARRAY_TASK_ID"))

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--main_std_def', type=str, required=True,
                    action='store', help='definition for stillbirth gestational age')
parser.add_argument('--version_estimate', type=int, required=True,
                    action='store', help='track estimate model version')
parser.add_argument('--working_dir', type=str, required=True,
                    action='store', help='directory where code is cloned')

args = parser.parse_args()
main_std_def = args.main_std_def
version_estimate = args.version_estimate
working_dir = args.working_dir

os.chdir('FILEPATH' % (version_estimate))
locs_w_regions = pd.read_csv('FILEPATH')

loc_parameter = locs_w_regions.loc[task_id - 1]

sys.path.append('FILEPATH' % (user))

import gpr

os.chdir(working_dir)

'''
Get GPR settings
'''

rr = loc_parameter["region_name"]
cc = loc_parameter["ihme_loc_id"]

'''
Import data
'''

os.chdir('FILEPATH' % (version_estimate))
data = pd.read_csv('FILEPATH'.format(main_std_def))

# data
index = (data['ihme_loc_id'] == cc) & (data['data'] == 1)
data_year = pl.array(data['year_id'][index])
data_mort = pl.array(data['log_mean_adj'][index])
data_var = pl.array(data['data_var'][index])
data_category = pl.array(data['category'][index])

# prior
index = (data['ihme_loc_id'] == cc)
prior_year = pl.array(data['year_id'][index])
prior_mort = data['pred_log_mean_st'][index]

# prediction years
predictionyears = pl.array(range(int(min(data['year_id'])), int(max(data['year_id']))+1)) + 0.5
mse = pl.array(data['mse'][index])
mse = float(mse[0])

'''
Import hyperparameters (calculated in the calculate_hyperparameters.R script)
'''

os.chdir('FILEPATH' % (version_estimate))
data = pd.read_csv('FILEPATH')

best_scale = float(data['scale'][(data['best'] == 1) & (data['ihme_loc_id'] == cc)].iloc[0])
best_amp2x = 1 # float(data['amp2x'][(data['best'] == 1) & (data['ihme_loc_id'] == cc)].iloc[0])

'''
Fit model
'''

# fit model, get predictions
if (len(data_year) == 0): # no data model
	[M,C] = gpr.gpmodel_nodata(pyear=prior_year,pmort=prior_mort,scale=best_scale,predictionyears=predictionyears,sim=1000,amp2x=best_amp2x,mse=mse)
else: # data model
	[M,C] = gpr.gpmodel(cc,rr,data_year,data_mort,data_var,data_category,prior_year,prior_mort,mse,best_scale,best_amp2x,predictionyears)

# find mean and standard error, drawing from M and C
draws = 1000
mort_draws = np.zeros((draws, len(predictionyears)))
gpr_seeds = [x+123456 for x in range(1,1001)]
for draw in range(draws):
	np.random.seed(gpr_seeds[draw])
	mort_draws[draw,:] = Realization(M, C)(predictionyears)

# collapse across draws
# note: space transformations need to be performed at the draw level
logit_est = gpr.collapse_sims(mort_draws)
unlogit_est = gpr.collapse_sims(mort_draws)

os.chdir('FILEPATH' % (version_estimate))
all_est = []

for i in range(len(predictionyears)):
	all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('med', '<f8'), ('lower', '<f8'), ('upper', '<f8')])

pd.DataFrame(all_est).to_csv('FILEPATH'.format(cc, main_std_def) % (version_estimate))

# save the sims
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((cc, predictionyears[i], s, mort_draws[s][i]))

all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])
pd.DataFrame(all_sim).to_csv('FILEPATH'.format(cc, main_std_def) % (version_estimate))
