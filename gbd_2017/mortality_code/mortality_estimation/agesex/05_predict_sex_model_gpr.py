'''
Description: Runs GPR on all data using selected parameters
'''
import os
import sys
import getpass
import argparse

import numpy as np
import pylab as pl

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern



# Get GPR settings
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The location_id for GPR')
parser.add_argument('--region_name', type=str, required=True,
                    action='store', help='The region_name for GPR')
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='The ihme_loc_id for GPR')
args = parser.parse_args()

# region_name = args.region_name
version_id = args.version_id
rr = args.region_name
cc = args.ihme_loc_id
user = getpass.getuser()

import gpr; reload(gpr)

input_dir = "FILEPATH"
output_dir = "FILEPATH"

try:
	os.makedirs(output_dir)
except:
	pass

'''
Get GPR settings
'''
rnum = int(1)
hivsims = int(0)

'''
Import data
'''
input_file = "FILEPATH"
data = pl.csv2rec(input_file, missing='NA')

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

model_parameter_file = "FILEPATH"
data = pl.csv2rec(model_parameter_file, missing='NA')

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

# collapse across draws
# note: space transformations need to be performed at the draw level
# not actually doing any transformations here, we'll do after
logit_est = gpr.collapse_sims(mort_draws)
unlogit_est = gpr.collapse_sims(mort_draws)

# save the predictions
all_est = []
for i in range(len(predictionyears)):
	all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('med', '<f8'), ('lower', '<f8'), ('upper', '<f8')])

output_file = "FILEPATH"
pl.rec2csv(all_est, output_file)

# save the sims
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((cc, predictionyears[i], s, mort_draws[s][i]))


all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])

output_file = "FILEPATH"
pl.rec2csv(all_sim, output_file)
