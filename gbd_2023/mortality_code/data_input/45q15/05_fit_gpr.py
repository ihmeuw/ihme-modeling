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
import math

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern

import argparse

# Get GPR settings
parser = argparse.ArgumentParser()
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='The ihme_loc_id for GPR')
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id for GPR')
parser.add_argument('--code_dir', type=str, required=True,
                    action='store', help='Directory where code is cloned')
args = parser.parse_args()
ihme_loc_id = args.ihme_loc_id
version_id = args.version_id
code_dir = args.code_dir

# Load GPR library
sys.path.append(code_dir) 
import gpr

# Directories
version_dir = "FILEPATH"
input_dir = "FILEPATH"
output_dir = "FILEPATH"

# Get data
input_file = "FILEPATH"
data = pd.read_csv(input_file)

iters = range(1, 2)

for iter in iters:
	for ss in ['male','female']:

		'''
		Prep
		'''

		# Create vectors of data
		all_location_index = (data['ihme_loc_id'] == ihme_loc_id)
		index = (data['ihme_loc_id'] == ihme_loc_id) & (data['sex'] == ss) & (data['data'] == 1)
		region_name = pl.array(data['region_name'][all_location_index])[0]
		print(region_name)
		data_year = pl.array(data['year'][index])
		data_mort = pl.array(data['log_mort'][index])
		data_var = pl.array(data['log_var'][index])
		data_category = pl.array(data['category'][index])

		# Prior
		index = (data['ihme_loc_id'] == ihme_loc_id) & (data['sex'] == ss)
		prior_year = pl.array(data['year'][index])
		prior_mort = gpr.logit(data['pred.2.final'][index]) # this is to convert the prior to logit space

		# Prediction years
		predictionyears = pl.array(range(int(math.floor(data['year'].min())),int(math.floor(data['year'].max()+1)))) + 0.5
		
		# MSE
		mse = data['mse'][index].unique()

		# Import best parameters
		spacetime_parameter_file = "FILEPATH"
		param_data = pd.read_csv(spacetime_parameter_file)
		best_scale = float(param_data['scale'][(param_data['best']==1) & (param_data['ihme_loc_id']==ihme_loc_id) & (param_data['sex']==ss)].iloc[0])
		best_amp2x = float(param_data['amp2x'][(param_data['best']==1) & (param_data['ihme_loc_id']==ihme_loc_id) & (param_data['sex']==ss)].iloc[0])


		'''
		Fit model
		'''

		# fit Gaussian Process model
		print("Start fitting model for {}...".format(ss))
		if (len(data_year) == 0):
			print("no data model")
			[M,C] = gpr.gpmodel_nodata(pyear=prior_year,
									   pmort=prior_mort,
									   scale=best_scale,
									   predictionyears=predictionyears,
									   sim=1000,
									   amp2x=best_amp2x,
									   mse=mse)
		else:
			print("data model")
			[M,C] = gpr.gpmodel(ihme_loc_id=ihme_loc_id,
			                    region_name=region_name,
								year=data_year,
								log10_mort=data_mort,
								log10_var=data_var,
								category=data_category,
								pyear=prior_year,
								pmort=prior_mort,
								mse=mse,
								scale=best_scale,
								amp2x=best_amp2x,
								predictionyears=predictionyears)

		# Get 1000 draws (realizations) from M and C
		draws = 1000
		mort_draws = np.zeros((draws, len(predictionyears)))
		gpr_seeds = [x+123456 for x in range(1,1001)]
		for draw in range(draws):
			np.random.seed(gpr_seeds[draw])
			mort_draws[draw,:] = Realization(M, C)(predictionyears)

		# Collapse across draws
		# Note: space transformations need to be performed at the draw level
		print("collapse across draws...")
		logit_est = gpr.collapse_sims(mort_draws)
		unlogit_est = gpr.collapse_sims(gpr.inv_logit(mort_draws))
		# Take the difference of the mean of the antilogited draws from the antilogit of the mean of the draws 
		mean_diff = np.subtract(unlogit_est['med'],gpr.inv_logit(logit_est['med']))

		all_est = []
		for i in range(len(predictionyears)):
			all_est.append((ihme_loc_id,ss, predictionyears[i], unlogit_est['med'][i] - mean_diff[i], unlogit_est['lower'][i] - mean_diff[i], unlogit_est['upper'][i] - mean_diff[i]))

		'''
		Format and save
		'''

		# save summaries
		labels = ['ihme_loc_id','sex','year','mort_med','mort_lower', 'mort_upper']
		all_est_df = pd.DataFrame.from_records(all_est, columns=labels)
		est_file = "FILEPATH"
		all_est_df.to_csv(est_file, index = False)

		# save draws
		all_sim = []
		for i in range(len(predictionyears)):
			for s in range(draws):
				all_sim.append((ihme_loc_id, predictionyears[i], s, gpr.inv_logit(mort_draws[s][i]) - mean_diff[i]))

		labels = ['ihme_loc_id','year','sim','mort']
		all_sim_df = pd.DataFrame.from_records(all_sim, columns=labels)
		sim_file = "FILEPATH"
		all_sim_df['sex'] = ss
		all_sim_df.to_csv(sim_file, index = False)
