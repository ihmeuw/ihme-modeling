'''
Description: Runs GPR on all data using selected parameters
'''
import os
import sys
import getpass
import argparse

import numpy as np
import pylab as pl
import pandas as pd
from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info('Setting up')

# Get GPR settings
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The location_id for GPR')
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='The ihme_loc_id for GPR')
parser.add_argument('--code_dir', type=str, required=True,
                    action='store', help='Directory where age-sex code is cloned')
args = parser.parse_args()

version_id = args.version_id
cc = args.ihme_loc_id
code_dir = args.code_dir
input_dir = "FILEPATH"

locations = pd.read_csv("FILEPATH")
rr = locations.loc[locations["ihme_loc_id"] == cc, "region_name"].item()

sys.path.append(code_dir)
import gpr
os.chdir(code_dir)

for sex in ['male', 'female']:
    for age in ["enn", "lnn", "pnn", "pna", "pnb", "inf", "ch", "cha", "chb"]:

        logging.info("Running {} {}".format(sex, age))

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
        data = pd.read_csv(input_file)

        # data
        index = (data['ihme_loc_id'] == cc) & (data['data'] == 1)
        data_year = pl.array(data['year_id'][index])
        data_mort = pl.array(data['log_qx_data'][index])
        data_var = pl.array(data['data_var'][index])
        data_category = pl.array(data['category'][index])

        # prior
        index = (data['ihme_loc_id'] == cc)
        prior_year = pl.array(data['year_id'][index])
        prior_mort = data['pred_log_qx_s2'][index]

        # prediction years
        predictionyears = pl.array(range(int(min(data['year_id'])),int(max(data['year_id']))+1)) + 0.5
        mse = pl.array(data['mse'][index])
        mse = float(mse[0])

        '''
        Import best parameters
        '''
        model_parameter_file = "FILEPATH"
        data = pd.read_csv(model_parameter_file)

        best_scale = float(data['scale'][(data['best']==1) & (data['ihme_loc_id']==cc)].iloc[0])
        best_amp2x = float(data['amp2x'][(data['best']==1) & (data['ihme_loc_id']==cc)].iloc[0])

        '''
        Fit model with best parameters
        '''
        # fit model, get predictions
        if (len(data_year) == 0): # no data model
            [M,C] = gpr.gpmodel_nodata(pyear=prior_year,
                                       pmort=prior_mort,
                                       scale=best_scale,
                                       predictionyears=predictionyears,
                                       sim=1000,
                                       amp2x=best_amp2x,
                                       mse=mse)
        else: # data model
            [M,C] = gpr.gpmodel(cc,rr,data_year,data_mort,data_var,data_category,prior_year,prior_mort,mse,best_scale,best_amp2x,predictionyears)


        ## find mean and standard error
        draws = 1000
        mort_draws = np.zeros((draws, len(predictionyears)))
        gpr_seeds = [x+123456 for x in range(1,1001)]
        for draw in range(draws):
            np.random.seed(gpr_seeds[draw])
            mort_draws[draw,:] = Realization(M, C)(predictionyears)

        # collapse across draws
        logit_est = gpr.collapse_sims(mort_draws)
        unlogit_est = gpr.collapse_sims(mort_draws)

        # save the predictions
        all_est = []
        for i in range(len(predictionyears)):
            all_est.append((cc, predictionyears[i], unlogit_est['med'][i], unlogit_est['lower'][i], unlogit_est['upper'][i]))
        labels = ['ihme_loc_id','year','med','lower','upper']
        all_est_df = pd.DataFrame.from_records(all_est, columns=labels)
        output_file = "FILEPATH"
        all_est_df.to_csv(output_file, index = False)

        # save the sims
        all_sim = []
        for i in range(len(predictionyears)):
            for s in range(draws):
                all_sim.append((cc, predictionyears[i], s, mort_draws[s][i]))
 
        labels = ['ihme_loc_id','year','sim','mort']
        all_sim_df = pd.DataFrame.from_records(all_sim, columns=labels)
        output_file = "FILEPATH"

        # first, assert not na
        logging.info('Asserting no NAs in gpr output for "FILEPATH"')
        nas = all_sim_df['mort'].notna()
        assert nas.all()

        all_sim_df.to_csv(output_file, index = False)
