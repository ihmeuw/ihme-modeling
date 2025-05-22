'''
Description: Runs GPR on all data using selected parameters

## INPUT: From Intermediate directory: ST predictions, hyper parameters
## OUTPUT: gpr predictions, gpr sims

## STEPS
##  A. Read in Directories and GPR Code
##  B. Obtain Prediction Data
##  C. Import `best` Parameters
##  D. Fit Model with `best` Parameters
##  E. Save GPR Data
'''
import argparse
import getpass
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pylab as pl
import scipy.stats.mstats as sp
import sys
import yaml

from pymc import *
from pymc.gp import *
from pymc.gp.cov_funs import matern
from configparser import ConfigParser as config

'''
# A. Read in Directories -----------------------------------------------------------------------
'''

# Set up the argument parser
parser = argparse.ArgumentParser(description="Base directory for this version run")
parser.add_argument("--main_dir", type=str, 
                    help="base directory for this version run")
parser.add_argument("--location_id", type=int, 
                    help="location_id")
parser.add_argument("--ihme_loc_id", type=str, 
                    help="ihme location id")

args = parser.parse_args()

# Set the directory as an environment variable if provided via command line
if args.main_dir:
    os.environ["main_dir"] = args.main_dir

# Define a base directory
if sys.flags.interactive:
    version_id = str("Run id") 
    main_dir = "FILEPATH"

    location_id = "Input location id"
    ihme_loc_id = "Input ihme_loc_id"
    if not os.path.isdir(main_dir):
        raise Exception("specify valid 'main_dir'")

# Set 'main_dir' as an environment variable if it's determined by the script
else:
    main_dir = args.main_dir
    location_id = args.location_id
    ihme_loc_id = args.ihme_loc_id
os.environ["main_dir"] = main_dir

# Load the configuration from the YAML file
config_path = os.path.join(main_dir, "srb_detailed.yml")
if not os.path.exists(config_path):
    raise Exception(f"Config file does not exist: {config_path}")

with open(config_path, "r") as stream:
    config = yaml.safe_load(stream)

## Use config["default"] to define variables from .yml file
config = config["default"]
version_id = config["version_id"]  
input_dir = config["input_dir"] 
output_dir = config["output_dir"]  
code_dir = config["code_dir"]
gpr_dir = config["gpr_dir"]
user = config["username"] 

# Load GPR library
sys.path.append(code_dir)
print("sys.path.append")
from gpr import get_unique, pmodel_func, collapse_sims, logit, inv_logit, gpmodel_nodata, gpmodel

'''
# B. Get Prediction Data -----------------------------------------------------------------------
'''

gpr_file = "{}gpr_input.csv".format(input_dir)
gpr_data = pd.read_csv(gpr_file)

# Create vectors of data
all_location_index = (gpr_data["ihme_loc_id"] == ihme_loc_id)
index = (gpr_data["ihme_loc_id"] == ihme_loc_id) & (gpr_data["data"].notnull()) 
region_name = pl.array(gpr_data["region_name"][all_location_index])[0]
print(region_name)
gpr_data_year = pl.array(gpr_data["year"][index])
gpr_data_val = pl.array(gpr_data["logit_val"][index])
gpr_data_var = pl.array(gpr_data["data_var"][index])
gpr_data_category = pl.array(gpr_data["category"][index])

# Prior
prior_index = (gpr_data["ihme_loc_id"] == ihme_loc_id)
prior_year = pl.array(gpr_data["year"][prior_index])
prior_val = logit(gpr_data["val_preds2"][prior_index]) # this is to convert the prior to logit space

# Prediction years
predictionyears = pl.array(range(int(min(gpr_data["year"])),int(max(gpr_data["year"]))+1)) + 0.5
mse = pl.array(gpr_data["mse"][prior_index])
mse = float(mse[0])

'''
C. Import best parameters -----------------------------------------------------------------------
'''

spacetime_parameter_file = "{}hyper_params.csv".format(input_dir)
st_data = pd.read_csv(spacetime_parameter_file)

best_scale = float(st_data["scale"][(st_data["best"]==1) & (st_data["location_id"]==location_id)].iloc[0])
best_amp2x = 1 

'''
D. Fit model with best parameters -------------------------------------------------------------
'''

# fit model, get predictions
if (len(gpr_data_year) == 0): # no data model
    [M,C] = gpmodel_nodata(pyear=prior_year, pval=prior_val, scale=best_scale, predictionyears=predictionyears, sim=1000, amp2x=best_amp2x, mse=mse)
else: # data model
    [M,C] = gpmodel(ihme_loc_id, region_name, gpr_data_year, gpr_data_val, gpr_data_var, gpr_data_category, prior_year, prior_val, mse, best_scale, best_amp2x, predictionyears)


## find mean and standard error, drawing from M and C
draws = 1000
val_draws = np.zeros((draws, len(predictionyears)))
gpr_seeds = [x+123456 for x in range(1,1001)]
for draw in range(draws):
    np.random.seed(gpr_seeds[draw])
    val_draws[draw,:] = Realization(M, C)(predictionyears)

# collapse across draws
# note: space transformations need to be performed at the draw level
logit_est = collapse_sims(val_draws)
unlogit_est = collapse_sims(inv_logit(val_draws))
# the difference of the mean of the antilogited draws from the antilogit of the mean of the draws 
mean_diff = np.subtract(unlogit_est["med"], inv_logit(logit_est["med"]))

'''
E. Save gpr data -------------------------------------------------------------
'''

# save the predictions
all_est = []
for i in range(len(predictionyears)):
	all_est.append((ihme_loc_id, predictionyears[i], unlogit_est["med"][i] - mean_diff[i], unlogit_est["lower"][i] - mean_diff[i], unlogit_est["upper"][i] - mean_diff[i]))
labels = ["ihme_loc_id","year","med","lower","upper"]
ihme_loc_gpr_file = "{}gpr_{}.txt".format(gpr_dir, ihme_loc_id) 
all_est_df = pd.DataFrame.from_records(all_est, columns=labels)
all_est_df.to_csv(ihme_loc_gpr_file, index = False)

# save the sims
all_sim = []
for i in range(len(predictionyears)):
	for s in range(draws):
		all_sim.append((ihme_loc_id, predictionyears[i], s, inv_logit(val_draws[s][i])-mean_diff[i]))
labels = ["ihme_loc_id","year","sim","val"]
ihme_loc_sim_file = "{}gpr_{}_sim.txt".format(gpr_dir, ihme_loc_id) 
all_sim_df = pd.DataFrame.from_records(all_sim, columns=labels)
all_sim_df.to_csv(ihme_loc_sim_file, index = False)
