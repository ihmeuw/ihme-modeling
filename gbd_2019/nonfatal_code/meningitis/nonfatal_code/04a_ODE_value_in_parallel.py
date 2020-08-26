"""
Date: 9/5/2017
Purpose: Setup for DisMod ODE Steps -- Value In Only.

This script will read in the output data_in files from 04a_parallel.do (STATA code) and return
"value_in" files. This step is parallelized from the ODE solver parent script by outcome (2 jobs)
"""

# SETUP --------------------------------------------------------------

# import packages
import pandas as pd
import numpy as np
import itertools
import os
import db_queries as db
import time
import sys
import copy

#import args from parent 
outcome = sys.argv[1]
in_dir = sys.argv[2]
out_dir = sys.argv[3]
ds = sys.argv[4]

print("outcome:", outcome, "in_dir:", in_dir, "out_dir:", out_dir, "ds:", ds)

#############################################################################################################
#write code here
#############################################################################################################

def read_data(outcome, loc, year, sex):
	"""Reads in data generated for data_in."""
	df = pd.read_csv(os.path.join(in_dir, str(loc), "{outcome}_{loc}_{year}_{sex}.csv".format(
		outcome = outcome, 
		loc = loc,
		year = year, 
		sex = sex)))
	return df

def collapse(df):
    """Collapses data over each integrand - incidence and excess mortality (mtexcess)."""
    df = df[['integrand', 'meas_value']]
    df = df.loc[df['meas_value'] > 0]
    df = df.groupby('integrand').median().reset_index()
    # we want eta to be 1% of the non-zero median of the integrand values
    df['meas_value'] = df['meas_value'] * 0.001
    df['name'] = "eta_" + df['integrand']
    df.drop('integrand', inplace = True, axis = 1)
    df.rename(columns = {'meas_value': 'value'}, inplace = True)
    return df

def get_values():
	"""Pulls value-in parameters from the best model version."""
	me_id = 1296
	model_version = db.get_best_model_versions(
	    entity = "modelable_entity",
	    ids = me_id,
	    status = "best", 
	    gbd_round_id = 6,
	    decomp_step = ds)["model_version_id"].iloc[0].astype(str)
	filepath = os.path.join(
	    "filepath",
	    model_version,
	    "full/value.csv")
	
	value = pd.read_csv(filepath)
	cond = value['name'].str.contains("eta")
	value.drop(value[cond].index.values, inplace = True)
	value.loc[value['name'] == "data_like", 'value'] = "log_gaussian"
	
	return value

def main(in_dir, out_dir):
	
	# grab the demographics to loop over when reading in all of the files
	dems = db.get_demographics(gbd_team = "epi", gbd_round_id = 6)
	location_ids = dems['location_id']
	# test location
	# location_ids = [44651]
	
	# read in all of the files
	data = []
	for loc in location_ids:
	    print("Reading {}".format(loc))
	    for year in dems['year_id']: 
	        for sex in dems['sex_id']: 
	            df = read_data(outcome, loc, year, sex)
	            data.append(df)
	
	# collapse the raw files so that it is just one value
	raw = pd.concat(data) # puts all of the little dfs into one big df
	collapsed = collapse(raw) # collapse it because we aren't interested in loc/year/sex
    
	# format the value file and then append to the collapsed values
	value = get_values()
	result = collapsed.append(value)
    
	# output the results to value_in directory
	folder = os.path.join(out_dir, "value_in")
	if not os.path.exists(folder):
	    os.makedirs(folder)
    
	filepath = os.path.join(folder, "value_in_{outcome}.csv".format(outcome = outcome))
	result.to_csv(filepath, index = False)
	   
	## create a finished.txt for check system 
	finished = []
	checks = os.path.join(out_dir, "value_in/checks")
	if not os.path.exists(checks):
	    os.makedirs(checks)
	np.savetxt(os.path.join(checks,"finished_{}.txt".format(outcome)), finished)

#run main function
main(in_dir, out_dir)