"""
Purpose: Setup for DisMod ODE Steps -- Value In Only.

This script will read in the output data_in files from 04a_parallel.do (STATA code) and return
"value_in" files. This step is parallelized from the ODE solver parent screipt by outcome 
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
import sys
import argparse

outcomes =["epilepsy", "long_modsev"]

#arguments from parent 
parser = argparse.ArgumentParser()
parser.add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)")
parser.add_argument("--step_num", help = "step number of this step (i.e. 01a)")
parser.add_argument("--step_name", help = "name of current step (i.e. first_step_name)")
parser.add_argument("--code_dir", help = "code directory")
parser.add_argument("--in_dir", help = "directory for external inputs")
parser.add_argument("--out_dir", help = "directory for this steps checks")
parser.add_argument("--tmp_dir", help = "directory for this steps intermediate draw files")
parser.add_argument("--release", help = "specify GBD release", type=int)
parser.add_argument("--location", help = "specify locations", type=int, nargs = "+")
parser.add_argument("--cause", help = "specify cause")
parser.add_argument("--etiology", help = "specify etiology")
args, unknown = parser.parse_known_args()

print(str(args))
in_dir = args.in_dir
code_dir = args.code_dir
dm_out_dir = args.tmp_dir + args.step_num + "_" + args.step_name + "/"
release = args.release
cause = args.cause
etiology = args.etiology

# read all locations
loc_meta = pd.read_csv(in_dir + "loc_meta.csv")
location_ids = loc_meta['location_id']

#############################################################################################################
#write code here
#############################################################################################################

def read_data(cause, outcome, etiology, loc, year, sex):
	"""Reads in data generated for data_in."""
	df = pd.read_csv(os.path.join(args.tmp_dir, "FILEPATH", str(loc), "{cause}_{etiology}_{outcome}_{loc}_{year}_{sex}.csv".format(
		cause = cause,
		etiology = etiology,
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

def get_values(me_id):
	"""Pulls value-in parameters from the best model version."""
	model_version = db.get_best_model_versions(
	    entity = "modelable_entity",
	    ids = me_id,
	    status = "best",
	    release_id = release
	    )["model_version_id"].iloc[0].astype(str)
	filepath = os.path.join(
	    "FILEPATH",
	    model_version,
	    "FILEPATH")
	
	value = pd.read_csv(filepath)
	cond = value['name'].str.contains("eta")
	value.drop(value[cond].index.values, inplace = True)
	value.loc[value['name'] == "data_like", 'value'] = "log_gaussian"
	
	return value

def main(in_dir, code_dir, dm_out_dir, location_ids, cause, outcome, etiology):
	
	# grab the demographics to loop over when reading in all of the files
	dems = db.get_demographics(gbd_team = "epi", release_id = release)
	year_ids = dems['year_id']
	sex_ids = dems['sex_id']

	# read in all of the files
	data = []
	print(str(location_ids))
	for loc in location_ids:
	    print("Reading {}".format(loc))
	    for year in year_ids: 
	        for sex in sex_ids: 
	            df = read_data(cause, outcome, etiology, loc, year, sex)
	            data.append(df)
	
	# collapse the raw files so that it is just one value
	raw = pd.concat(data) 
	collapsed = collapse(raw) # collapse 

	# pull the MEID for get_values
	dim_dt = pd.read_csv(os.path.join(code_dir, (cause + "FILEPATH")))
	parent_meid_needed = dim_dt[(dim_dt.grouping == "cases") & (dim_dt.healthstate == "_parent") & (dim_dt.acause == cause)]
	me_id = int(parent_meid_needed["modelable_entity_id"])
    
	# format the value file and then append to the collapsed values
	value = get_values(me_id)
	result = collapsed.append(value)
    
	# output the results to value_in directory
	folder = os.path.join(dm_out_dir, "value_in")
	if not os.path.exists(folder):
	    os.makedirs(folder)
    
	filepath = os.path.join(folder, "FILEPATH".format(
		cause = cause,
		outcome = outcome,
		etiology = etiology))
	result.to_csv(filepath, index = False)
	   
	## create a finished.txt for check system 
	finished = []
	checks = os.path.join(dm_out_dir, "FILEPATH")
	if not os.path.exists(checks):
	    os.makedirs(checks)
	np.savetxt(os.path.join(checks,"finished_{}_{}_{}.txt".format(cause, outcome, etiology)), finished)

for outcome in outcomes:
	main(in_dir, code_dir, dm_out_dir, location_ids, cause, outcome, etiology)
