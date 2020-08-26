""" Set of functions to make the rate-in file for DisMod ODE Solver.
    9/6/2017
"""

#import modules

import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import sys

#arguments from parent 

input_dir = sys.argv[1]
dm_out_dir = sys.argv[2]
ds = sys.argv[3]

#arguments from parameter map
code_dir = # filepath
filename = "04a_ODE_rate_in_parameter.csv"
param_map = pd.read_csv(os.path.join(code_dir, filename))

task_id = int(os.environ.get("SGE_TASK_ID")) - 1
location_id = param_map.loc[task_id, 'location_id']
sex_id = param_map.loc[task_id, 'sex_id']
year_id = param_map.loc[task_id, 'year_id']
outcome = param_map.loc[task_id, 'outcome']

##write code here##

def get_age_bounds(input_dir):
    """This function includes age midpoint that is already calculated. Easier to just have it as a flat file
    than to calculate it here."""
    ages = pd.read_csv(os.path.join(input_dir, "age_bounds.csv"))
    return ages

def format_mortality_for_rate_in(mortality, input_dir):
    """This function formats all-cause mortality to be used for the rate-in files."""
    
    # calculate what we need for age inputs
    ages = get_age_bounds(input_dir)
    mortality = mortality.merge(ages, on = ['age_group_id'])
    mortality["age"] = (mortality["age_lower"] + mortality["age_upper"]) / 2
    
    # in the function that creates the rate_in file.
    # keep only needed variables
    mortality = mortality[['age', 'age_lower', 'age_upper', 'location_id', 'year_id', 'sex_id', 'mean', 'std', 'upper', 'lower']]
    mortality["mean_std"] = mortality.groupby(['location_id', 'year_id', 'sex_id'])["std"].transform('mean')
    
    for var in ['mean_std', 'std', 'upper', 'lower']:
        mortality[var] = mortality[var].astype(str)
    
    # manipulate ages
    young = mortality.loc[mortality['age_lower'] == 0].copy()
    young["age"] = 0
    young["std"] = "inf"
    young["lower"] = "0"
    young["upper"] = "inf"
    
    older = mortality.loc[mortality['age_upper'] == 100].copy()
    older["age"] = 100
    older["std"] = older['mean_std']
    older["lower"] = "0"
    older["upper"] = "inf"
    
    omega = young.append(mortality).append(older)
    omega["type"] = "omega"
    
    domega = copy.deepcopy(omega)
    domega["type"] = "domega"
    domega = domega.loc[domega['age'] != 100]
    domega["lower"] = "_inf"
    domega["upper"] = "inf"
    domega["mean"] = 0
    domega["std"] = "inf"
    
    mortality = omega.append(domega)
    mortality.drop(['age_lower', 'age_upper', 'mean_std'], axis = 1, inplace = True)
    mortality = mortality[['age', 'lower', 'mean', 'std', 'upper', 'type', 'location_id', 'sex_id', 'year_id']]
    return mortality

def make_rate_in(mortality, location_id, sex_id, year_id, dm_out_dir, input_dir, outcome):
    """Makes rate in file for outcome-location-sex-year combo. Assumes excess mortality w/ Chi. edited for rate file; which contains -inf to inf on all priors exept all cause mortality"""
    
    if outcome == "long_modsev":
        rate_in = pd.read_csv(os.path.join(input_dir, "rates_long_modsev.csv"))
    if outcome == "epilepsy":
        rate_in = pd.read_csv(os.path.join(input_dir, "rates_epilepsy.csv"))

    mortality.drop(['location_id', 'sex_id', 'year_id'], axis = 1, inplace = True)
    rate_in = rate_in.append(mortality)
    
    filename = str(location_id) + "_" + str(year_id) + "_" + str(sex_id) + "_" + outcome + ".csv"
    filepath = os.path.join(dm_out_dir, "04_ODE", "rate_in", str(location_id), str(year_id), str(sex_id))
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    rate_in.to_csv(os.path.join(filepath, filename), index = False)
    
def main(input_dir, year_id, sex_id, location_id, dm_out_dir, location):

    #grab mortality csv from parent and subset
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id = 6)

    mortality = pd.read_csv(os.path.join(dm_out_dir, "02_temp/03_data/all_cause_mortality.csv"))
     
    #mortality = mortality['age_group_id' == dems["age_group_ids"], 'location_id' == location_id, 'year_id' == year_id, 'sex_id' == sex_id]
    mortality = mortality.loc[(mortality.age_group_id.isin(dems['age_group_id'])) &
        (mortality.location_id == location_id) & 
        (mortality.year_id == year_id) &
        (mortality.sex_id == sex_id)]
        
    #format mortality to append
    mortality = format_mortality_for_rate_in(mortality, input_dir)
        
    # grab iso3 for naming and file structure, pull and format all-cause mortality, run make_rate_in to generate loc_year_sex specific files
    make_rate_in(mortality, location_id, sex_id, year_id, dm_out_dir, input_dir, outcome)
    
    ## create a finished.txt for check system 
    finished = []
    checks = os.path.join(dm_out_dir,"04_ODE/rate_in/checks")
    if not os.path.exists(checks):
        os.makedirs(checks)
    np.savetxt(os.path.join(checks,"finished_{}_{}_{}_{}.txt".format(location_id, sex_id, year_id, outcome)), finished)

main(input_dir, year_id, sex_id, location_id, dm_out_dir, location_id)