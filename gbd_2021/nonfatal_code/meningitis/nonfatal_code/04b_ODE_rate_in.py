""" Set of functions to make the rate-in file for DisMod ODE Solver.
    USERNAME
"""

#import modules

import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import sys
import argparse

#arguments from parent 
parser = argparse.ArgumentParser()
parser.add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)")
parser.add_argument("--step_num", help = "step number of this step (i.e. 01a)")
parser.add_argument("--step_name", help = "name of current step (i.e. first_step_name)")
parser.add_argument("--code_dir", help = "code directory")
parser.add_argument("--in_dir", help = "directory for external inputs")
parser.add_argument("--out_dir", help = "directory for this steps checks")
parser.add_argument("--tmp_dir", help = "directory for this steps intermediate draw files")
parser.add_argument("--ds", help = "specify decomp step")
parser.add_argument("--gbd_round", help = "specify GBD round", type=int)
parser.add_argument("--location", help = "location", type=int)
args, unknown = parser.parse_known_args()

print(str(args))
in_dir = args.in_dir
dm_out_dir = args.tmp_dir + args.step_num + "_" + args.step_name + "/"
gbd_round = args.gbd_round

#arguments from parameter map
filename = "FILEPATH"

location_id = args.location

outcomes = ["epilepsy", "long_modsev"]

# get demographics
dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round)
year_ids = dems['year_id']
sex_ids = dems['sex_id']

##write code here##

def format_mortality_for_rate_in(mortality, gbd_round):
    """This function formats all-cause mortality to be used for the rate-in files."""
    
    # calculate what we need for age inputs
    ages = pd.read_csv(in_dir + "age_meta.csv")
    ages = ages.rename({"age_group_years_start":"age_lower","age_group_years_end":"age_upper"}, axis='columns')
    mortality = mortality.merge(ages, on = ['age_group_id'])
    mortality.sort_values(by=['age_lower'], inplace=True, ascending=True)
        # set the last age to be 100
    mortality.loc[mortality.age_upper > 100, "age_upper"] = 100
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

def make_rate_in(mortality, location_id, sex_id, year_id, dm_out_dir, in_dir, outcome):
    """Makes rate in file for outcome-location-sex-year combo. Assumes excess mortality w/ Chi. edited for rate file; which contains -inf to inf on all priors exept all cause mortality"""
    
    if outcome == "long_modsev":
        rate_in = pd.read_csv(in_dir + "04a_ODE_solver/rates_long_modsev.csv")
    if outcome == "epilepsy":
        rate_in = pd.read_csv(in_dir + "04a_ODE_solver/rates_epilepsy.csv")

    mortality.drop(['location_id', 'sex_id', 'year_id'], axis = 1, inplace = True)
    rate_in = rate_in.append(mortality)
    
    filename = str(location_id) + "_" + str(year_id) + "_" + str(sex_id) + "_" + outcome + ".csv"
    filepath = os.path.join(dm_out_dir, "rate_in", str(location_id), str(year_id), str(sex_id))
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    rate_in.to_csv(os.path.join(filepath, filename), index = False)
    
def main(in_dir, year_id, sex_id, location_id, dm_out_dir, dems, outcome):

    # grab mortality csv from parent and subset
    mortality = pd.read_csv(in_dir + "/04a_dismod_prep_wmort/mortality_envelope.csv")
    # TEMP - use old mortality filepath - 3/14/22
    # mortality = pd.read_csv("FILEPATH")
    # calculate the standard error
    mortality["std"] = (mortality["upper"] - mortality["lower"])/3.92
    # mortality.drop(['run_id'], axis = 1, inplace = True)
     
    #mortality = mortality['age_group_id' == dems["age_group_ids"], 'location_id' == location_id, 'year_id' == year_id, 'sex_id' == sex_id]
    mortality = mortality.loc[(mortality.age_group_id.isin(dems['age_group_id'])) &
        (mortality.location_id == location_id) & 
        (mortality.year_id == year_id) &
        (mortality.sex_id == sex_id)]
        
    #format mortality to append
    mortality = format_mortality_for_rate_in(mortality, gbd_round)
        
    # grab iso3 for naming and file structure, pull and format all-cause mortality, run make_rate_in to generate loc_year_sex specific files
    make_rate_in(mortality, location_id, sex_id, year_id, dm_out_dir, in_dir, outcome)
    
    ## create a finished.txt for check system 
    finished = []
    checks = dm_out_dir + "/rate_in/checks"
    if not os.path.exists(checks):
        os.makedirs(checks)
    np.savetxt(os.path.join(checks,"finished_{}_{}_{}_{}.txt".format(location_id, sex_id, year_id, outcome)), finished)

for year_id in year_ids: 
    for sex_id in sex_ids: 
        for outcome in outcomes:
            main(in_dir, year_id, sex_id, location_id, dm_out_dir, dems, outcome)