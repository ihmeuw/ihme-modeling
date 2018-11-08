
##################################################################################################################
#Author: 
#Date: 9/5/2017
#Purpose: submits code check for rate_in and value_in files and runs ODE solver; gives prevalence draws as output  
##################################################################################################################

################### Manual input #####################
# date must coincide with stata run of 02-04
date = "2018_07_13"
# do you want to activate checks and time log system?
activate_checks = 1
######################################################

# import modules

import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import sys
import time
import shutil

#import demographics, set directories, set variables 

dems = db.get_demographics(gbd_team = "epi")
location_ids = dems['location_id']

#create test variables !! Do not forget to open parallel file and select one year and sex if trying to replicate one file

code_dir = "FILEPATH"
shell =  "FILEPATH"

in_dir = "FILEPATH"
in_dir_data = "FILEPATH" + date + "FILEPATH"
in_dir_value = "FILEPATH" + date + "FILEPATH"
in_dir_rate = "FILEPATH" + date + "FILEPATH"
out_dir_ODE = "FILEPATH" + date + "FILEPATH"

etiologies = ["meningitis_hib", "meningitis_meningo", "meningitis_pneumo", "meningitis_other"]
outcomes = ["epilepsy", "long_modsev"]

##delete / recreate checks folder, log start time
if activate_checks == 1:

    shutil.rmtree(os.path.join(out_dir_ODE, "prev_results", "checks"), ignore_errors = True, onerror = None)

    checks = os.path.join(out_dir_ODE, "prev_results", "checks")
    if not os.path.exists(checks):
        os.makedirs(checks)

    start_time = time.strftime('%X %x %Z')
    start_time = str(start_time)
    np.savetxt(os.path.join(out_dir_ODE, "ODE_solve_start_time.txt"), ["start_time: %s" % start_time], fmt='%s')

#############################################################################################################
#write code here
#############################################################################################################

def submit_qsub_ode_solver(etiology, outcome, location, in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE):
    
    base = "qsub -P proj_sepsis -N ode_solver_{etiology}_{outcome}_{location} -j y -o "FILEPATH" -e "FILEPATH" -pe multi_slot {slots} {shell} {script} ".format(
        etiology = etiology,
        outcome = outcome,
        location = location, 
        slots = 4,
        script = code_dir + "04a_ODE_run_parallel.py",
        shell = shell
        )
    """ Additional Arguments to Pass to Child Script"""
    args = "{etiology} {outcome} {location} {in_dir} {in_dir_data} {in_dir_value} {in_dir_rate} {out_dir_ODE}".format(
        etiology = etiology,
        outcome = outcome,
        location = location, 
        in_dir = in_dir,
        in_dir_data = in_dir_data,
        in_dir_value = in_dir_value,
        in_dir_rate = in_dir_rate,
        out_dir_ODE = out_dir_ODE
        )
    os.popen(base + args)

##check system for value_in and rate_in 

##submit jobs for running ode solver 
for etiology in etiologies:
#for etiology in ["meningitis_pneumo"]:
    for outcome in outcomes:
    #for outcome in ["epilepsy"]:
        for location in location_ids:
        #for location in [501]:
            submit_qsub_ode_solver(etiology, outcome, location, in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE)
            print "{} {} {} submitted".format(etiology, outcome, location) 

#############################################################################################################
#check system
#############################################################################################################

if activate_checks == 0:
    print "finished, did not activate checks"

if activate_checks == 1:
    finished = []
    files_expected = len(etiologies) * len(outcomes) * len(location_ids)
    while files_expected > len(os.listdir(os.path.join(out_dir_ODE, "prev_results/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_ODE, "prev_results/checks")))
        print complete_jobs, "of", files_expected, "completed"
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_ODE, "finished_solving.txt"), finished)

    ##log end time
    end_time = time.strftime('%X %x %Z')
    end_time = str(end_time)
    np.savetxt(os.path.join(out_dir_ODE, "ODE_solve_end_time.txt"), ["end_time: %s" % end_time], fmt='%s')
    print "finally freaking done"


