
##################################################################################################################
#Author: 
#Date: 9/5/2017
#Purpose: submits code check for rate_in and value_in files and runs ODE solver; gives prevalence draws as output  
##################################################################################################################

################### Manual input #####################
# date must coincide with stata run of 02-04
date = "2020_01_27"
# do you want to activate checks and time log system?
activate_checks = 1
# do you want to run the DisMod ODE for epilespy?
run_epilepsy = 1
# do you want to run the DisMod ODE for long_modsev?
run_long_modsev = 1
gbd_round_id = 6
######################################################

#import modules

import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import sys
import time
import shutil 

#import demographics, set directories, set variables 
dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
location_ids = dems['location_id']
sex_ids = dems['sex_id']
year_ids = dems['year_id']

#test dems 
# location_ids = [4911, 4912, 4913, 4914, 4915, 4916, 4917, 4918, 4919, 4921, 4922, 4927, 4928]

code_dir = # filepath
shell =  # filepath

in_dir = # filepath
in_dir_data = # filepath
in_dir_value = # filepath
in_dir_rate = # filepath
out_dir_ODE =  # filepath

if run_epilepsy == 1 and run_long_modsev == 1:
    outcomes = ["epilepsy", "long_modsev"]
elif not run_epilepsy == 1 and run_long_modsev == 1:
    outcomes = ["long_modsev"]
elif run_epilepsy == 1 and not run_long_modsev == 1:
    outcomes = ["epilepsy"]
else:
    outcomes = []

functional = "encephalitis"

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

def submit_qsub_ode_solver(functional, in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE, n_jobs):
    
    base = "qsub -N ode_solver_child -P proj_hiv -t 1:{N} -tc 600 -o /filepath/ -e /filepath/ -l m_mem_free={mem} -l fthread={threads} -l h_rt={time} -q all.q {shell} {script} ".format( 
        N = n_jobs,
        mem = "5G",
        threads = 2,
        time = "01:00:00",
        shell = shell,
        script = code_dir + "04a_ODE_run_parallel.py" 
        )
    """ Additional Arguments to Pass to Child Script"""
    args = "{functional} {in_dir} {in_dir_data} {in_dir_value} {in_dir_rate} {out_dir_ODE}".format(
        functional = functional,
        in_dir = in_dir,
        in_dir_data = in_dir_data,
        in_dir_value = in_dir_value,
        in_dir_rate = in_dir_rate,
        out_dir_ODE = out_dir_ODE
        )
    os.popen(base + args)

##submit jobs for running ode solver
## Create parameter map for array job
df_loc  = pd.DataFrame(data = {'location_id': location_ids, 'key' : 1})
df_out  = pd.DataFrame(data = {'outcome': outcomes, 'key' : 1})

product = pd.merge(df_loc, df_out, on='key')
product.drop(['key'], axis = 1, inplace = True)

filename = "04a_ODE_run_parallel_parameter.csv"
product.to_csv(os.path.join(code_dir, filename), index = False)
n_jobs = len(product.index)

submit_qsub_ode_solver(functional, in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE, n_jobs)
            
#############################################################################################################
#check system
#############################################################################################################

if activate_checks == 0:
    print("Finished, did not activate checks")

if activate_checks == 1:
    finished = []
    files_expected = len(location_ids) * len(outcomes)
    while files_expected > len(os.listdir(os.path.join(out_dir_ODE, "prev_results/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_ODE, "prev_results/checks")))
        print("{jobs} of {total} completed!".format(jobs = complete_jobs, total = files_expected))
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_ODE, "finished_solving.txt"), finished)

    ##log end time
    end_time = time.strftime('%X %x %Z')
    end_time = str(end_time)
    np.savetxt(os.path.join(out_dir_ODE, "ODE_solve_end_time.txt"), ["end_time: %s" % end_time], fmt='%s')
    print("done")

