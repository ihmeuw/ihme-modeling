
###########################################################################################
#Author: 
#Date: 9/5/2017
#Purpose: submits code to generate rate_in and value_in; requires data_in already created 
###########################################################################################

################### Manual input ######################
# date must coincide with run of 02-04
date = "2020_01_27"
# do you want to activate checks and time log system?
activate_checks = 1
# do you want to prep DisMod ODE files for epilespy?
run_epilepsy = 1
# do you want to prep DisMod ODE files for long_modsev?
run_long_modsev = 1
# specify decomp step
ds = "step4"
gbd_round_id = 6
#######################################################

#set packages
import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import shutil 
import time

#import demographics
dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
location_ids = dems['location_id']

sex_ids = dems['sex_id']
year_ids = dems['year_id']

# test dems 
# location_ids = [44735]

##create paths
code_dir = # filepath
shell =  # filepath
in_dir_rate = # filepath
out_dir_rate = # filepath
in_dir_value = # filepath
out_dir_value = # filepath

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

    shutil.rmtree(os.path.join(out_dir_rate, "04_ODE/rate_in/checks"), ignore_errors = True, onerror = None)
    shutil.rmtree(os.path.join(out_dir_value, "value_in/checks"), ignore_errors = True, onerror = None)

    checks_rate = os.path.join(out_dir_value, "rate_in/checks")
    if not os.path.exists(checks_rate):
        os.makedirs(checks_rate)

    checks_value = os.path.join(out_dir_value, "value_in/checks")
    if not os.path.exists(checks_value):
        os.makedirs(checks_value)

    start_time = time.strftime('%X %x %Z')
    start_time = str(start_time)
    np.savetxt(os.path.join(out_dir_value, "ODE_prep_start_time.txt"), ["start_time: %s" % start_time], fmt='%s')

##############################################################################
## write code here
##############################################################################

#grab all-cause mortality envelope and save as a temp file for child rate_in to pull 
def get_all_cause_mortality():
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id = 6) 
    mortality = db.get_envelope(age_group_id = dems["age_group_id"],
        location_id = dems["location_id"], year_id = dems["year_id"], sex_id = dems["sex_id"], with_hiv = 1, rates = 1, decomp_step = ds, gbd_round_id = gbd_round_id)
    # calculate the standard error
    mortality["std"] = (mortality["upper"] - mortality["lower"])/3.92
    mortality.drop(['run_id'], axis = 1, inplace = True)
    filename = "all_cause_mortality.csv"
    mortality.to_csv(os.path.join(out_dir_rate, "02_temp/03_data", filename), index = False)

mortality = get_all_cause_mortality()

def submit_qsub_rate(in_dir_rate, out_dir_rate, ds, n_jobs):
    
    base = "qsub -P proj_hiv -N rate_in_child -t 1:{N} -tc 600 -o /filepath/ -e /filepath/ -l archive -l m_mem_free={mem} -l fthread={threads} -l h_rt={time} -q all.q {shell} {script} ".format(
        N = n_jobs,
        mem = "2G",
        threads = 1,
        time = "01:00:00",
        shell = shell,
        script = code_dir + "04a_ODE_rate_in_parallel.py"
        )

    """ Additional Arguments to Pass to Child Script"""
    args = "{in_dir} {out_dir} {ds}".format(
        in_dir = in_dir_rate,
        out_dir = out_dir_rate,
        ds = ds
        )
    #print base + args
    os.popen(base + args)

def submit_qsub_value(functional, outcome, in_dir_value, out_dir_value):
    
    base = "qsub -P proj_hiv -N value_in_{functional}_{outcome} -j y -o /filepath/ -e /filepath/ -l archive -l m_mem_free={mem} -l fthread={threads} -l h_rt={time} -q all.q {shell} {script} ".format(
        functional = functional,
        outcome = outcome,
        mem = "10G",
        threads = 1,
        time = "01:00:00",
        shell = shell,
        script = code_dir + "04a_ODE_value_in_parallel.py"
        )
    """ Additional Arguments to Pass to Child Script"""
    args = "{functional} {outcome} {in_dir} {out_dir} {ds}".format(
        functional = functional,
        outcome = outcome,
        in_dir = in_dir_value,
        out_dir = out_dir_value,
        ds = ds
        )
    os.popen(base + args)


##launch code

## submit jobs for rate_in    
## Create parameter map for array job
df_loc  = pd.DataFrame(data = {'location_id': location_ids, 'key' : 1})
df_sex  = pd.DataFrame(data = {'sex_id': sex_ids, 'key' : 1})
df_year = pd.DataFrame(data = {'year_id': year_ids, 'key' : 1})
df_outcome = pd.DataFrame(data = {'outcome': outcomes, 'key' : 1})

product = pd.merge(df_loc, df_sex, on='key')
product = pd.merge(product, df_year, on='key')
product = pd.merge(product, df_outcome, on='key')

product.drop(['key'], axis = 1, inplace = True)

filename = "04a_ODE_rate_in_parameter.csv"
product.to_csv(os.path.join(code_dir, filename), index = False)

n_jobs = len(product.index)
submit_qsub_rate(in_dir_rate, out_dir_rate, ds, n_jobs)

# submit jobs for value_in
for outcome in outcomes: 
    submit_qsub_value(functional, outcome, in_dir_value, out_dir_value)

#############################################################################################################
#check system
#############################################################################################################

if activate_checks == 0:
    print("finished, did not activate checks")

if activate_checks == 1:
    finished = []
    files_expected = len(year_ids) * len(sex_ids) * len(location_ids) * len(outcomes)
    while files_expected > len(os.listdir(os.path.join(out_dir_rate, "04_ODE/rate_in/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_rate, "04_ODE/rate_in/checks")))
        print("{jobs} of {total} completed!".format(jobs = complete_jobs, total = files_expected))
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_rate_in.txt"), finished)

    finished = []
    files_expected = len(outcomes)
    while files_expected > len(os.listdir(os.path.join(out_dir_value, "value_in/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_value, "value_in/checks")))
        print("{jobs} of {total} completed! you got this!".format(jobs = complete_jobs, total = files_expected))
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_value_in.txt"), finished)
    ##log end time
    end_time = time.strftime('%X %x %Z')
    end_time = str(end_time)
    np.savetxt(os.path.join(out_dir_value, "ODE_prep_end_time.txt"), ["end_time: %s" % end_time], fmt='%s')
    print("sigh")
