"""
Date: 9/5/2017
Purpose: submits code to generate rate_in and value_in; requires data_in already created 

"""

################### Manual input ######################
# date must coincide with R code run of 02-04
date = "2020_01_26"
# do you want to activate checks and time log system?
activate_checks = 1
# specify decomp_step
ds = "step4"
# specify gbd_round
gbd_round = 6
# do you want to run the DisMod ODE for epilepsy?
run_epilepsy = True

# do you want to run the DisMod ODE for epilepsy?
run_long_modsev = True
#######################################################

#set modules
import db_queries as db
import pandas as pd
import numpy as np
import os
import copy
import time
import shutil

#import demographics
dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round)
location_ids = dems['location_id']
sex_ids = dems['sex_id']
year_ids = dems['year_id']

# test one location
# location_ids = [44651]

##create variables
code_dir = # filepath
shell =  code_dir +"python_shell.sh"
in_dir_rate = # filepath
out_dir_rate = # filepath
in_dir_value = # filepath
out_dir_value = # filepath

if run_epilepsy and run_long_modsev:
    outcomes = ["epilepsy", "long_modsev"]
elif run_epilepsy and not run_long_modsev:
    outcomes = ["epilepsy"]
elif not run_epilepsy and run_long_modsev:
    outcomes = ["long_modsev"]
else:
    outcomes = []

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
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round)
    mortality = db.get_envelope(age_group_id = dems["age_group_id"], gbd_round_id = gbd_round,
        location_id = dems["location_id"], year_id = dems["year_id"], sex_id = dems["sex_id"], with_hiv = 1, rates = 1, decomp_step = ds)
    # calculate the standard error
    mortality["std"] = (mortality["upper"] - mortality["lower"])/3.92
    mortality.drop(['run_id'], axis = 1, inplace = True) #added this line as test
    
    filename = "all_cause_mortality.csv"
    mortality.to_csv(os.path.join(out_dir_rate, "02_temp/03_data", filename), index = False)

mortality = get_all_cause_mortality()

def submit_qsub_rate(in_dir_rate, out_dir_rate, ds, n_jobs):
    base = "qsub -P proj_hiv -N rate_in_child -t 1:{N} -o /filepath/ -e /filepath/ -l m_mem_free={mem} -l fthread={threads} -l h_rt={time} -q all.q {shell} {script} ".format(
        N = n_jobs,
        mem = "1G",
        threads = 1,
        time = "00:05:00",
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

def submit_qsub_value(outcome, in_dir_value, out_dir_value):
    base = "qsub -P proj_hiv -N value_in_{outcome} -o /filepath/ -e /filepath/ -l m_mem_free={mem} -l fthread={threads} -l h_rt={time} -q all.q {shell} {script} ".format(
        outcome = outcome,
        mem = "1G",
        threads = 1,
        time = "00:30:00",
        shell = shell,
        script = code_dir + "04a_ODE_value_in_parallel.py"
    )

    """ Additional Arguments to Pass to Child Script"""
    args = "{outcome} {in_dir} {out_dir} {ds}".format(
        outcome = outcome,
        in_dir = in_dir_value,
        out_dir = out_dir_value,
        ds = ds
    )
    print(base + args)
    os.popen(base + args)


##launch code
## submit jobs for rate_in
## Create parameter map for array job
df_loc     = pd.DataFrame(data = {'location_id': location_ids, 'key' : 1})
df_sex     = pd.DataFrame(data = {'sex_id' : sex_ids, 'key' : 1})
df_year    = pd.DataFrame(data = {'year_id' : year_ids, 'key' : 1})
df_outcome = pd.DataFrame(data = {'outcome' : outcomes, 'key' : 1})

product = pd.merge(df_loc, df_sex, on='key')
product = pd.merge(product, df_year, on='key')
product = pd.merge(product, df_outcome, on='key')
product.drop(['key'], axis = 1, inplace = True)

filename = "04a_ODE_rate_in_parameter.csv"
product.to_csv(os.path.join(code_dir, filename), index = False)

n_jobs = len(product.index)
submit_qsub_rate(in_dir_rate, out_dir_rate, ds, n_jobs)

##submit jobs for value_in
for outcome in outcomes: 
    submit_qsub_value(outcome, in_dir_value, out_dir_value)

#############################################################################################################
#check system
#############################################################################################################

if activate_checks == 0:
    print("finished, did not activate checks")

if activate_checks == 1:
    finished = []
    files_expected = len(year_ids) * len(sex_ids) * len(location_ids)
    while files_expected > len(os.listdir(os.path.join(out_dir_rate, "04_ODE/rate_in/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_rate, "04_ODE/rate_in/checks")))
        print(complete_jobs, "of", files_expected, "completed!")
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_rate_in.txt"), finished)

    finished = []
    files_expected = len(outcomes)
    while files_expected > len(os.listdir(os.path.join(out_dir_value, "value_in/checks"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_value, "value_in/checks")))
        print(complete_jobs, "of", files_expected, "completed")
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_value_in.txt"), finished)

    ##log end time
    end_time = time.strftime('%X %x %Z')
    end_time = str(end_time)
    np.savetxt(os.path.join(out_dir_value, "ODE_prep_end_time.txt"), ["end_time: %s" % end_time], fmt='%s')
    print("Done")
