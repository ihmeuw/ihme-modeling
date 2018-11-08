
###########################################################################################
#Author:  
#Date: 9/5/2017
#Purpose: submits code to generate rate_in and value_in; requires data_in already created 
###########################################################################################

################### Manual input ######################
# date must coincide with stata run of 02-04
date = "2018_07_13"
# do you want to activate checks and time log system?
activate_checks = 1
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
dems = db.get_demographics(gbd_team = "epi")
location_ids = dems['location_id']
sex_ids = dems['sex_id']
year_ids = dems['year_id']

#test dems 
#location_ids = [10, 101]

##create paths
code_dir = "FILEPATH"
shell =  "FILEPATH"
in_dir_rate = "FILEPATH"
out_dir_rate = "FILEPATH" + date + "FILEPATH"
in_dir_value = "FILEPATH" + date + "FILEPATH"
out_dir_value = "FILEPATH"+ date + "FILEPATH"

outcomes = ["epilepsy", "long_modsev"]
functional = "encephalitis"

##delete / recreate checks folder, log start time
if activate_checks == 1:

    shutil.rmtree(os.path.join(out_dir_rate, "FILEPATH"), ignore_errors = True, onerror = None)
    shutil.rmtree(os.path.join(out_dir_value, "FILEPATH"), ignore_errors = True, onerror = None)

    checks_rate = os.path.join(out_dir_value, "FILEPATH")
    if not os.path.exists(checks_rate):
        os.makedirs(checks_rate)

    checks_value = os.path.join(out_dir_value, "FILEPATH")
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
    dems = db.get_demographics(gbd_team = "epi") 
    mortality = db.get_envelope(age_group_id = dems["age_group_id"],
        location_id = dems["location_id"], year_id = dems["year_id"], sex_id = dems["sex_id"], with_hiv = 1, rates = 1)
    # calculate the standard error
    mortality["std"] = (mortality["upper"] - mortality["lower"])/3.92
    mortality.drop(['run_id'], axis = 1, inplace = True)
    filename = "all_cause_mortality.csv"
    mortality.to_csv(os.path.join(out_dir_rate, "02_temp/03_data", filename), index = False)

mortality = get_all_cause_mortality()

def submit_qsub_rate(loc, sex, year, in_dir_rate):
    
    base = "qsub -P proj_custom_models -N rate_in_{loc}_{year}_{sex} -j y -o "FILEPATH" -e "FILEPATH" -pe multi_slot {slots} {shell} {script} ".format(
        loc = loc,
        year = year,
        sex = sex,
        slots = 4,
        script = code_dir + "04a_ODE_rate_in_parallel.py",
        shell = shell,
        )
    """ Additional Arguments to Pass to Child Script"""
    args = "{loc} {sex} {year} {in_dir} {out_dir}".format(
        loc = loc,
        sex = sex,
        year = year,
        in_dir = in_dir_rate,
        out_dir = out_dir_rate,
        )
    os.popen(base + args)

def submit_qsub_value(functional, outcome, in_dir_value, out_dir_value):
    
    base = "qsub -P proj_custom_models -N value_in_{functional}_{outcome} -j y -o "FILEPATH" -e "FILEPATH" -pe multi_slot {slots} {shell} {script} ".format(
        functional = functional,
        outcome = outcome,
        slots = 4,
        script = code_dir + "04a_ODE_value_in_parallel.py",
        shell = shell,
        )
    """ Additional Arguments to Pass to Child Script"""
    args = "{functional} {outcome} {in_dir} {out_dir}".format(
        functional = functional,
        outcome = outcome,
        in_dir = in_dir_value,
        out_dir = out_dir_value,
        )
    os.popen(base + args)


##launch code

## submit jobs for rate_in    
for loc in location_ids:
    for sex in sex_ids:
        for year in year_ids:
            submit_qsub_rate(loc, sex, year, in_dir_rate)

##submit jobs for value_in
    for outcome in outcomes: 
        submit_qsub_value(functional, outcome, in_dir_value, out_dir_value)

#############################################################################################################
#check system
#############################################################################################################

if activate_checks == 0:
    print "finished, did not activate checks"

if activate_checks == 1:
    finished = []
    files_expected = len(outcomes)
    while files_expected > len(os.listdir(os.path.join(out_dir_value, "FILEPATH"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_value, "FILEPATH")))
        print complete_jobs, "of", files_expected, "completed", ", you just have to make it to practicum, you got this"
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_value_in.txt"), finished)

    finished = []
    files_expected = len(year_ids) * len(sex_ids) * len(location_ids)
    while files_expected > len(os.listdir(os.path.join(out_dir_rate, "FILEPATH"))):
        complete_jobs = len(os.listdir(os.path.join(out_dir_rate, "FILEPATH")))
        print complete_jobs, "of", files_expected, "completed", ", !"
        time.sleep(30)
    np.savetxt(os.path.join(out_dir_value, "finished_rate_in.txt"), finished)

    ##log end time
    end_time = time.strftime('%X %x %Z')
    end_time = str(end_time)
    np.savetxt(os.path.join(out_dir_value, "ODE_prep_end_time.txt"), ["end_time: %s" % end_time], fmt='%s')
    print "sigh"


