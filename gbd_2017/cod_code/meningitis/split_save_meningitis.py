
#########################################################################################################
# Author:
# Date: 12/07/2017
# Purpose: split cod models for meningitis 
# Notes: 
version = 'run 6.2'
#########################################################################################################

##import modules
import os
import pandas as pd
from split_models.split_cod import split_cod_model
import datetime
import subprocess
from subprocess import call
from save_results import save_results_cod

#date and shell
date = datetime.datetime.now().strftime ("%Y_%m_%d")
shell =  "FILEPATH"

#split info
parent_cause = 332
target_cause_list = [333, 334, 335, 336]
target_meid_list = [10494, 10495, 10496, 10497]

# create output directory
out_dir = "FILEPATH".format(date)
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# split_cod_model
output_dir = split_cod_model(
    source_cause_id=parent_cause,
    target_cause_ids=target_cause_list,
    target_meids=target_meid_list,
    output_dir=out_dir)

print "split complete, moving on to save_results"

##save results
for cause_id in target_cause_list:
    
    in_dir = "{}/{}".format(out_dir,cause_id)
    file_pat = "death_{location_id}.csv"
    description = "split_cod from {}, SCD data returned".format(version)
    
    slots = 15
    qsub = "qsub -pe multi_slot {slots} -l mem_free={ram}g "\
                    "-P proj_custom_models -N save_results_{cause_id} "\
                    "-o "FILEPATH" {shell} "\
                    "FILEPATH"\
                    "{cause_id} {in_dir} {file_pat} {description}"
    qsub = qsub.format(
            slots=slots, ram=slots*2, cause_id=cause_id, shell=shell, 
            in_dir=in_dir, file_pat=file_pat, description=description)
    print(qsub)
    os.popen(qsub)
    
    #save_results_cod(in_dir, file_pat, cause_id=cause_id, description=description, mark_best=True)
    print "{} saved".format(cause_id)

