
#########################################################################################################
# Author: AUTHOR
# Date: 12/07/2017
# Purpose: split cod models for cirrhosis using dismod etiology proportion models
# Notes: 
version = 'run_6.2'
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

parent_cause = 99999
target_cause_list = 99999
target_meid_list = 99999

out_dir = "FILEPATH".format(date)
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# split_cod_model
output_dir = split_cod_model(
    source_cause_id=parent_cause,
    target_cause_ids=target_cause_list,
    target_meids=target_meid_list,
    output_dir=out_dir)

print "split complete, beginning save_results"

##save results
for cause_id in target_cause_list:
    
    in_dir = "{}/{}".format(out_dir,cause_id)
    file_pat = "death_{location_id}.csv"
    description = "split_cod_from {} SCD data returned".format(version)
    
    #qsub meningitis script, this is not a mistake its just a generic save results 
    slots = 15
    qsub = "qsub -pe multi_slot {slots} -l mem_free={ram}g "\
                    "-P proj_custom_models -N save_results_{cause_id} "\
                    "-o FILEPATH {shell} "\
                    "FILEPATH "\
                    "{cause_id} {in_dir} {file_pat} {description}"
    qsub = qsub.format(
            slots=slots, ram=slots*2, cause_id=cause_id, shell=shell, 
            in_dir=in_dir, file_pat=file_pat, description=description)
    print(qsub)
    os.popen(qsub)
    
    #save_results_cod(in_dir, file_pat, cause_id=cause_id, description=description, mark_best=True)
    print "{} submitted save_results".format(cause_id)
    