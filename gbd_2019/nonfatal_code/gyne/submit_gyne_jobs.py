'''
This part of the code sets up directories to store the draws retrieved from 
the epi database and executes the congenital core and save_custom_results code
'''

import pandas as pd
import os
import shutil
import json
import re
import subprocess
import time
from db_queries import get_best_model_versions, get_demographics
from datetime import datetime
import gbd.constants
import getpass

running_save = False
username = getpass.getuser()
decomp_step = 'step4'
root = os.path.join('FILEPATH', username)
error_path = os.path.join(root, 'errors')
output_path = os.path.join(root, 'output')

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)


# make timestamped output folder
date_unformatted = time.strftime('%m_%d_%Y')
out_dir = os.path.join(root, 'gyne/{}'.format(date_unformatted))

#out_dir = os.path.join(root, 'gyne', '03_01_2019')

# run code
epi_demographics = get_demographics("epi", 
    gbd_round_id=gbd.constants.GBD_ROUND_ID)
locations = epi_demographics['location_id']

#TODO: Take this out and put it in a separate file
me_map = {
     "uterine_fibroids": {
         "srcs": {
             "total": "2064",
         },
         "trgs": {
             "symp": "24396",
             "asymp": "3121"
         }
     },
    "pms": {
        "srcs": {
            "tot": "2079"
        },
        "trgs": {
            "adj": "3133"
        }
    }
}

###########################################################################
# subdirectories
###########################################################################

if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# add me_id output directories
save_ids = []
for _, v in me_map.items():
    outputs = v.get('trgs',{})
    for trg_key, me_id in outputs.items():
        if not os.path.exists(os.path.join(out_dir, str(me_id))):
            os.makedirs(os.path.join(out_dir, str(me_id)))
        save_ids.append(me_id)

###########################################################################
# core - just calc_asymp_uf and adjust_pms for right now
###########################################################################

job_string = ''
for loc in locations:
    hold = 'no_holds'
    job_name = 'gyne_{l}'.format(l=loc)
    job_string = job_string + ',' + job_name
    call = ('qsub -hold_jid {hj} -l fthread=2 -l m_mem_free=5G'
                ' -q all.q '
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' python_shell.sh'
                ' gyne_core.py'
                ' \'{arg1}\' {arg2} {arg3} {arg4}'.format(hj=hold, 
                    o=output_path, e=error_path, jn=job_name, 
                    arg1=json.dumps(me_map), arg2=loc, arg3=out_dir, 
                    arg4=decomp_step))
    subprocess.call(call, shell=True)

###########################################################################
# save
###########################################################################
# get version ids of models used in calculations
# upload this information in the save description

for mapper_key, v in me_map.items():
    # get mvid info for sources and add to description
    inputs = v.get("srcs",{})
    model_version_list = []
    for src_key, me_id in inputs.items():
        model_version_list.append(int(me_id))

    df = get_best_model_versions(entity='modelable_entity', 
                                ids=model_version_list, 
                                gbd_round_id=gbd.constants.GBD_ROUND_ID, 
                                decomp_step = decomp_step)
    description = ''
    for index, row in df.iterrows():
        description += "Used mvid {} for modelable entity {}.".format(
            row.model_version_id,row.modelable_entity_id)
    
    # save targets with source mvid info in description
    outputs = v.get("trgs",{})
    for trg_key, save_id in outputs.items():
        job_string= job
        job_name = "save_{svid}".format(svid=save_id)
        call = ('qsub  -hold_jid {hj} -l fthread -l m_mem_free'
                    ' -q
                    ' -cwd -P proj'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' gyne_save.py'
                    ' {arg1} {arg2} {arg3} \'{arg4}\''.format(hj=job_string,
                        o=output_path, e=error_path, jn=job_name, arg1=save_id,
                        arg2=out_dir, arg3=decomp_step, arg4=description))
        subprocess.call(call, shell=True)
