'''
This part of the code sets up directories to store the draws retrieved from 
the epi database and executes the gyne_core.py and gyne_save.py code
'''

import pandas as pd
import os
import shutil
import json
import re
import subprocess
from db_queries import get_best_model_versions, get_demographics
from datetime import datetime


#TODO: Change the username and/or root path so that the person running the code has write privileges to the output directories
username = "USERNAME"
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)


# make timestamped output folder
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)
out_dir = "FILEPATH"
# run code

epi_demographics = get_demographics("epi", gbd_round_id=5)
locations = epi_demographics['location_id']

me_map = {
    "uterine_fibroids": {
        "srcs": {
            "symp": "2064",
        },
        "trgs": {
            "asymp": "3121"
        }
    },
    "pms": {
        "srcs": {
            "tot": "2079",
            "prop": "2080"
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
# core
###########################################################################

job_string = ''
for loc in locations:
    job_name = 'gyne_{l}'.format(l=loc)
    job_string = job_string + ',' + job_name
    call = ('qsub -hold_jid {hj} -l mem_free=4.0G -pe multi_slot 2'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' python_shell.sh'
                ' gyne_core.py'
                ' \'{arg1}\' {arg2} {arg3}'.format(hj='no_holds', 
                    o=output_path, 
                    e=error_path, 
                    jn=job_name, 
                    arg1=json.dumps(me_map), 
                    arg2=loc, 
                    arg3=out_dir))
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
        ids=model_version_list, gbd_round_id=5)
    description = ''
    for index, row in df.iterrows():
        description += "Used mvid {} for modelable entity {}.".format(row.model_version_id,row.modelable_entity_id)
    
    # save targets with source mvid info in description
    outputs = v.get("trgs",{})
    for trg_key, save_id in outputs.items():
        job_name = "save_{svid}".format(svid=save_id)
        call = ('qsub  -hold_jid {hj} -l mem_free=20.0G -pe multi_slot 10'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' gyne_save.py'
                    ' {arg1} {arg2} \'{arg3}\''.format(hj=job_string, 
                        o=output_path, 
                        e=error_path, 
                        jn=job_name, 
                        arg1=save_id, 
                        arg2=out_dir, 
                        arg3=description))

        subprocess.call(call, shell=True)
