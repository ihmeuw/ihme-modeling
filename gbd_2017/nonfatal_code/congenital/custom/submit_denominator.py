from __future__ import division
import subprocess
import numpy as np
import pandas as pd
import os
import shutil
import glob
from db_queries import get_location_metadata

username = 'USERNAME'
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

out_dir = "FILEPATH"
share_dir = "FILEPATH"

loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=5)
loc_meta = loc_meta.loc[loc_meta.most_detailed==1, ['location_id', 'ihme_loc_id']]


if not os.path.exists(share_dir):
    os.makedirs(share_dir)
else:
    shutil.rmtree(share_dir)
    os.makedirs(share_dir)


job_string = ""
for index, row in loc_meta.iterrows():
    if row.location_id > -1:
        job_name = 'denom_{}'.format(row.location_id)
        job_string = job_string + ',' + job_name
        call = ('qsub -hold_jid {hj} -l mem_free=4.0G -pe multi_slot 4'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' calc_denominator.py'
                    ' {arg1} {arg2} {arg3}'.format(hj='no_holds', 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=share_dir, arg2=row.location_id, 
                        arg3=row.ihme_loc_id))
        subprocess.call(call, shell=True)


hold = job_string
params = [share_dir, out_dir,
         '--loc_list', 
         " ".join([str(x) for x in loc_meta.location_id.tolist()])]

call = ('qsub -hold_jid {hj} -l mem_free=10.0G -pe multi_slot 5'
            ' -cwd -P proj_custom_models'
            ' -o {o}'
            ' -e {e}'
            ' -N {jn}'
            ' cluster_shell.sh'
            ' combine_denominators.py'
            ' {arg1}'.format(hj=hold, o=output_path, e=error_path, 
                jn='combine_denominators', arg1=' '.join(params)))
subprocess.call(call, shell=True)
