"""
Submits save cod results in parallel after split_cod_model.
"""
import pandas as pd
import subprocess
import os

# load in the info table and create a cause_id_list
username = 'USERNAME'
work_dir = os.path.dirname(os.path.realpath(__file__))
info_df = pd.read_csv(work_dir + '/info_table.csv')
cause_id_list = info_df['child_causes'].tolist()
main_upload_dir = 'UPLOAD_DIR'.format(username)
# submit compute job for each me_id and form a string of job names
for cause_id in cause_id_list:
    job_name = 'upload_cod_{0}'.format(cause_id)
    upload_dir = '{0}/{1}'.format(main_upload_dir, cause_id)
    best = "True"
    description = 'split from parent cause using split_cod_model'
    call = ('qsub -cwd -P proj_custom_models -N {1}'
            ' -pe multi_slot 40 -l mem_free=80'
            ' SAVE RESULTS SHARED FUNCTION'
            ' {0} -in_dir {2} -d {3}'
            ''.format(cause_id, job_name, upload_dir, description))
    print(call)
    subprocess.call(call, shell=True)

print('fin')
