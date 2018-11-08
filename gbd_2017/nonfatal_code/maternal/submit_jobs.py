import subprocess
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
import time
from db_queries import get_best_model_versions
import getpass

# set up stdout and stderr
username = getpass.getuser()
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

# pull in dependency map
dep_map = pd.read_csv('{}/dependency_map.csv'.format(os.getcwd()))

# create new dataframe that converts all values in the dep_map to ints and 
# matches each output_me with each of its input_mes
columns = ['input_me','output_me']
extended_map = pd.DataFrame(columns=columns)
for index, row in dep_map.iterrows():
    ins = [int(x) for x in row.input_me.split(';')]
    outs = [int(x) for x in row.output_mes.split(';')]
    df = pd.DataFrame(columns=columns, 
        data=zip(np.repeat(ins, len(outs)), outs*len(ins))) 
    extended_map = extended_map.append(df, ignore_index=True)

# make timestamped output folders
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)

base_dir = "FILEPATH"
for output_me in extended_map.output_me.unique():
    out_dir = (os.path.join(base_dir, str(output_me)))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

# grab version ids for all input models
model_version_list = extended_map.input_me.unique()
mvid_df = get_best_model_versions(entity='modelable_entity',
    ids=model_version_list, gbd_round_id=5)

###########################################################################
# Zero out fistula locations and save result
###########################################################################

zero_fime = 1552
zero_fome = 16535
yearvals = [1990, 1995, 2000, 2005, 2010, 2017]

job_list = []
job_string = ''
for year in yearvals:
    class_name = "Zero_Fistula"
    input_me = zero_fime
    output_me = zero_fome
    job_name = "{cn}_{y}".format(cn=class_name, y=year)
    job_string = job_string + ',' + job_name
    call = ('qsub -cwd -P proj_neonatal '
                    '-o {o} '
                    '-e {e} '
                    '-pe multi_slot 5 '
                    '-N {jn} '
                    'python_shell.sh '
                    'maternal_core.py '
                    '"{arg1}" "{arg2}" "{arg3}" "{arg4}" "{arg5}"'.format(
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=class_name, arg2=base_dir, arg3=year, 
                        arg4=input_me, arg5=output_me))
    subprocess.call(call, shell=True)

# Save zero'd out fistula locations
hold = job_string
model_version_ids = str(mvid_df.loc[mvid_df.modelable_entity_id==zero_fime, 'model_version_id'].item())
save_job_name = "save_{meid}".format(meid=zero_fome)
out_dir = "FILEPATH"
call = ('qsub -hold_jid {hj} -cwd -P proj_neonatal '
                '-o {o} '
                '-e {e} '
                '-N {jn} '
                '-pe multi_slot 15 '
                'python_shell.sh '
                'save.py '
                '{arg1} \'{arg2}\' {arg3}'.format(hj=hold, 
                    o=output_path, e=error_path, jn=save_job_name, 
                    arg1=zero_fome, arg2=model_version_ids, arg3=out_dir))
subprocess.call(call, shell=True)

###########################################################################
# adjust maternal
###########################################################################
# Run Adjustments
dep_map = dep_map.loc[dep_map.class_name != 'Zero_Fistula',:]
yearvals = [1990, 1995, 2000, 2005, 2010, 2017]
adjust_job_string = ''
for index, row in dep_map.iterrows():
    for year in yearvals:
        class_name = row['class_name']
        input_me = row['input_me']
        output_me = row['output_mes']
        job_name = 'adjust_{cn}_{y}'.format(cn=class_name, y=year)
        if class_name == "Fistula":
            hold = save_job_name
        else:
            hold = "no_holds"

        if (class_name != "this line is to facilitate testing one class at a time, change as needed"):
            adjust_job_string = adjust_job_string + ',' + job_name
            call = ('qsub -hold_jid {hj} -cwd -P proj_neonatal '
                    '-o {o} '
                    '-e {e} '
                    '-pe multi_slot 6 '
                    '-N {jn} python_shell.sh maternal_core.py '
                    '{arg1} {arg2} {arg3} "{arg4}" "{arg5}"'.format(hj=hold, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=class_name, arg2=base_dir, arg3=year, 
                        arg4=input_me, arg5=output_me))
            subprocess.call(call, shell=True)

###########################################################################
# upload epi data
###########################################################################
hold = adjust_job_string
dismod_mes = [2624, 2627, 1546]
acause_bundle_list = [('maternal_sepsis', 377), ('maternal_htn', 827),
                    ('maternal_htn', 829)]
me_bundle_pairs = zip(dismod_mes, acause_bundle_list)
for me, bundle_tuple in me_bundle_pairs:
    # this line is to facilitate testing one bundle at a time, 
    # change to == as needed
    if (int(bundle_tuple[1]) > -1):
        data_dir = os.path.join(base_dir, str(me))
        call = ('qsub -hold_jid {hj} -cwd -P proj_neonatal '
                    '-o {o} '
                    '-e {e} '
                    '-pe multi_slot 15 '
                    '-N upload_bundle_{jn} '
                    'python_shell.sh '
                    'upload_maternal_epi_bundles.py '
                    '{arg1} {arg2} {arg3}'.format(hj=hold, 
                        o=output_path, e=error_path, jn=bundle_tuple[1], 
                        arg1=data_dir, arg2=bundle_tuple[0], 
                        arg3=bundle_tuple[1]))
        subprocess.call(call, shell=True)

###########################################################################
# save
###########################################################################
hold = adjust_job_string
# Save Results in parallel for everything except dismod mes
remove_mes = [2624, 2627, 1546, 16535]
output_mes = [x for x in extended_map.output_me.unique() if x not in remove_mes]
for me in output_mes:
    data_dir = os.path.join(base_dir, str(me))
    # this line is to facilitate testing one me at a time, 
    # change to == as needed
    if me > -1:
        model_ids_str_for_save = ''
        df = extended_map.loc[extended_map.output_me==me,:]
        for index,row in df.iterrows():
            model_ids_str_for_save += ' meid {}, mvid {};'.format(row.input_me, 
                mvid_df.loc[mvid_df.modelable_entity_id==row.input_me,'model_version_id'].item())
        model_ids_str_for_save = model_ids_str_for_save[:-1] + "."
        call = ('qsub -hold_jid {hj} -cwd -P proj_neonatal '
                '-o {o} '
                '-e {e} '
                '-N save_{jn} '
                '-pe multi_slot 15 '
                'python_shell.sh '
                'save.py '
                '{arg1} \'{arg2}\' {arg3}'.format(hj=hold, o=output_path, 
                    e=error_path, jn=me, arg1=me, arg2=model_ids_str_for_save, 
                    arg3=data_dir))
        subprocess.call(call, shell=True)

