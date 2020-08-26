import subprocess
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
import time
from db_queries import get_best_model_versions
import getpass
import time
import gbd.constants as gbd

### TODO: Add a way to be able to submit Zero_Fistula and Fistula at the same
###       time, as currently the job would fail as the me_id for Fistula would
###       not have a model version id returned by get_model_versions, which is
###       used for the adjustments job name. Current workaround is to run
###       Zero_Fistula first (with zero=True) and once that all is done
###       then run Fistula (with zero=False).
################################################################################
# Please read the readme.txt in this repo. It will explain the whole strategy
################################################################################

# set round id, years to execute, and boolean for fistula
gbdrid = gbd.GBD_ROUND_ID
yearvals = gbd.ESTIMATION_YEARS
zero = False
decomp_step = 'step2'

# set up stdout and stderr
username = getpass.getuser()
root = os.path.join('FILEPATH', username)
error_path = os.path.join(root, 'errors')
output_path = os.path.join(root, 'output')

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

# pull in dependency map
dep_map = pd.read_csv('{}/dependency_map_save.csv'.format(os.getcwd()))

""" create new dataframe that converts all values in the dep_map to ints and 
matches each output_me with each of its input_mes """
columns = ['input_me','output_me']
extended_map = pd.DataFrame(columns=columns)
for index, row in dep_map.iterrows():
    ins = [int(x) for x in str(row.input_me).split(';')]
    outs = [int(x) for x in str(row.output_mes).split(';')]
    df = pd.DataFrame(columns=columns, data=list(zip(np.repeat(ins, len(outs)), 
        outs*len(ins)))) 
    extended_map = extended_map.append(df, ignore_index=True)
        
# make timestamped output folders
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)

base_dir = os.path.join(root, 'nonfatal_maternal', '{}'.format(c_date))

for output_me in extended_map.output_me.unique():
    out_dir = (os.path.join(base_dir, str(output_me)))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

""" grab version ids for all input models. This strategy assumes that no new 
dismod source models have been marked best since the start of the LBA 
code. """
model_version_list = extended_map.input_me.unique()
mvid_df = get_best_model_versions(entity='modelable_entity',
    ids=model_version_list, gbd_round_id=gbdrid, decomp_step=decomp_step)

################################################################################
# Zero out non-fistula locations and save results
################################################################################

if zero:
    class_name = "Zero_Fistula"
    input_me  = int(dep_map.loc[dep_map.class_name==class_name,
        'input_me'].item())
    output_me = int(dep_map.loc[dep_map.class_name==class_name,
        'output_mes'].item())

    # zero out locations
    job_list = []
    for year in yearvals:
        job_name = "{cn}_{y}".format(cn=class_name, y=year)
        job_list.append(job_name)
        call = ('qsub -l fthread=3 -l m_mem_free=7.5G'
                ' -q all.q -l archive=TRUE'
                ' -cwd -P proj_rgud'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' python_shell.sh'
                ' maternal_core.py'
                ' "{arg1}" "{arg2}" "{arg3}" "{arg4}" "{arg5}" {arg6}'.format(
                    o=output_path, e=error_path, jn=job_name, 
                    arg1=class_name, arg2=base_dir, arg3=year, 
                    arg4=input_me, arg5=output_me, arg6=decomp_step))
        subprocess.call(call, shell=True)

    # save zero'd out locations
    #hold = "no_holds"
    hold = ",".join(job_list)
    model_version_ids = str(mvid_df.loc[mvid_df.modelable_entity_id==input_me, 
        'model_version_id'].item())
    save_job_name = "save_{meid}".format(meid=output_me)
    out_dir = ('{bd}/{meid}'.format(bd=base_dir, meid=output_me))
    call = ('qsub -hold_jid {hj} -l fthread=25 -l m_mem_free=30G'
            ' -q all.q -l archive=TRUE'
            ' -cwd -P proj_rgud'
            ' -o {o}'
            ' -e {e}'
            ' -N {jn}'
            ' python_shell.sh'
            ' save.py'
            ' {arg1} \'{arg2}\' {arg3} {arg4}'.format(hj=hold, o=output_path, 
                e=error_path, jn=save_job_name, arg1=output_me, 
                arg2=model_version_ids, arg3=out_dir, arg4=decomp_step))
    subprocess.call(call, shell=True)

################################################################################
# Adjust maternal causes for live births
################################################################################

# Run Adjustments
# remove the Zero_Fistula class or it will be uploaded twice
dep_map = dep_map.loc[dep_map.class_name != 'Zero_Fistula',:]
adjust_job_list = []
for index, row in dep_map.iterrows():
    class_name = row['class_name']
    input_me = row['input_me']
    output_me = row['output_mes']

    if class_name == "Fistula" and zero:
        hold = save_job_name
    else:
        hold = "no_holds"

    for year in yearvals:
        job_name = 'adj_{cn}_{y}'.format(cn=class_name, y=year)
        """ this line is to facilitate testing one class at a time, 
        change as needed """
        if (class_name not in ["abcdefghijk..."]):
            adjust_job_list.append(job_name)
            call = ('qsub -hold_jid {hj} -l fthread=4 -l m_mem_free=5G'
                    ' -q all.q -l archive=TRUE'
                    ' -cwd -P proj_rgud'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' maternal_core.py'
                    ' {arg1} {arg2} {arg3} "{arg4}" "{arg5}" {arg6}'.format(hj=hold, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=class_name, arg2=base_dir, arg3=year, 
                        arg4=input_me, arg5=output_me, arg6=decomp_step))
            subprocess.call(call, shell=True)

    # ############################################################################
    # # Upload epi data
    # ############################################################################

    # hold = "no_holds"
    hold = ",".join(adjust_job_list)
    dismod_dict = {
        2624 : ('maternal_sepsis', 377),
        2627 : ('maternal_htn', 827),
        1546 : ('maternal_htn', 829)
    }
    output_mes = [int(x) for x in str(output_me).split(';')]
    epi_upload_list = [x for x in output_mes if x in list(dismod_dict.keys())]
    
    for output_me in epi_upload_list:
        bundle_tuple = dismod_dict[output_me]
        """ this line is to facilitate testing one bundle at a time, 
        change to == as needed """
        if (int(bundle_tuple[1]) > -1):
            data_dir = os.path.join(base_dir, str(output_me))
            call = ('qsub -hold_jid {hj} -l fthread=3 -l m_mem_free=5G'
                    ' -q all.q -l archive=TRUE'
                    ' -cwd -P proj_rgud'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N upload_bundle_{jn}'
                    ' python_shell.sh'
                    ' upload_maternal_epi_bundles.py'
                    ' {arg1} {arg2} {arg3} {arg4}'.format(hj=hold, o=output_path, 
                        e=error_path, jn=bundle_tuple[1], arg1=data_dir, 
                        arg2=bundle_tuple[0], arg3=bundle_tuple[1], arg4=decomp_step))
            subprocess.call(call, shell=True)

    ############################################################################
    # Save results in parallel for everything except dismod_dict keys
    ############################################################################

    # hold = "no_holds"
    hold = ",".join(adjust_job_list)
    save_mes = [x for x in output_mes if x not in list(dismod_dict.keys())]
    for me in save_mes:
        data_dir = os.path.join(base_dir, str(me))
        """ this line is to facilitate testing one bundle at a time, 
        change as needed """
        if me not in [-1]:
            model_ids_str_for_save = ''
            df = extended_map.loc[extended_map.output_me==me,:]
            for index,row in df.iterrows():
                model_ids_str_for_save += ' meid {}, mvid {};'.format(
                    row.input_me, 
                    mvid_df.loc[mvid_df.modelable_entity_id==row.input_me,
                    'model_version_id'].item())
            model_ids_str_for_save = model_ids_str_for_save[:-1] + "."
            call = ('qsub -hold_jid {hj} -l fthread=25 -l m_mem_free=30G'
                    ' -q long.q -l archive=TRUE'
                    ' -cwd -P proj_rgud'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N save_{jn}'
                    ' python_shell.sh'
                    ' save.py'
                    ' {arg1} \'{arg2}\' {arg3} {arg4}'.format(hj=hold, o=output_path, 
                        e=error_path, jn=me, arg1=me, 
                        arg2=model_ids_str_for_save, 
                        arg3=data_dir, arg4=decomp_step))
            subprocess.call(call, shell=True)
    
    sleeptime = 5
    print("Sleeping for {} seconds".format(sleeptime))
    time.sleep(sleeptime)
