import subprocess
import pandas as pd
import os
import re
from datetime import datetime
from adding_machine import agg_locations as sr
import glob
from cluster_utils import submitter
import time
from elmo import run
from db_queries import get_best_model_versions

#############################################################################
# Please read the readme.txt in this repo. It will explain the whole strategy
#############################################################################

def wait(pattern, seconds):
    seconds = int(seconds)
    while True:
        qstat = submitter.qstat()
        if qstat['name'].str.contains(pattern).any():
            print time.localtime()
            time.sleep(seconds)
            print time.localtime()
        else:
            break


# pull in dependency map
dep_map = pd.read_csv('%s/dependency_map.csv' % os.getcwd())
input_mes = dep_map.input_me.unique()
output_string = dep_map.output_mes.unique()
output_mes = []
for i in output_string:
    output_mes.extend(i.split(';'))

# make timestamped output folder
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)
base_dir = '{FILEPATH}/%s' % c_date


for output_me in output_mes:
    out_dir = ('%s/%s' % (base_dir, output_me))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)


yearvals = [1990, 1995, 2000, 2005, 2010, 2016] 

# Zero out Fistula draws and save results
job_string = ''
for year in yearvals:
    class_name = "Zero_Fistula"
    input_me = zero_fime
    output_me = zero_fome
    job_name = "%s_%s" % (class_name, year)
    job_string = job_string + ',' + job_name
    call = ('qsub -cwd -P proj_custom_models '
                    '-o {FILEPATH} '
                    '-e {FILEPATH} '
                    '-pe multi_slot 15 '
                    '-N %s cluster_shell.sh fistula_zero_out_locations.py '
                    '"%s" "%s" "%s" "%s" "%s"'
                    % (job_name, class_name, base_dir, year, input_me,
                       output_me))
    subprocess.call(call, shell=True)


#get best fistual model version ids
zero_fime = 1552
zero_fome = 16535
df = get_best_model_versions(entity='modelable_entity',ids=zero_fime, gbd_round_id=4)
model_version_ids = ", ".join(str(e) for e in df['model_version_id'].tolist())

# Save zero'd out fistula locations
#job_string= "no_holds"
save_job_name = "save_{}".format(zero_fome)
out_dir = ('%s/%s' % (base_dir, zero_fome))
call = ('qsub  -hold_jid {0} -cwd -P proj_custom_models -o'
                ' {FILEPATH}'
                ' -e {FILEPATH} -N {1}'
                ' -pe multi_slot 15'
                ' cluster_shell.sh save.py {2} \'{3}\' {4}'.format(job_string, job_name, zero_fome, model_version_ids, out_dir))
subprocess.call(call, shell=True)

# Run Adjustments
for index, row in dep_map.iterrows():
    for year in yearvals:
        #job_string= "no_holds"
        class_name = row['class_name']
        input_me = row['input_me']
        output_me = row['output_mes']
        if class_name == "Fistula":
            call = ('qsub -hold_jid %s -cwd -P proj_custom_models '
                    '-o {FILEPATH} '
                    '-e {FILEPATH} '
                    '-pe multi_slot 15 '
                    '-N adjust_%s_%s cluster_shell.sh maternal_core.py "%s" '
                    '"%s" "%s" "%s" "%s"'
                    % (save_job_name, class_name, year, class_name, base_dir, year, input_me,
                       output_me))
            print call
            subprocess.call(call, shell=True)

# # Wait for Adjustments to finish.
wait('adjust', 300)

# Conatenate epi uploader for some of the sepsis and hypertension mmodelable
dismod_mes = ['2624', '3931', '3928']
name_bundle_list = ['maternal_sepsis/377', 'maternal_htn/827',
                    'maternal_htn/829']
me_bundle_pairs = zip(dismod_mes, name_bundle_list)
for me, bundle in me_bundle_pairs:
    grab_dir = '{0}/{1}/'.format(base_dir, me)
    to_upload_dir = ('{FILEPATH}'.format(bundle))
    if not os.path.exists(to_upload_dir):
        os.makedirs(to_upload_dir)
    to_upload = []
    for f in glob.glob(grab_dir + "*.csv"):
        df = pd.read_csv(f)
        to_upload.append(df)
    upload_me = pd.concat(to_upload)
    upload_me.to_excel('%s/LBA_upload.xlsx'
                       % to_upload_dir, sheet_name='extraction', index=False)
    
    # grab current data in bundle, delete it all, then upload new data
    # make sure that export=True so that a copy of the current data is saved
    # before modification
    
    # delete data
    epi = run.get_epi_data(bundle, export=True)
    epi = epi[['bundle_id','seq']]
    destination_file = '%s/delete_all.xlsx' % to_upload_dir
    epi.to_excel(destination_file, sheet_name="extraction", index=False)
    report = run.upload_epi_data(bundle, destination_file)
    assert (report['request_status'].item()=='Successful')
    
    # upload new data
    destination_file = '%s/LBA_upload.xlsx' % to_upload_dir
    report = run.upload_epi_data(bundle, destination_file)
    assert (report['request_status'].item()=='Successful')


# Save Results in parallel for everything except dismod mes
remove_mes = ['2624', '3931', '3928', '16535']
for me in remove_mes:
    output_mes.remove(me)

for me in output_mes:
    me = int(me)
    out_dir = ('%s/%s' % (base_dir, me))
    if (me == 1553) or (me == 1554):
        call = ('qsub -cwd -P proj_custom_models -o'
                ' {FILEPATH}'
                ' -e {FILEPATH} -N save_{0}'
                ' -pe multi_slot 15'
                ' cluster_shell.sh save.py {0} \'{1}\' {2}'.format(me, '', out_dir))
        print call
        subprocess.call(call, shell=True)
