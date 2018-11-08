import pandas as pd
import numpy as np
import subprocess
import os
import shutil
from db_queries import (get_location_metadata, 
                            get_population, get_ids)
import time


## ****************************************************************************
## WORKSPACE
## ****************************************************************************
# TODO: Change the username and/or root path so that the person running the 
# code has write privileges to the output directories
username = "USERNAME"
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

out_dir = "FILEPATH"
code_dir = "FILEPATH"
temp_dir = "FILEPATH"

if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)
else:
    shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
have_paths = 1
eurocat = True
norway = False

fname = "FILENAME"

## ****************************************************************************
## DATA AND HELPER FUNCTIONS
## ****************************************************************************

# pull in congenital bundle/model/cause mapping file
map_file = "FILEPATH"
map_df = pd.read_excel(map_file, header=0)
# drop null rows
map_df = map_df.loc[~map_df.fullmod_bundle.isnull()]

def get_envelope_bundles():
    envelope_bundles = map_df.loc[map_df.description.str.startswith("Total", 
        na=False),'fullmod_bundle'].tolist()
    #cong_neural 608 doesn't have 'other' so remove it from envelope list
    envelope_bundles.remove(608)
    return envelope_bundles

def prep_norway_pop_weights(code_dir):
    loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=5)
    loc_meta.to_csv(os.path.join(code_dir, 'location_metadata.csv'), 
        index=False, encoding='utf8')
    norway_id = 90
    norway_subs = loc_meta.loc[loc_meta.parent_id==norway_id, 'location_id'].tolist() + [norway_id]
    country_pop = get_population(location_id=norway_id, year_id='all', 
        sex_id='all', age_group_id=164, gbd_round_id=5, status='best')
    country_pop.drop('location_id', axis=1, inplace=True)
    subs_pop = get_population(location_id=norway_subs, year_id='all', 
        sex_id='all', age_group_id=164, gbd_round_id=5, status='best')
    population = subs_pop.merge(country_pop, 
        on=[c for c in subs_pop.columns if c not in ['location_id','population']], 
        suffixes=('_subs', '_national'))
    population.loc[:, 'pop_weight'] = population['population_subs'] / 
        population['population_national']
    sex_meta = get_ids('sex')
    population = pd.merge(population, sex_meta, on='sex_id')
    population.loc[:, 'age_start'] = 0
    population.rename(columns={'year_id':'year_start'}, inplace=True)
    population.to_csv(os.path.join(code_dir, 'norway_population.csv'), 
        index=False, encoding='utf8')


cause_bundle_pairs = list(zip(map_df.cause, map_df.fullmod_bundle))
bundle_num = len(map_df.fullmod_bundle.tolist())

if have_paths==0:
    job_string = ''
    for cause, bundle in cause_bundle_pairs:
        bundle = int(bundle)
        job_name = "get_reqids_{b}_{c}".format(b=bundle, c=cause)
        job_string = job_string + ',' + job_name
        call = ('qsub -l mem_free=6.0G -pe multi_slot 3'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' get_request_ids.py'
                ' {arg1} {arg2}'.format(o=output_path, e=error_path,
                    jn=job_name, arg1=bundle, arg2=temp_dir))
        subprocess.call(call, shell=True)

    # aggregate all the download paths into one file
    call = ('qsub  -hold_jid {hj} -l mem_free=6.0G -pe multi_slot 3'
            ' -cwd -P proj_custom_models'
            ' -o {o}'
            ' -e {e}'
            ' -N agg_paths'
            ' cluster_shell.sh'
            ' aggregate_paths.py'
            ' {arg1} \'{arg2}\' {arg3} {arg4}'.format(hj=job_string, 
                o=output_path, e=error_path, arg1=temp_dir, arg2=out_dir, 
                arg3=fname, arg4=bundle_num))

    subprocess.call(call, shell=True)

if have_paths==0: 
    hold = "agg_paths"
else:
    hold = "no_holds"

# ****************************************************************************
## UPLOAD EUROCAT FROM PRE-EXISTING COMPILED FILE
## ****************************************************************************
if eurocat:
    for cause, bundle in cause_bundle_pairs:
        bundle = int(bundle)
        if bundle > -1:
            job_name = 'upload_eurocat_{}'.format(bundle)
            call = ('qsub  -hold_jid {hj} -l mem_free=6.0G -pe multi_slot 3'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' upload_eurocat.py'
                        ' {arg1} {arg2} \'{arg3}\' {arg4}'.format(hj=hold, 
                            o=output_path, e=error_path, jn=job_name, 
                            arg1=bundle, arg2=cause, arg3=code_dir, 
                            arg4=fname))
            subprocess.call(call, shell=True)
            time.sleep(10)

# ****************************************************************************
## BACKFILL EUROCAT SUBNATIONALS
## ****************************************************************************
if norway:
    prep_norway_pop_weights(code_dir)
    for cause, bundle in cause_bundle_pairs:
        bundle = int(bundle)
        if bundle > -1:
            job_name = 'norway_backfill_{}'.format(bundle)
            call = ('qsub  -hold_jid {hj} -pe multi_slot 2'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' norway_backfill.py'
                        ' {arg1} {arg2} \'{arg3}\' {arg4}'.format(hj=hold, 
                            o=output_path, e=error_path, jn=job_name, 
                            arg1=bundle, arg2=cause, arg3=code_dir, 
                            arg4=fname))
            subprocess.call(call, shell=True)
            time.sleep(10)
