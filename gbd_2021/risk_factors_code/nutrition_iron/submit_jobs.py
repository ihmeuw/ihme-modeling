from time import sleep
from glob import glob
import pandas as pd
from db_queries import get_location_metadata
from db_queries import get_demographics
import subprocess
import os
from get_draws.api import get_draws

username = 'USERNAME'
ca_version = 'run_10106'
gbd_round = 7
decomp_step = 'iterative'

demogs = get_demographics(gbd_team = 'epi', gbd_round_id = gbd_round)

locs = get_location_metadata(location_set_id=35, gbd_round_id=gbd_round, decomp_step = decomp_step)
locs = locs[locs['most_detailed'] == 1]
locs = locs.location_id.unique().tolist()

ages = demogs['age_group_id']
outdir = 'FILEPATH'
save_dir = 'FILEPATH'
outdir = 'FILEPATH'
sex_ids = demogs['sex_id']
year_ids = demogs['year_id']


def format_list(l):
    l = str(l)
    l = l.replace(" ", "")
    return l


year_id_list = format_list(year_ids)
sex_id_list = format_list(sex_ids)


def submit_exposure_calc():
    for l in locs:
        job_name = 'exposure_calc_{0}'.format(l)
        call = ('qsub -cwd -l m_mem_free=8G -l fthread=1 -l h_rt=02:00:00 -q long.q -P proj_anemia -o'
                ' FILEPATH' +
                ' -e FILEPATH' +
                ' -N {0} test_shell.sh calculate_exposure.py'
                ' --loc_id {1} --gbd_round_id {2} --decomp_step {3}'.format(job_name, l, gbd_round, decomp_step)
                )
        subprocess.call(call, shell=True)


meids_exposure = [8882]

def save_rf():
    for meid in meids_exposure:
        job_name = 'save_exposure_{}'.format(meid)
        call = ('qsub -cwd -l m_mem_free=100G -l fthread=10 -l h_rt=05:00:00 -q long.q -P proj_anemia -o'
                ' FILEPATH' +
                ' -e /FILEPATH' +
                ' -N {0} test_shell.sh save_rf.py'
                ' --modelable_entity_id {1}'
		        ' --gbd_round_id {2} --decomp_step {3}'.format(job_name, meid, gbd_round, decomp_step)
                )
        subprocess.call(call, shell=True)



def submit_tmrel_calc():
    for l in locs:
        job_name = 'tmrel_calc_{0}'.format(l)
        call = ('qsub -cwd -l m_mem_free=8G -l fthread=1 -l h_rt=02:00:00 -q long.q -P proj_anemia -o'
                ' FILEPATH' +
                ' -e FILEPATH' +
                ' -N {0} test_shell.sh calculate_tmrel.py'
                ' --loc_id {1} --gbd_round_id {2} --decomp_step {3}'.format(job_name, l, gbd_round, decomp_step)
                )
        subprocess.call(call, shell=True)



meids_tmrel = [9151]
def save_tmrel():
    for meid in meids_tmrel:
        job_name = 'tmrel_for_iron_{}'.format(meid)
        call = ('qsub -cwd -l m_mem_free=120G -l fthread=6 -l h_rt=05:00:00 -q long.q -P proj_anemia -o'
                ' FILEPATH' +
                ' -e FILEPATH' +
                ' -N {0} test_shell.sh save_tmrel_draws.py'
                ' --modelable_entity_id {1} --gbd_round_id {2}'
		' --decomp_step {3}'.format(job_name, meid, gbd_round, decomp_step)
                )
        subprocess.call(call, shell=True)

