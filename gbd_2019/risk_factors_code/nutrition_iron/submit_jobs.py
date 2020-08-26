from time import sleep
from glob import glob
import pandas as pd
from db_queries import get_location_metadata
import subprocess
import os
from get_draws.api import get_draws

username = 'USERNAME'
ca_version = 'D4V5'

locs = get_location_metadata(location_set_id=35, gbd_round_id=6, decomp_step = 'step4')
locs = locs[locs['most_detailed'] == 1]
locs = locs.location_id.unique().tolist()

ages = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235]
# DO NOT put a slash at the end of this filepath!!!
outdir = 'FILEPATH'
save_dir = 'FILEPATH'
sex_ids = [1, 2]
# year_ids = [1990, 1995, 2000, 2005, 2010, 2017, 2019]
year_ids = [1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019]


def format_list(l):
    l = str(l)
    l = l.replace(" ", "")
    return l


year_id_list = format_list(year_ids)
sex_id_list = format_list(sex_ids)


def make_new_hgb_mean_file():
    '''
    This function writes a flat file of the draws for mean hemoglobin.
    Will only work with many slots.
    Run this function prior to anemia CA, any time that the mean hemoglobin model has been updated.
    '''
    print("Operation starting")
    hgb = get_draws('modelable_entity_id', 10487, 'epi', gbd_round_id=6, decomp_step='step4')
    hgb["hgb_mean"] = hgb[['draw_%s' % d for d in list(range(1000))]].mean(axis=1)
    hgb["mean_hgb"] = hgb["hgb_mean"]
    hgb = hgb.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
    renames = {'draw_%s' % d: 'hgb_%s' % d for d in list(range(1000))}
    hgb.rename(columns=renames, inplace=True)
    print("Writing File")
    hgb.to_hdf('FILEPATH',
               key='draws',
               mode='w',
               format='table',
               data_columns=['location_id',
                             'age_group_id',
                             'year_id',
                             'sex_id'
                             ]
               )


def submit_normal_hgb_calc():
    for y in year_ids:
        for s in sex_ids:
            for a in ages:
                job_name = 'cf{0}_{1}_{2}'.format(y, s, a)
                call = ('qsub -cwd -l m_mem_free=7G -l fthread=1 -l h_rt=02:00:00 -q all.q -P proj_anemia -o'
                        ' FILEPATH' +
                        ' FILEPATH' +
                        ' -N {0} FILEPATH'
                        ' --year_id {1} --sex_id {2} --age_group_id {3}'.format(job_name, y, s, a)
                        )
                subprocess.call(call, shell=True)


# Next, need to run compile_normal_hgb.py to generate normal hemoglobin file.
meids_tmrel = [9151]
gbd_round_id = 6
decomp_step = 'step4'


def submit_tmrel():
    for meid in meids_tmrel:
        job_name = 'tmrel_for_iron_{}'.format(meid)
        call = ('qsub -cwd -l m_mem_free=120G -l fthread=6 -l h_rt=05:00:00 -q long.q -P proj_anemia -o'
                ' -N {0} FILEPATH'
		' --modelable_entity_id {1} --gbd_round_id {2}'
		' --decomp_step {3}'.format(job_name, meid, gbd_round_id, decomp_step)
                )
        subprocess.call(call, shell=True)


def submit_rf():
    for loc in locs:
        job_name = 'anem_rf_{}'.format(loc)
        call = ('qsub -cwd -l m_mem_free=15G -l fthread=2 -l h_rt=03:00:00 -q all.q -P proj_anemia -o'
                ' -N {0} FILEPATH'
		' --location {1} --indir {2} --year_id 1990 1995 2000 2005 2010 2015 2017 2019'
		' --gbd_round_id {3} --decomp_step {4}'.format(job_name, loc, outdir, gbd_round_id, decomp_step)
                )
        subprocess.call(call, shell=True)

meids_rf = [8882]

def save_rf():
    for meid in meids_rf:
        job_name = 'save_rf_{}'.format(meid)
        call = ('qsub -cwd -l m_mem_free=100G -l fthread=10 -l h_rt=05:00:00 -q long.q -P proj_anemia -o'
                ' -N {0} FILEPATH'
		' --modelable_entity_id {1} --year_id 1990 1995 2000 2005 2010 2015 2017 2019'
		' --gbd_round_id {2} --decomp_step {3}'.format(job_name, meid, gbd_round_id, decomp_step)
                )
        subprocess.call(call, shell=True)
