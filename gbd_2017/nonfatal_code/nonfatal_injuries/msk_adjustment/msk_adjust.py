from __future__ import division

import pandas as pd
from gbd_inj.inj_helpers import help, versions, inj_info, paths
import os
import get_draws.api as gd
import db_queries as db


def main(year):
    inj_list = []
    # get fracture/dislocation/crush ncodes from ALL ecodes
    for ecode in inj_info.DETAILED_ECODES:
        print(ecode)
        parent = inj_info.ECODE_PARENT[ecode]
        ecode_list = []
        for ncode in inj_info.MSK_NCODES:
            print(ncode)
            path = os.path.join('FILEPATH.h5'.format(str(year)))
            inj_data = pd.read_hdf(path)
            ecode_list.append(inj_data)
        try:
            ecode_inj = pd.concat(ecode_list)
            ecode_inj = ecode_inj.groupby(['location_id', 'year_id', 'sex_id', 'age_group_id']).sum()
            inj_list.append(ecode_inj)
        except ValueError, e:
            print('error for {}, {}'.format(ecode, year))
            print(e)
    all_inj = pd.concat(inj_list)
    all_inj = all_inj.groupby(['location_id','year_id','sex_id','age_group_id']).sum()
    
    print('Getting oth msk')
    # get other msk model
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    oth_msk = gd.get_draws(gbd_id_type='modelable_entity_id',
                           gbd_id=2161,
                           location_id=dems['location_id'],
                           year_id=year,
                           sex_id=dems['sex_id'],
                           age_group_id=dems['age_group_id'],
                           status='best',
                           source='epi',
                           measure_id=5,
                           gbd_round_id=help.GBD_ROUND)
    
    dropcols = ['modelable_entity_id', 'model_version_id', 'measure_id', 'metric_id']
    oth_msk.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
    oth_msk.set_index(indexcols, inplace=True)
    
    print('Subtracting and saving')
    # subtract injuries from oth msk
    new_msk = oth_msk - all_inj
    new_msk[new_msk < 0] = 0
    
    # save
    new_msk.reset_index(inplace=True)
    save_dir = os.path.join('FILEPATH')
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    new_msk.to_hdf(os.path.join(save_dir, 'FILEPATH.h5'.format(y=year)), 'draws', mode='w', format='table',
                   data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    print('All done!')


if __name__ == '__main__':
    task_id = int(os.environ.get("SGE_TASK_ID"))
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    year = dems['year_id'][task_id-1]
    print(year)
    main(year)
