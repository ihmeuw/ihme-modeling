from __future__ import division

import pandas as pd
from gbd_inj.inj_helpers import help, versions, inj_info, paths
import os
import get_draws.api as gd
import db_queries as db
import argparse

ecode_run_version = {
    'inj_animal': 43,
    'inj_animal_nonven': 43,
    'inj_animal_venom': 43,
    'inj_drowning': 46,
    'inj_falls': 43,
    'inj_fires': 46,
    'inj_foreign_aspiration': 43,
    'inj_foreign_eye': 46,
    'inj_foreign_other': 43,
    'inj_homicide': 46,
    'inj_homicide_gun': 46,
    'inj_homicide_knife': 46,
    'inj_homicide_other': 46,
    'inj_mech': 43,
    'inj_mech_gun': 43,
    'inj_mech_other': 46,
    'inj_medical': 44,
    'inj_non_disaster': 46,
    'inj_othunintent': 43,
    'inj_poisoning': 43,
    'inj_poisoning_gas': 43,
    'inj_poisoning_other': 43,
    'inj_suicide': 46,
    'inj_suicide_firearm': 46,
    'inj_suicide_other': 46,
    'inj_trans_other': 43,
    'inj_trans_road': 46,
    'inj_trans_road_2wheel': 46,
    'inj_trans_road_4wheel': 46,
    'inj_trans_road_other': 46,
    'inj_trans_road_pedal': 46,
    'inj_trans_road_pedest': 46,
    'inj_war_execution': 46,
    'inj_war_warterror': 46,
    'inj_disaster': 46,
}

def main(year, decomp):

    inj_list = []
    ecodes_list = inj_info.DETAILED_ECODES
    for ecode in ecodes_list:
        print(ecode)
        parent = inj_info.ECODE_PARENT[ecode]
        ecode_list = []
        for ncode in inj_info.MSK_NCODES:
            print(ncode)
            version = ecode_run_version[ecode]
            path = os.path.join(paths.DATA_DIR, decomp, parent, str(version), 'upload',
                                ecode, ncode, '36_{}.h5'.format(str(year)))
            inj_data = pd.read_hdf(path)
            ecode_list.append(inj_data)
        try:
            ecode_inj = pd.concat(ecode_list)
            ecode_inj = ecode_inj.groupby(['location_id', 'year_id', 'sex_id', 'age_group_id']).sum()
            inj_list.append(ecode_inj)
        except ValueError as e:
            print('error for {}, {}'.format(ecode, year))
            print(e)
    all_inj = pd.concat(inj_list)
    all_inj = all_inj.groupby(['location_id','year_id','sex_id','age_group_id']).sum()
    
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    oth_msk = gd.get_draws(gbd_id_type='modelable_entity_id',
                           gbd_id=24623, 
                           location_id=dems['location_id'],
                           year_id=year,
                           sex_id=dems['sex_id'],
                           age_group_id=dems['age_group_id'],
                           status='best',
                           source='epi',
                           measure_id=5,
                           gbd_round_id=help.GBD_ROUND,
                           decomp_step=decomp)
    
    dropcols = ['modelable_entity_id', 'model_version_id', 'measure_id', 'metric_id']
    oth_msk.drop(dropcols, axis=1, inplace=True)
    
    indexcols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
    oth_msk.set_index(indexcols, inplace=True)
    
    new_msk = oth_msk - all_inj
    new_msk[new_msk < 0] = 0
    
    new_msk.reset_index(inplace=True)
    save_dir = os.path.join(paths.DATA_DIR, decomp, 'oth_msk')
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    new_msk.to_hdf(os.path.join(save_dir, '5_{y}.h5'.format(y=year)), 'draws', mode='w', format='table',
                   data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id'])
