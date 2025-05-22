"""
We run an adjustment on the Other MSK DisMod model (not our team) to account for
the long-term effects of injuries. Basically, we're just saying that we need to
subtract all long-term injury prevalence due to fractures and some dislocations
from the Other MSK model prevalence estimates. This step needs to happen after
all the e-codes are done running, since it draws fracture prevalence from
every e-code.
"""

from __future__ import division

import sys

import os
import pandas as pd

import db_queries as db
import get_draws.api as gd

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import inj_info, paths, input_manager
from gbd_inj.types import Ecodes

# Pipeline versions are hard-coded to ensure the right files are pulled. This should
# be updated once we have a more stable versioning system in place to automatically
# pull the newest or best results.
ecode_run_version = {
    'inj_animal_nonven': 196,
    'inj_animal_venom': 197,
    'inj_drowning': 199,
    'inj_electrocution': 205, 
    'inj_falls': 200,
    'inj_fires': 198,
    'inj_foreign_aspiration': 215,
    'inj_foreign_eye': 216,
    'inj_foreign_other': 217,
    'inj_homicide_gun': 208,
    'inj_homicide_knife': 206,
    'inj_homicide_other': 207,
    'inj_mech_gun': 203,
    'inj_mech_other': 204,
    'inj_medical': 199,
    'inj_non_disaster': 201,
    'inj_othunintent': 204,
    'inj_poisoning_gas': 202,
    'inj_poisoning_other': 201,
    'inj_suicide_firearm': 208,
    'inj_suicide_other': 207,
    'inj_trans_other': 203,
    'inj_trans_road_2wheel': 210,
    'inj_trans_road_4wheel': 211,
    'inj_trans_road_other': 212,
    'inj_trans_road_pedal': 213,
    'inj_trans_road_pedest': 214,
    'inj_war_execution': 1,
    'inj_war_warterror': 1,
    'inj_disaster': 1
}

# Ecode names hard-coded to make sure all are included (electrocution (new for 2020), shocks, 
# etc.) but there should be a better way to to do this. 
'''
ecodes_list = ['inj_animal_nonven',
    'inj_animal_venom',
    'inj_drowning',
    'inj_electrocution', 
    'inj_falls',
    'inj_fires',
    'inj_foreign_aspiration',
    'inj_foreign_eye',
    'inj_foreign_other',
    'inj_homicide_gun',
    'inj_homicide_knife',
    'inj_homicide_other',
    'inj_mech_gun',
    'inj_mech_other',
    'inj_medical',
    'inj_non_disaster',
    'inj_othunintent',
    'inj_poisoning_gas',
    'inj_poisoning_other',
    'inj_suicide_firearm',
    'inj_suicide_other',
    'inj_trans_other',
    'inj_trans_road_2wheel',
    'inj_trans_road_4wheel',
    'inj_trans_road_other',
    'inj_trans_road_pedal',
    'inj_trans_road_pedest',
    'inj_war_execution',
    'inj_war_warterror',
    'inj_disaster']
'''

def main(year):
 """
    Main function to adjust the Other MSK DisMod model for injury prevalence.
    
    Args:
        year (int): The year for which the adjustment is being made.
    """
    print(f"Running MSK for {year}")

    # Initialize an empty list to store injury data from each e-code
    inj_list = []

    # get fracture/dislocation/crush ncodes from ALL ecodes
    ecodes_list = inj_info.DETAILED_ECODES
    # want to include shocks... can adjust script below if we want to leave out shocks
    #ecodes_list_no_shocks = [e for e in ecodes_list if e not in ['inj_disaster', 'inj_war_warterror', 'inj_war_execution']]
    for ecode in ecodes_list:
        print(f"Running {ecode}")
        ecode_list = []
        version = str(ecode_run_version[ecode])
        for ncode in inj_info.MSK_NCODES:
            path = ("FILEPATH")
            try:
                inj_data = pd.read_hdf(path)
                ecode_list.append(inj_data)
            except:
                print("ERROR: ", path)
        try:
            ecode_inj = pd.concat(ecode_list)
            ecode_inj = ecode_inj.groupby(['location_id', 'year_id', 'sex_id', 'age_group_id']).sum()
            inj_list.append(ecode_inj)
        except ValueError as e:
            print(f"error for {ecode}, {year}")
            print(e)
    print("Concatenating...")
    all_inj = pd.concat(inj_list)
    all_inj = all_inj.groupby(['location_id','year_id','sex_id','age_group_id']).sum()

    print('Getting oth msk')
    # get other msk model
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
    oth_msk = gd.get_draws(gbd_id_type='modelable_entity_id',
                           gbd_id=24623,
                           location_id=dems['location_id'],
                           year_id=year,
                           sex_id=dems['sex_id'],
                           age_group_id=dems['age_group_id'],
                           status='best',
                           source='epi',
                           measure_id=5,
                           gbd_round_id=config.GBD_ROUND,
                           decomp_step=config.DECOMP)

    # Drop unnecessary columns
    dropcols = ['modelable_entity_id', 'model_version_id', 'measure_id', 'metric_id']
    oth_msk.drop(dropcols, axis=1, inplace=True)

    indexcols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
    oth_msk.set_index(indexcols, inplace=True)

    print('Subtracting and saving')
    # Subtract injury prevalence from Other MSK prevalence
    new_msk = oth_msk - all_inj
    
    # Ensure no negative values 
    new_msk[new_msk < 0] = 0

    # save
    new_msk.reset_index(inplace=True)
    new_msk['measure_id'] = 5 
    save_dir = paths.DATA_DIR / config.DECOMP / 'oth_msk'
    if not save_dir.exists():
        save_dir.mkdir(parents=True)

    new_msk.to_hdf(save_dir/f"5_{year}.h5", 'draws', mode='w', format='table',
                   data_columns=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    print('All done!')


if __name__ == '__main__':
    task_id = int(os.environ.get("SGE_TASK_ID"))
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
    year = dems['year_id'][task_id-1]
    main(year)