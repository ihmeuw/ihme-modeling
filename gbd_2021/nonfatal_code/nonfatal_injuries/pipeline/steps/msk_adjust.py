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

def main(year):

    print(f"Running MSK for {year}")

    inj_list = []
    # get fracture/dislocation/crush ncodes from ALL ecodes
    ecodes_list = inj_info.DETAILED_ECODES
<<<<<<< HEAD:FILEPATH
    ecodes_list_no_shocks = [e for e in ecodes_list if e not in ['inj_disaster', 'inj_war_warterror', 'inj_war_execution']]
    for ecode in ecodes_list_no_shocks:
        print(f"Running {ecode}")
=======
    for ecode in ecodes_list:
        print(ecode)
        parent = inj_info.ECODE_PARENT[ecode]
>>>>>>> origin/master:FILEPATH
        ecode_list = []
        version = str(input_manager.get_input(Ecodes.by_name(ecode)).id)
        for ncode in inj_info.MSK_NCODES:
<<<<<<< HEAD:gbd_inj/msk/msk_adjust.py
            path = (FILEPATH
            )
            try:
                inj_data = pd.read_hdf(path)
                ecode_list.append(inj_data)
            except:
                print("ERROR: ", path)
=======
            print(ncode)
            version = ecode_run_version[ecode]
            path = os.path.join(FILEPATH))
            inj_data = pd.read_hdf(path)
            ecode_list.append(inj_data)
>>>>>>> origin/master:FILEPATHy
        try:
            ecode_inj = pd.concat(ecode_list)
            ecode_inj = ecode_inj.groupby(['location_id', 'year_id', 'sex_id', 'age_group_id']).sum()
            inj_list.append(ecode_inj)
        except ValueError as e:
            print(f"error for {ecode}, {year}")
            print(e)
    print("Concating...")
    all_inj = pd.concat(inj_list)
    all_inj = all_inj.groupby(['location_id','year_id','sex_id','age_group_id']).sum()

    print('Getting oth msk')
    # get other msk model
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
    oth_msk = gd.get_draws(gbd_id_type='modelable_entity_id',
<<<<<<< HEAD:FILEPATH
                           gbd_id=24623,
=======
                           gbd_id=24623,  # Updated from 2161 on 10-8. This is the new EMR model.
>>>>>>> origin/master:FILEPATH
                           location_id=dems['location_id'],
                           year_id=year,
                           sex_id=dems['sex_id'],
                           age_group_id=dems['age_group_id'],
                           status='best',
                           source='epi',
                           measure_id=5,
                           gbd_round_id=config.GBD_ROUND,
                           decomp_step=config.DECOMP)

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
    new_msk['measure_id'] = 5 
    save_dir = FILEPATH'
    if not save_dir.exists():
        save_dir.mkdir(parents=True)

    new_msk.to_hdf(FILEPATH)
    print('All done!')


if __name__ == '__main__':
<<<<<<< HEAD:FILEPATH
=======
    print("Running adjustment")
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--decomp', default='iterative', help='What decomp step this pipeline run is for. '
                                                                    'Specify either step1, step2, step3, step4 or step5. '
                                                                    'Default is to run in iterative mode.')
    args = parser.parse_args()
    decomp = args.decomp
    decomp = decomp.rstrip()
    print(decomp)
>>>>>>> origin/master:FILEPATH
    task_id = int(os.environ.get("SGE_TASK_ID"))
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=config.GBD_ROUND)
    year = dems['year_id'][task_id-1]
    main(year)
