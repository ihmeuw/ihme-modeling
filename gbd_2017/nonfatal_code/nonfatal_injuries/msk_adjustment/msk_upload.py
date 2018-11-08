import os
from gbd_inj.inj_helpers import help, paths
import db_queries as db
from save_results import save_results_epi


def upload():
    folder = os.path.join('FILEPATH')
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    save_results_epi(input_dir=folder,
                     input_file_pattern='FILEPATH.h5',
                     modelable_entity_id=3136,
                     description='other msk adjusted for injuries fractures and dislocations',
                     year_id=dems['year_id'],
                     measure_id=5,
                     mark_best=True)
    print('Successfully uploaded Other MSK')
    

if __name__ == '__main__':
    upload()
