# this part uploads the results of the HIV split to the epi database

from save_results import save_results_epi
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("save_dir", help="upload directory", type=str)
parser.add_argument("year_id", nargs='*', type=int, help='year ids to include')
args = parser.parse_args()
me_id = args.me_id
save_dir = args.save_dir
year_id = args.year_id


description = 'Proportions calculated from anemia casual attribution'
sexes = [1, 2]
save_results_epi(
    input_dir=save_dir,
    input_file_pattern = '{year_id}.h5', 
    modelable_entity_id=me_id,
    description=description,
    year_id=year_id,
    measure_id=[18],
    mark_best=True,
    db_env='prod',
    gbd_round_id=5
    )
