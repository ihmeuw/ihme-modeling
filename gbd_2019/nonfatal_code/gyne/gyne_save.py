# this part uploads the code to the epi database
### this script will still need to be manually updated as decomp_step changes

from save_results import save_results_epi
import gbd.constants as gbd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("in_dir", help="The directory where the draws are stored")
parser.add_argument("decomp_step", help='The decomp step')
parser.add_argument("description", help="A description of the model versions used in the congenital core code")

args = parser.parse_args()
me_id = args.me_id
in_dir = args.in_dir
decomp_step = args.decomp_step
description = args.description

year_ids = gbd.ESTIMATION_YEARS
upload_dir = os.path.join(in_dir, str(me_id))
sexes = [2]

model_version_df = save_results_epi(input_dir=upload_dir,
                                    input_file_pattern="{location_id}.h5",
                                    modelable_entity_id=me_id,
                                    description=description,
                                    year_id=year_ids,
                                    sex_id=sexes,
                                    mark_best=True,
                                    measure_id=[5,6],
                                    metric_id=3,
                                    n_draws=1000,
                                    db_env='prod',
                                    gbd_round_id=gbd.GBD_ROUND_ID,
                                    decomp_step = decomp_step)
