# this part uploads the code to the epi database

from save_results import save_results_epi
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("in_dir", help="The directory where the draws are stored")
parser.add_argument("description", 
      help="A description of the model versions used in the congenital core code")

args = parser.parse_args()
me_id = args.me_id
in_dir = args.in_dir
description = args.description

year_ids = [1990, 1995, 2000, 2005, 2010, 2017]
upload_dir = os.path.join(in_dir, str(me_id))
sexes = [1, 2]

model_version_df = save_results_epi(input_dir=upload_dir,
                                    input_file_pattern="{location_id}.h5",
                                    modelable_entity_id=me_id,
                                    description=description,
                                    year_id=year_ids,
                                    sex_id=sexes,
                                    measure_id=[5],
                                    metric_id=3,
                                    mark_best=True,
                                    birth_prevalence=True,
                                    n_draws=1000,
                                    db_env='prod',
                                    gbd_round_id=5)
