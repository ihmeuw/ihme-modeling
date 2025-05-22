# this part uploads the code to the epi database

from save_results import save_results_epi
from gbd.estimation_years import estimation_years_from_gbd_round_id
from job_utils.nch_db_queries import get_bundle_xwalk_version_for_best_model
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("in_dir", help="The directory where the draws are stored")
parser.add_argument("description", 
    help="A description of the model versions used in the congenital core code")
parser.add_argument("gbd_round_id", help="The gbd round for the draws to be uploaded", type=int)
parser.add_argument("decomp_step", help="Decomp step string, ex. step2 ")
parser.add_argument("bundle_id", help="Bundle id to associate with uploaded draws", type=int)
parser.add_argument("crosswalk_version_id", help="Crosswalk to associate with uploaded draws", type=int)

args = parser.parse_args()
me_id = args.me_id
gbd_round_id = args.gbd_round_id
decomp_step = args.decomp_step
in_dir = args.in_dir
description = args.description
bundle_id = args.bundle_id
xwalk_version = args.crosswalk_version_id

year_ids = estimation_years_from_gbd_round_id(gbd_round_id)
upload_dir = os.path.join(in_dir, str(me_id))
sexes = [1, 2]
print(str(args))
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
                                    gbd_round_id=gbd_round_id,
                                    decomp_step=decomp_step,
                                    bundle_id=bundle_id,
                                    crosswalk_version_id=xwalk_version)
