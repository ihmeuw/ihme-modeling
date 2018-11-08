# this part of the code uploads the draws to the database

from save_results import save_results_epi
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("model_version_ids", 
    help="The model versions of the input me_ids used in this custom code", 
    type=str)
parser.add_argument("out_dir", help="upload directory", type=str)
args = parser.parse_args()
me_id = args.me_id
model_version_ids = args.model_version_ids
out_dir = args.out_dir

year_ids = [1990, 1995, 2000, 2005, 2010, 2017]
if me_id == 16535:
    description = "Obstetric fistula DisMod {}".format(model_version_ids)
else:
    description = """
        Applied live births to incidence; applied duration to prevalence.
        Used the following modelable_entity_ids and 
        model_version_ids as inputs: {}""".format(model_version_ids)

print(description)
if (me_id == 3620) | (me_id == 3629):
    measids = 6
else:
    measids = [5,6]

sexes = [2]

model_version_df = save_results_epi(input_dir=out_dir,
    input_file_pattern="{measure_id}_{location_id}_{year_id}_{sex_id}.csv",
    modelable_entity_id=me_id,
    description=description,
    year_id=year_ids,
    sex_id=sexes,
    mark_best=True,
    measure_id=measids,
    metric_id=3,
    n_draws=1000,
    db_env='prod',
    gbd_round_id=5)