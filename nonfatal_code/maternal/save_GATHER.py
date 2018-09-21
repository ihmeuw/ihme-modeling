# this part uploads the code to the epi database

from adding_machine import agg_locations as al
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("model_version_ids", help="The model versions of the input me_ids used in this custom code", type=str)
parser.add_argument("out_dir", help="upload directory", type=str)
args = parser.parse_args()
me_id = args.me_id
model_version_ids = args.model_version_ids
out_dir = args.out_dir

year_ids = [1990, 1995, 2000, 2005, 2010, 2016]
if me_id == 16535:
	description = "obstetric fistula DisMod {}".format(model_version_ids)
	sexes = [1,2]
else:
	description = 'applied live births to incidence; applied duration'
	sexes = [2]
al.save_custom_results(meid=me_id, description=description,
                       input_dir=out_dir,
                       sexes=sexes, mark_best=True, env='prod',
                       years=year_ids)
