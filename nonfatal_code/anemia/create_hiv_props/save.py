# this part uploads the results of the HIV split to the epi database

from adding_machine import agg_locations as al
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("save_dir", help="upload directory", type=str)
args = parser.parse_args()
me_id = args.me_id
save_dir = args.save_dir

year_ids = [{YEAR IDS}]
description = 'Proportions calculated from anemia casual attribution'
sexes = [{SEX IDS}]
al.save_custom_results(meid=me_id, description=description,
                       input_dir=save_dir,
                       sexes=sexes, mark_best=True, env={ENV},
                       years=year_ids, custom_file_pattern='{year_id}.h5',
                       h5_tablename="draws")
