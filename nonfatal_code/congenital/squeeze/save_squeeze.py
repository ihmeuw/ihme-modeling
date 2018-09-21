# this part uploads the code to the epi database

from adding_machine import agg_locations as al
import argparse
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

parser = argparse.ArgumentParser()
parser.add_argument("me_id", help="The me_id to upload", type=int)
parser.add_argument("cause_name", help="The name of cause group for this upload")
parser.add_argument("model_version_ids", help="The model versions used to calculate squeeze proportions")

args = parser.parse_args()
me_id = args.me_id
cause_name = args.cause_name
model_version_ids = args.model_version_ids

directory =  "{root}/{proc}".format(root=root, proc=cause_name)

year_ids = [1990, 1995, 2000, 2005, 2010, 2016]

description = "Used model_version_ids " + model_version_ids + "."

upload_dir = os.path.join(directory, str(me_id))
sexes = [1, 2]
al.save_custom_results(meid=me_id, description=description,
                       input_dir=upload_dir,
                       sexes=sexes,
                       birth_prev=True,
                       mark_best=True,
                       env='prod', years=year_ids, 
                       custom_file_pattern="{year_id}.h5",
                       h5_tablename="data")
