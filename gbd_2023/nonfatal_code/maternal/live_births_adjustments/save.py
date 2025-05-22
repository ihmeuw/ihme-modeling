from save_results import save_results_epi
import gbd.constants as gbd
import argparse
import pandas as pd
import numpy as np
import os

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
decomp_step = args.decomp_step

year_ids = gbd.ESTIMATION_YEARS
description = ("Applied live births to incidence; applied duration to"
                 " prevalence. Used the following modelable_entity_ids and"
                 " model_version_ids as inputs: {}".format(model_version_ids))
print(description)
if (me_id == 3620) | (me_id == 3629):
  measids = 6
else:
  measids = [5,6]

sexes = [2]

file_pattern = "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"

# pull in dependency map
dep_map = pd.read_csv('{}/dependency_map_save.csv'.format(os.getcwd()))

columns = ['input_me','output_me']
extended_map = pd.DataFrame(columns=columns)
for index, row in dep_map.iterrows():
  ins = [int(x) for x in str(row.input_me).split(';')]
outs = [int(x) for x in str(row.output_mes).split(';')]
df = pd.DataFrame(columns=columns, data=list(zip(np.repeat(ins, len(outs)), 
                                                 outs*len(ins)))) 
extended_map = extended_map.append(df, ignore_index=True)

input_me = extended_map[extended_map.output_me==me_id]['input_me'].values[0]

epi_ids = pd.read_csv('{}/epi_ids.csv'.format(os.getcwd()))
bundle_id = epi_ids[epi_ids.me_id == input_me]['bundle_id'].values[0]
crosswalk_version_id = epi_ids[epi_ids.me_id == input_me]['crosswalk_version_id'].values[0]

print(f'Using bundle {bundle_id} and crosswalk {crosswalk_version_id} for ME {me_id}')

model_version_df = save_results_epi(input_dir=out_dir,
                                    input_file_pattern=file_pattern,
                                    modelable_entity_id=me_id,
                                    description=description,
                                    year_id=year_ids,
                                    sex_id=sexes,
                                    mark_best=True,
                                    measure_id=measids,
                                    metric_id=3,
                                    n_draws=1000,
                                    db_env='prod',
                                    release_id=release_id,
                                    bundle_id = bundle_id,
                                    crosswalk_version_id = crosswalk_version_id)

print(model_version_df)