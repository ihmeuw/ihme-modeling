import os
import argparse
from multiprocessing import Pool
import sys
import getpass
import pandas as pd

from adding_machine import summarizers as sm
sys.path.insert(0, "FILEPATH")
from gbd5q0py.config import Config5q0


def get_submodel_adjustment_file(file_path):
  # Read in the data
  data = pd.read_csv(file_path)
data.adjre_fe.fillna(1)
# Rename columns to start
new_col_names = {
  'ptid': 'upload_5q0_data_id',
  'mort': 'adjust_mean',
  'adjre_fe': 'adjust_re_fe'
}
data = data.rename(columns=new_col_names)
# Keep just the data points
data = data.loc[data['upload_5q0_data_id'].notnull()]
data = data.loc[~(data['upload_5q0_data_id'].duplicated())]
# Keep just the columns we keed
keep_cols = ['upload_5q0_data_id', 'adjust_mean', 'adjust_re_fe',
             'reference', 'variance']
return data[keep_cols]

def get_submodel_file(file_path):
  # Read in the data
  data = pd.read_csv(file_path)
# Format columns
data['year_id'] = data['year'].astype('int64')
data['viz_year'] = data['year']
data['sex_id'] = 3
data['age_group_id'] = 1
# Reshape first and second stage data logging
index_cols = ['location_id', 'year_id', 'viz_year', 'sex_id',
              'age_group_id']
data = data[index_cols + ['pred1b', 'pred2final']]
data = data.drop_duplicates()
data = pd.melt(data, id_vars=index_cols,
               value_vars=['pred1b', 'pred2final'],
               var_name="estimate_stage_name",
               value_name='mean')
# Assign estimate_stage_id
data.loc[data['estimate_stage_name'] == "pred1b", "estimate_stage_id"] = 1
data.loc[data['estimate_stage_name'] == "pred2final", "estimate_stage_id"] = 2
data["estimate_stage_id"] = data["estimate_stage_id"].astype('int64')
# Final format
keep_cols = index_cols + ['estimate_stage_id', 'mean']
return data[keep_cols]


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True, action='store',
                    help='The version_id to run')
args = parser.parse_args()
version_id = args.version_id

# Set directories
output_dir = "FILEPATH"

location_data = pd.read_csv("{}/data/04_fit_prediction_model_targets.csv".format(output_dir))

# Read in config file
config_file = "{}/5q0_{}_config.json".format(output_dir, version_id)
config = Config5q0.from_json(config_file)

stage_1_2_files = "{}/model/gpr_input.csv".format(output_dir)

# Get a list of stage 1 & 2 files and a list of GPR files not in raking
summary_data = pd.DataFrame()
summary_dir = "{}/summaries".format(output_dir)
for s in location_data.location_id.unique():
  temp = pd.read_csv("{}/{}.csv".format(summary_dir, s))
summary_data = pd.concat([temp,summary_data], sort = False)
keep_cols = ['location_id', 'year_id', 'viz_year', 'sex_id', 'age_group_id',
             'estimate_stage_id', 'mean', 'lower', 'upper']
summary_data['viz_year'] = summary_data['year_id'] + 0.5
summary_data = summary_data[keep_cols]

# Read in stage 1 & 2 model estimates
submodel_data = get_submodel_file(stage_1_2_files)

# Read in stage 1 & 2 model data for adjustments
adjustment_data = get_submodel_adjustment_file(stage_1_2_files)

# Combine all estimate together for upload
data = pd.concat([submodel_data, summary_data], sort = False)
data = data.sort_values(['location_id', 'year_id', 'estimate_stage_id'])
data.to_csv("{}/upload_estimates.csv".format(output_dir), index=False)

# Save adjustment data for upload
adjustment_data.to_csv("{}/upload_adjustments.csv".format(output_dir),
                       index=False)
