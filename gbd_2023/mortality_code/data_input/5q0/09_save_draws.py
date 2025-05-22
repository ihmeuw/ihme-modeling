import os
import argparse
from multiprocessing import Pool

import pandas as pd

from core_maths.summarize import get_summary
from gbd5q0py.config import Config5q0


def make_summaries(data):
    """
    This function expects a DataFrame with the following
    columns:
        location_id
        ihme_loc_id
        year
        sim
        mort
    """
    # Input
    input_keep_cols = ['location_id', 'ihme_loc_id', 'year', 'sim', 'mort']
    data = data[input_keep_cols]
    # Format columns
    data['year_id'] = data['year'].astype('int64')
    data['sex_id'] = 3
    data['age_group_id'] = 1
    data['estimate_stage_id'] = 3
    data['sim'] = data['sim'].astype('int64')
    # Reshape draws wide
    index_cols = ['location_id', 'ihme_loc_id', 'year_id', 'year',  'sex_id',
                  'age_group_id', 'estimate_stage_id']
    data = data.pivot_table(values="mort", index=index_cols, columns="sim")
    data = data.reset_index()
    data = data.rename(columns={x: 'draw_{}'.format(x) for x in range(1000)})
    # Get the summary statistics
    draw_cols = [col for col in data.columns if 'draw' in col]
    data = get_summary(data, draw_cols)
    # Format for upload
    keep_cols = index_cols + ['mean', 'lower', 'upper']
    return data[keep_cols]


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True, action='store',
                    help='The version_id to run')
parser.add_argument('--location_id', type=int, required=True, action='store',
                    help='The version_id to run')
args = parser.parse_args()
version_id = args.version_id
location_id = args.location_id


# Set directories
output_dir = "FILEPATH"

# Read in config file
config_file = "FILEPATH"
config = Config5q0.from_json(config_file)

# Get location data
location_data = pd.read_csv("FILEPATH")

# Get a list of raked locations and files
raking_dict = {}
for r in config.rakings:
    raking_dir = "FILEPATH"
    for l in [r.parent_id] + r.child_ids:
        raking_dict[l] = "FILEPATH"
raking_files = [v for k, v in raking_dict.items()]
raking_locations = [k for k, v in raking_dict.items()]

# Get a list of GPR locations
gpr_dict = {}
for l in location_data.location_id.unique():
    gpr_dict[l] = "FILEPATH"

# Read in and format raked and GPR data
keep_cols = ['location_id', 'ihme_loc_id', 'year', 'sim', 'mort']
if location_id in raking_locations:
    data = pd.read_csv(raking_dict[location_id])
    data['year'] = data['viz_year']
    data = pd.merge(data, location_data[['location_id', 'ihme_loc_id']],
                    on=['location_id'])
else:
    data = pd.read_csv(gpr_dict[location_id])
    data = pd.merge(data, location_data[['location_id', 'ihme_loc_id']],
                    on=['ihme_loc_id'])
data = data[keep_cols]

# Save draws
output_draws_file_path = "FILEPATH"
data.to_csv(output_draws_file_path, index=False)

# Generate summaries
data = make_summaries(data)

# Save summaries
output_summary_file_path ="FILEPATH"
data.to_csv(output_summary_file_path, index=False)
