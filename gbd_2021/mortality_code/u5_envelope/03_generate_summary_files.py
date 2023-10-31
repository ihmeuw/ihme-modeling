import os
import math
import datetime
import argparse
import re

import pandas as pd
import numpy as np
from sklearn import linear_model
from core_maths.summarize import get_summary

from gbdu5.io import get_sex_data, read_empirical_life_tables, get_finalizer_draws
from gbdu5.rescale import rescale_qx_conditional, convert_qx_to_days
from gbdu5.age_groups import MortAgeGroup, bin_five_year_ages, btime_to_wk
from gbdu5.transformations import reshape_wide, ready_to_merge, calculate_annualized_pct_change, back_calculate

"""
This script is used to generate summary files for each location
"""

# Parse arguments
parser=argparse.ArgumentParser()
parser.add_argument('--location_id', type=int, required=True,
                    action='store', help='location_id')
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='version_id')
parser.add_argument('--conda_env', type = str)

args = parser.parse_args()
location_id = args.location_id
version_id = args.version_id
conda_env = args.conda_env

# Need to manually specify to use wrappers
os.environ['R_HOME'] = 'FILEPATH'.format(conda_env)
os.environ['R_USER'] = 'FILEPATH'.format(conda_env)

from mort_wrappers.call_mort_function import call_mort_function

# create summary files
input_dir = "FILEPATH"
output_dir = "FILEPATH"
output_file = "FILEPATH"

os.makedirs(output_dir, exist_ok=True)

def reshape_wide(data, index_columns, data_columns, reshape_column):
  # Get reshape IDs
  reshape_ids = data[reshape_column].drop_duplicates().tolist()
  # Cycle through and create slices for each of the reshape IDs
  data_reshaped = []
  for i in reshape_ids:
    keep_columns = index_columns + data_columns
    temp = data.loc[data[reshape_column]==i, keep_columns].copy(deep=True)
    nc = {c: "{}_{}".format(c, i) for c in data_columns}
    temp = temp.rename(columns=nc).set_index(index_columns)
    data_reshaped.append(temp)
  data_reshaped = pd.concat(data_reshaped, axis=1).reset_index()
  # Return data
  return data_reshaped

# Get input file
data = pd.read_csv("FILEPATH")

# Reshape metric-age long
index_cols = ['ihme_loc_id', 'year', 'sex']
data_cols = [col for col in data.columns.tolist() if re.match("^pys", col)]
data = data[index_cols + ['sim'] + data_cols]
data = pd.melt(data, id_vars=(index_cols + ['sim']), value_vars=data_cols, var_name="age_group", value_name='draw')

# Reshape draws wide
data = reshape_wide(data, index_cols + ['age_group'], ['draw'], 'sim')
data = data.sort_values(['ihme_loc_id', 'year', 'sex', 'age_group']).reset_index(drop=True)
data['age_group'] = data['age_group'].map(lambda x: x.replace("pys", ""))

# Take point estimates
index_cols = ['ihme_loc_id', 'year', 'sex']
draw_cols = ['draw_{}'.format(x) for x in range(1000)]
summary_data = get_summary(data, data.filter(like='draw_').columns)
summary_data = summary_data.reset_index(drop=True)

# Reformat
summary_data['location_id'] = location_id
summary_data['year_id'] = summary_data['year'].astype('int64')
summary_data.loc[(summary_data['sex'] == "male"), 'sex_id'] = 1
summary_data.loc[(summary_data['sex'] == "female"), 'sex_id'] = 2
summary_data['sex_id'] = summary_data['sex_id'].astype('int64')
summary_data = summary_data[['location_id', 'ihme_loc_id', 'year_id', 'sex_id', 'age_group', 'mean', 'lower', 'upper']]

# Save
summary_data.to_csv(output_file, index=False)
