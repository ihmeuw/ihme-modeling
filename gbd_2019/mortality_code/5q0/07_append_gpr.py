import os
import argparse
from multiprocessing import Pool

import pandas as pd

def read_gpr_output(file_path):
  return pd.read_csv(file_path)

def get_gpr_file_path(input_dir, location_id):
  path = "{}/gpr_{}.csv".format(input_dir, location_id)
return path

def check_files(file_list):
  missing_files = []
for f in file_list:
  if not os.path.exists(f):
  missing_files.append(f)
if missing_files:
  msg = ["The following files are missing:"] + missing_files
raise FileNotFoundError("\n".join(msg))

def check_missing_data(location_ids, year_start, year_end, draws):
  pass


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id to run')
parser.add_argument('--location_ids', type=int, required=True, nargs='+',
                    action='store', help='The location_ids to append to run')
args = parser.parse_args()
version_id = args.version_id
location_ids = args.location_ids

# Set directories
output_dir = "FILEPATH"
model_dir = "{}/model".format(output_dir)
data_dir = "{}/data".format(output_dir)

# Get list of files to read in
gpr_files = [get_gpr_file_path(model_dir, l) for l in location_ids]

# Check that all files exist
check_files(gpr_files)

# Read in data and append together
pool = Pool(5)
data = pool.map(read_gpr_output, gpr_files)
pool.close()
pool.join()
data = pd.concat(data)

# Merge on locations
location_file = "{}/location.csv".format(data_dir)
location_data = pd.read_csv(location_file)
data = pd.merge(data, location_data[['location_id', 'ihme_loc_id']],
                on=['ihme_loc_id'])

# Save
output_file = "{}/gpr_compiled.csv".format(model_dir)
data.to_csv(output_file, index=False)
