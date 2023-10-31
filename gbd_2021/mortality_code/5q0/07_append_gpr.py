import os
import argparse
from multiprocessing import Pool

import pandas as pd

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id to run')
args = parser.parse_args()
version_id = args.version_id

# Set directories
output_dir = "FILEPATH"
model_dir = "FILEPATH"
data_dir = "FILEPATH"

# Get location_ids
location_ids = pd.read_csv("FILEPATH")
location_ids = location_ids.loc[location_ids['level'] >= 3]
location_ids = location_ids.location_id

def read_gpr_output(file_path):
    return pd.read_csv(file_path)

def get_gpr_file_path(input_dir, location_id):
    path = "FILEPATH"
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
location_file = "FILEPATH"
location_data = pd.read_csv(location_file)
data = pd.merge(data, location_data[['location_id', 'ihme_loc_id']],
                on=['ihme_loc_id'])

# Save
output_file = "FILEPATH"
data.to_csv(output_file, index=False)
