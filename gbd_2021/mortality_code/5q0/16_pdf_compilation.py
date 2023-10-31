## Python script to compile together change scatters and regular diagnostic plots

import PyPDF2
import argparse
import sys
import pandas as pd

parser = argparse.ArgumentParser(description='Process version')
parser.add_argument('--version_id', type = int)
parser.add_argument('--gbd_year', type = int)
parser.add_argument('--conda_env', type = str)
args = parser.parse_args()
version_id = args.version_id
gbd_year = args.gbd_year
conda_env = args.conda_env

import os
os.environ['R_HOME'] = 'FILEPATH'.format(conda_env)
os.environ['R_USER'] = 'FILEPATH'.format(conda_env)

# Set filepaths
dir_5q0 = "FILEPATH"
scatter_file = "FILEPATH"
location_dir = "FILEPATH"

change_scatter = PyPDF2.PdfFileReader(scatter_file)

# Pull the location hierarchy
locations = pd.read_csv("FILEPATH")
locations = locations.sort_values(by=["super_region_name", "region_name", "path_to_top_parent"])
ihme_loc_ids = list(locations.loc[locations.level >= 3, 'ihme_loc_id'])


# Outer loop: Do this once for year starting at 1950, once for year starting 1970

for yr in [1950, 1970]:

  # Inner loop: compile all location specific directories + change scatters
  m = PyPDF2.PdfFileMerger()
  m.append(change_scatter)

  for loc in ihme_loc_ids:
    # Iteratively add location specific pdfs together
    loc_specific_pdf = PyPDF2.PdfFileReader(open("FILEPATH".format(loc_dir = location_dir, yr=yr, loc=loc, version_id = version_id), 'rb'))
    m.append(loc_specific_pdf)

  # Set output file name
  output_file = "FILEPATH".format(graph_dir = dir_5q0, year=yr, version=version_id)

  # Write out the final file
  m.write(output_file)

  # End loop

