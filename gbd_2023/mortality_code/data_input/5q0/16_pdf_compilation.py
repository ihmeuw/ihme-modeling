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

# Need to manually specify to use wrappers
import os
os.environ['R_HOME'] = "FILEPATH"
os.environ['R_USER'] = "FILEPATH"

# Set filepaths
dir_5q0 = "FILEPATH"
scatter_file = "FILEPATH"
location_dir = "FILEPATH"

# Since the scatter file is invariant, load it here
change_scatter = PyPDF2.PdfFileReader(scatter_file)

# Pull the location hierarchy
locations = pd.read_csv("FILEPATH")
locations = locations.sort_values(by=["super_region_name", "region_name", "path_to_top_parent"])
ihme_loc_ids = list(locations.loc[locations.level >= 3, 'ihme_loc_id'])



for yr in [1950, 1970]:

  # Inner loop: compile all location specific directories + change scatters
  m = PyPDF2.PdfFileMerger()
  m.append(change_scatter)

  for loc in ihme_loc_ids:
    # Iteratively add location specific pdfs together
    loc_specific_pdf = PyPDF2.PdfFileReader(open("FILEPATH", 'rb'))
    m.append(loc_specific_pdf)

  # Set output file name
  output_file = "FILEPATH"

  # Write out the final file
  m.write(output_file)


