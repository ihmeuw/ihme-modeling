################################################################################
# Description: Create sandbox landing page to view diagnostics for the
# population model run
# - compile together location-specific population model and processing
# diagnostic graphs
# - copy over high level diagnostics like percent change heatmaps
# - create landing page
# - To run:
#   - need to activate mortality conda environment
################################################################################

import sys
import os
import glob
from multiprocessing import Pool
from shutil import copyfile
import PyPDF2
import random
import pandas as pd

from db_queries import get_location_metadata

import sys
sys.path.insert(0, "FILEPATH")
from call_mort_function import call_mort_function

from sandbox_graphs.location_specific import LocationSpecificGraphs

# Get username
import getpass
USER = getpass.getuser()

import argparse
parser = argparse.ArgumentParser(description='Process version')
parser.add_argument('model_version_id', type = str)
parser.add_argument('processing_version_id', type = str)
parser.add_argument('version_description', type = str, nargs = "+")
args = parser.parse_args()

model_version_id = args.model_version_id
processing_version_id = args.processing_version_id
version_description = ' '.join(args.version_description)

from jinja2 import Environment, PackageLoader, select_autoescape
env = Environment(
    loader=PackageLoader('sandbox_graphs', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# Set the output file path
link = "LINK".format(USER, model_version_id)
input_model_dir = "FILEPATH".format(model_version_id)
output_dir = "FILEPATH".format(USER, model_version_id)
os.makedirs(output_dir, exist_ok=True)

# Get the population location_hierarchy and subset to estimated locations
location_hierarchy = pd.read_csv(input_model_dir + "/database/all_location_hierarchies.csv")
location_hierarchy = location_hierarchy.loc[location_hierarchy['is_estimate'] == 1]

def make_graph(ihme_loc_id, model_version, processing_version):
    mg = LocationSpecificGraphs(ihme_loc_id, version_pop_id = model_version, version_census_processing_id = processing_version, output_dir = output_dir)
    try:
        output_file = mg.run()
        return output_file
    except:
        print("FAIL: {}".format(ihme_loc_id))
        return None


# compile together location specific model fits into one large file for mass review
m = PyPDF2.PdfFileMerger()
bad_loc_counter = 0

for ihme_loc_id in location_hierarchy['ihme_loc_id']:

  fp = "FILEPATH".format(v = model_version_id, l = ihme_loc_id)

  try:
    if os.path.getsize(fp) > 0:
      with open(fp, 'rb') as input:
        pdf = PyPDF2.PdfFileReader(input)
        m.append(pdf)
    else:
      bad_loc_counter += 1
      print("Input graph for {} is empty".format(ihme_loc_id))
  except OSError as e:
    bad_loc_counter += 1
    print("Input graph for {} is missing".format(ihme_loc_id))

if bad_loc_counter > 0:
  sys.exit("{} location input graphs were either empty or missing, check output".format(bad_loc_counter))
else:
  m.write("{}/diagnostics/compiled_model_fit.pdf".format(input_model_dir))
  
# Compile just the total pop over time graphs
m = PyPDF2.PdfFileMerger()
bad_loc_counter = 0

for ihme_loc_id in location_hierarchy['ihme_loc_id']:

  fp = "FILEPATH".format(v = model_version_id, l = ihme_loc_id)

  try:
    if os.path.getsize(fp) > 0:
      with open(fp, 'rb') as input:
        pdf = PyPDF2.PdfFileReader(input)
        m.append(pdf)
    else:
      bad_loc_counter += 1
      print("Input graph for {} is empty".format(ihme_loc_id))
  except OSError as e:
    bad_loc_counter += 1
    print("Input graph for {} is missing".format(ihme_loc_id))

if bad_loc_counter > 0:
  sys.exit("{} location input graphs were either empty or missing, check output".format(bad_loc_counter))
else:
  m.write("{}/diagnostics/compiled_total_pop.pdf".format(input_model_dir))
  
# Compile just the total pop over time graphs
m = PyPDF2.PdfFileMerger()
bad_loc_counter = 0

for ihme_loc_id in location_hierarchy['ihme_loc_id']:

  fp = "FILEPATH"v.format(v = model_version_id, l = ihme_loc_id)

  try:
    if os.path.getsize(fp) > 0:
      with open(fp, 'rb') as input:
        pdf = PyPDF2.PdfFileReader(input)
        m.append(pdf, pages = (0, 1))
    else:
      bad_loc_counter += 1
      print("Input graph for {} is empty".format(ihme_loc_id))
  except OSError as e:
    bad_loc_counter += 1
    print("Input graph for {} is missing".format(ihme_loc_id))

if bad_loc_counter > 0:
  sys.exit("{} location input graphs were either empty or missing, check output".format(bad_loc_counter))
else:
  m.write("{}/diagnostics/compiled_total_pop.pdf".format(input_model_dir))

# Compile pop by age and sex over time
m = PyPDF2.PdfFileMerger()
bad_loc_counter = 0

for ihme_loc_id in location_hierarchy['ihme_loc_id']:

  fp = "FILEPATH".format(v = model_version_id, l = ihme_loc_id)

  try:
    if os.path.getsize(fp) > 0:
      with open(fp, 'rb') as input:
        pdf = PyPDF2.PdfFileReader(input)
        m.append(pdf, pages = (1, 3))
    else:
      bad_loc_counter += 1
      print("Input graph for {} is empty".format(ihme_loc_id))
  except OSError as e:
    bad_loc_counter += 1
    print("Input graph for {} is missing".format(ihme_loc_id))

if bad_loc_counter > 0:
  sys.exit("{} location input graphs were either empty or missing, check output".format(bad_loc_counter))
else:
  m.write("{}/diagnostics/compiled_pop_by_age_sex.pdf".format(input_model_dir))


# Create location specific graphs including modeling and processing graphs
for ihme_loc_id in location_hierarchy['ihme_loc_id']:
  print("Making location graph for: {}".format(ihme_loc_id))
  make_graph(ihme_loc_id, model_version_id, processing_version_id)

# Copy overall diagnostics (pct_change_heatmap)
overall_diagnostics = ["compiled_model_fit", "pct_change_heatmap_current", "pct_change_heatmap_previous", "pct_change_heatmap_WPP", "compiled_pop_by_age_sex", "compiled_total_pop"]
for diagnostic in overall_diagnostics:
  copyfile("{}/diagnostics/{}.pdf".format(input_model_dir, diagnostic), "{}/{}.pdf".format(output_dir, diagnostic))

# Get a list of the files that were generated
output_files = glob.glob("{}/*.pdf".format(output_dir))
output_ihme_loc_ids = [o.replace("{}/".format(output_dir), "").replace(".pdf", "") for o in output_files]
output_ihme_loc_ids = sorted(output_ihme_loc_ids)

# Generate index.html file
output_graph_list_file = "{}/index.html".format(output_dir)
template = env.get_template('graph_list_template.html')
template.stream({'version_id': model_version_id,
                 'version_description': version_description,
                 'ihme_loc_ids': output_ihme_loc_ids,
                 'overall_diagnostics': overall_diagnostics}).dump(output_graph_list_file)

