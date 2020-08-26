################################################################################
# Description: Create sandbox landing page to view diagnostics for the
# census processing run
# - compile together processing diagnostic graphs
# - copy over high level diagnostics like scatters
# - create landing page
################################################################################

import os
import random
import glob
from multiprocessing import Pool
from shutil import copyfile
import PyPDF2

from db_queries import get_location_metadata
from sandbox_graphs.location_specific import LocationSpecificGraphs

import sys
sys.path.insert(0, 'FILEPATH')
from call_mort_function import call_mort_function

# Get username
import getpass
USER = getpass.getuser()

import argparse
parser = argparse.ArgumentParser(description='Process version')
parser.add_argument('processing_version_id', type = str, default = "99999")
parser.add_argument('processing_version_description', type = str, nargs = "+", default = "Test version")
args = parser.parse_args()

from jinja2 import Environment, PackageLoader, select_autoescape
env = Environment(
    loader=PackageLoader('sandbox_graphs', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# Set the output file path
processing_version_id = args.processing_version_id
processing_version_description = ' '.join(args.processing_version_description)
link = "LINK PATH""
input_model_dir = "FILEPATH + processing_version_id"
output_dir = "FILEPATH + processing_version_id"
os.makedirs(output_dir, exist_ok=True)

# Get the list of locations
locations = get_location_metadata(location_set_id = 93, gbd_round_id = 6)
locations = locations.loc[locations['is_estimate'] == 1]

# compile together location specific model fits into one large file for mass review
diagnostic_type = ['baseline_pop', 'total_pop', 'age_pattern_pop']
for filetype in diagnostic_type:
  print(filetype)
  m = PyPDF2.PdfFileMerger()
  for ihme_loc_id in locations['ihme_loc_id']:
    fp = '{}/diagnostics/location_specific/{}_{}.pdf'.format(input_model_dir, ihme_loc_id, filetype)
    if os.path.exists(fp):
      pdf = PyPDF2.PdfFileReader(open(fp, 'rb'))
      m.append(pdf)
    else:
      print("Missing input graph for {}".format(ihme_loc_id))
  m.write("{}/diagnostics/compiled_{}.pdf".format(input_model_dir, filetype))
  copyfile("{}/diagnostics/compiled_{}.pdf".format(input_model_dir, filetype), "{}/compiled_{}.pdf".format(output_dir, filetype))

overall_diagnostics = ['compiled_baseline_pop', 'compiled_total_pop', 'compiled_age_pattern_pop']

def make_graph(location_id, processing_version):
    mg = LocationSpecificGraphs(location_id, version_census_processing_id = processing_version, output_dir = output_dir)
    try:
        output_file = mg.run()
        return output_file
    except:
        print("FAIL: {}".format(ihme_loc_id_dict[location_id]))
        return None

# Create location graphs
for location_id in locations['location_id']:
  print("Making location graph for: {}".format(location_id))
  make_graph(location_id, processing_version_id)

# Get a list of the files that were generated
output_files = glob.glob("{}/*.pdf".format(output_dir))
output_files = [os.path.basename(f) for f in output_files]
output_ihme_loc_ids = [o.replace(".pdf", "") for o in output_files]
output_ihme_loc_ids = list(set(output_ihme_loc_ids) & set(locations['ihme_loc_id'].tolist()))
output_ihme_loc_ids = sorted(output_ihme_loc_ids)

# Generate index.html file
output_graph_list_file = "{}/index.html".format(output_dir)
template = env.get_template('graph_list_template.html')
template.stream({'version_id': processing_version_id,
                 'version_description': processing_version_description,
                 'ihme_loc_ids': output_ihme_loc_ids,
                 'overall_diagnostics': overall_diagnostics}).dump(output_graph_list_file)
