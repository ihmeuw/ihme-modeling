import os
import glob
import pandas as pd
from multiprocessing import Pool
from shutil import copyfile
import PyPDF2

from db_queries import get_location_metadata


import argparse
parser = argparse.ArgumentParser(description='Process version')
parser.add_argument('--model_version_id', type = str)
parser.add_argument('--gbd_year', type = str)
args = parser.parse_args()
model_version_id = args.model_version_id
gbd_year = args.gbd_year

# Set the output file path
input_model_dir = "FILEPATH"" + "/loc_specific"

# Get the fertility location_hierarchy and subset to estimated locations
location_hierarchy = pd.read_csv("FILEPATH/model_locs.csv")

# compile together location specific model fits into one large file for mass review
m = PyPDF2.PdfFileMerger()
for ihme_loc_id in location_hierarchy['ihme_loc_id']:
  fp = "FILEPATH' + ihme_loc_id + '_' + model_version_id + '.pdf'
  if os.path.exists(fp):
    pdf = PyPDF2.PdfFileReader(open(fp, 'rb'))
    m.append(pdf)
  else:
    print("Missing input graph for {}".format(ihme_loc_id))

m.write(input_model_dir + '/FILEPATH/plots_' + model_version_id + '_appended.pdf')


parents = location_hierarchy[location_hierarchy.ihme_loc_id.str.contains('_')]['parent_id']
parents = location_hierarchy[location_hierarchy['location_id'].isin(parents)]
parents = parents[parents['level']==3]
parents = parents['ihme_loc_id']
parents.append(pd.Series('CHN'))

for parent in parents:
  subnats = location_hierarchy[location_hierarchy.ihme_loc_id.str.contains(parent)]

s = PyPDF2.PdfFileMerger()

for ihme_loc_id in subnats['ihme_loc_id']:
  fp = "FILEPATH/' + ihme_loc_id + '_' + model_version_id + '.pdf'
  if os.path.exists(fp):
    pdf = PyPDF2.PdfFileReader(open(fp, 'rb'))
    s.append(pdf)
  else:
    print("Missing input graph for {}".format(ihme_loc_id))

s.write(input_model_dir + '/FILEPATH/plots_' + model_version_id + '_' + parent + '.pdf')
