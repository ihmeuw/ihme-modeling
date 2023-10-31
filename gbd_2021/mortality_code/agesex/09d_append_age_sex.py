################################################################################
## DESCRIPTION Appends all plots together
## INPUTS diagnostic plot pdf for each location we estimate
## OUTPUTS combined plots into a single pdf

################################################################################

import os
import glob
from multiprocessing import Pool
from shutil import copyfile
import PyPDF2
import pandas as pd

from db_queries import get_location_metadata


import argparse
parser = argparse.ArgumentParser(description="Process version")
parser.add_argument("version_id", type = str)
parser.add_argument("gbd_year", type = str)
parser.add_argument("gbd_round_id", type = int)
args = parser.parse_args()
version_id = args.version_id
gbd_year = args.gbd_year
gbd_round_id = args.gbd_round_id

# Set the output file path
input_dir = "FILEPATH"

# Get the fertility location_hierarchy and subset to estimated locations
location_hierarchy = get_location_metadata(location_set_id = 82, gbd_round_id = gbd_round_id)
location_hierarchy = location_hierarchy.loc[(location_hierarchy["is_estimate"] == 1) | (location_hierarchy["ihme_loc_id"] == "GBR")]


# compile together location specific model fits into one large file for mass review
m = PyPDF2.PdfFileMerger()
for ihme_loc_id in location_hierarchy["ihme_loc_id"]:
  fp = "FILEPATH"
  if os.path.exists(fp):
    pdf = PyPDF2.PdfFileReader(open(fp, "rb"))
    m.append(pdf)
  else:
    print("Missing input graph for {}".format(ihme_loc_id))

m.write("FILEPATH")


parents = location_hierarchy[location_hierarchy.ihme_loc_id.str.contains("_")]["parent_id"]
parents = location_hierarchy[location_hierarchy["location_id"].isin(parents)]
parents = parents[parents["level"]==3]
parents = parents["ihme_loc_id"]
parents = parents.append(pd.Series("CHN"))

for parent in parents:
  subnats = location_hierarchy[location_hierarchy.ihme_loc_id.str.contains(parent)]

s = PyPDF2.PdfFileMerger()

for ihme_loc_id in subnats["ihme_loc_id"]:
  fp = "FILEPATH"
  if os.path.exists(fp):
    pdf = PyPDF2.PdfFileReader(open(fp, "rb"))
    s.append(pdf)
  else:
    print("Missing input graph for {}".format(ihme_loc_id))
