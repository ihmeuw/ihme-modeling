import os
import glob
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help="Version id of age-sex model run")
parser.add_argument('--hiv_version', type=str, required=True,
                    action='store', help="Version id to pull HIV files")

args = parser.parse_args()
version_id = args.version_id
hiv_version_id = args.hiv_version

# Set input and output directories
hiv_input_dir = "FILEPATH"
output_dir = "FILEPATH"

# Read in locations with HIV information
hiv_location_information = pd.read_csv("{}/hiv_location_information.csv".format(output_dir))
hiv_group_1a_locations = hiv_location_information.loc[(hiv_location_information['group'] == "1A"), 'ihme_loc_id'].tolist()


# Read in HIV files
data = []
for ihme_loc_id in hiv_group_1a_locations:
  file_path = "{}/{}.csv".format(hiv_input_dir, ihme_loc_id)
temp = pd.read_csv(file_path)
temp = temp.loc[(temp['age'] == 0)]
temp = temp.rename(columns={'year': 'year_id'})
data.append(temp)

data = pd.concat(data)


# Calculate HIV-free ratio
data['hiv_free_ratio'] = (data['all_cause_mort_rate'] - data['hiv_mort_rate']) / data['all_cause_mort_rate']

# Save
keep_cols = ['ihme_loc_id', 'year_id', 'sex', 'age', 'hiv_free_ratio']
data[keep_cols].to_csv("{}/hiv_free_ratios.csv".format(output_dir), index=False)
