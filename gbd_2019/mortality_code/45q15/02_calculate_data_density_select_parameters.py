import sys
import getpass
import argparse
import pandas as pd

sys.path.insert(0, 'FILEPATH')
from call_mort_function import call_mort_function

parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help="Version id of 45q15 model run")
parser.add_argument('--ddm_estimate_version', type=str, required=True,
                    action='store', help="DDM estimate version")

args = parser.parse_args()
version_45q15_id = args.version_id
version_ddm_id = args.ddm_estimate_version

if (version_ddm_id == "best") | (version_ddm_id == "recent"):
  version_ddm_id = call_mort_function("get_proc_version", {"model_name" : "ddm", "model_type" : "estimate", "run_id" : version_ddm_id})
else:
  version_ddm_id = int(version_ddm_id)

output_ddm_dir = "FILEPATH"
output_45q15_dir = "FILEPATH"
location_file = "{}/data/locations.csv".format(output_45q15_dir)

code_dir = "FILEPATH"

# Get location hierarchy
location_hierarchy = pd.read_csv(location_file)

# Get 45q15 input
data_45q15 = pd.read_csv("{}/data/input_data.csv".format(output_45q15_dir))

# Save a copy of non-sibling history data
data_45q15_non_sibs = data_45q15.loc[(data_45q15['data'] == 1) & (data_45q15['category'] != "sibs")].copy(deep=True)
data_45q15_non_sibs['year'] = data_45q15_non_sibs['year'] - 0.5
data_45q15_non_sibs['year'] = data_45q15_non_sibs['year'].astype('int64')

# Get sibling histories
data_45q15 = data_45q15.loc[(data_45q15['data'] == 1) & (data_45q15['category'] == "sibs")]
data_45q15['year'] = data_45q15['year'].astype('int64')

# Set sibling histories to a count of 25 deaths
data_45q15['45q15_deaths'] = 25

# Get DDM output
raw_data = pd.read_stata("{}/data/d10_45q15.dta".format(output_ddm_dir))
data = raw_data.copy(deep=True)

# Get a list of data points that are present in the 45q15 data
keep_45q15_non_sibs_cols = ['ihme_loc_id', 'sex', 'year', 'source_type']
data_45q15_non_sibs = data_45q15_non_sibs[keep_45q15_non_sibs_cols].drop_duplicates()
data_45q15_non_sibs['source_type'] = data_45q15_non_sibs['source_type'].str.upper()
data_45q15_non_sibs['keep'] = 1

# Merge on 45q15 (non-outliered points to DDM output)
data = pd.merge(data, data_45q15_non_sibs,
                on=['ihme_loc_id', 'year', 'sex', 'source_type'], how='left')
data = data.loc[(data['keep'] == 1)]


# Create column for total number of observed deaths for 15 to 60 year olds
keep_45q15_cols = ['vr_15to19', 'vr_20to24', 'vr_25to29', 'vr_30to34',
                   'vr_35to39', 'vr_40to44', 'vr_45to49', 'vr_50to54',
                   'vr_55to59']
keep_cols = ['ihme_loc_id', 'source_type', 'sex', 'year', 'deaths_source',
             'obs45q15', 'adj45q15'] + keep_45q15_cols
data['45q15_deaths'] = (
  data['vr_15to19'] + data['vr_20to24'] + data['vr_25to29'] + data['vr_30to34'] + data['vr_35to39'] +
    data['vr_40to44'] + data['vr_45to49'] + data['vr_50to54'] + data['vr_55to59'])
data['45q15_deaths'] = data['45q15_deaths'].fillna(0)

# Combine sibling history and other data
data = pd.concat([data, data_45q15], sort = False)

# Sum up number of observed deaths by location, sex, and year
data = data.groupby(['ihme_loc_id', 'sex', 'year'])['45q15_deaths'].sum().reset_index()

# Cap deaths at 1000
data.loc[data['45q15_deaths'] >= 1000, '45q15_deaths'] = 1000

# Divide by 1000
data['45q15_deaths'] = data['45q15_deaths'] / 1000

# Add up deaths by location and sex
data = data.groupby(['ihme_loc_id', 'sex'])['45q15_deaths'].sum().reset_index()

# Rename to data density
data = data.rename(columns={'45q15_deaths': 'data_density'})

# Merge on locations
location_hierarchy = location_hierarchy.loc[(location_hierarchy['level'] >= 3) |
                                              (location_hierarchy['level'] == -1)]
location_square = []
for sex in ['male', 'female']:
  temp = location_hierarchy[['location_id', 'ihme_loc_id']].copy(deep=True)
temp['sex'] = sex
location_square.append(temp)
location_square = pd.concat(location_square)
data = pd.merge(location_square, data, on=['ihme_loc_id', 'sex'], how='left')
data.loc[data['data_density'].isnull(), 'data_density'] = 0

# Make hyperparameter categories
data.loc[data['data_density'] < 10, 'data_density_category'] = "0_to_10"
data.loc[(data['data_density'] >= 10) &
           (data['data_density'] < 20), 'data_density_category'] = "10_to_20"
data.loc[(data['data_density'] >= 20) &
           (data['data_density'] < 30), 'data_density_category'] = "20_to_30"
data.loc[(data['data_density'] >= 30) &
           (data['data_density'] < 50), 'data_density_category'] = "30_to_50"
data.loc[(data['data_density'] >= 50), 'data_density_category'] = "50_plus"

# Merge on hyperparameters
hyperparameters = pd.read_csv("{}/45q15_hyperparameter_values.csv".format(code_dir))
data = pd.merge(data, hyperparameters, on='data_density_category', how='left')

# Add in legacy variables
data['best'] = 1
data['amp2x'] = 1

# Manually set some locations
data.loc[(data['ihme_loc_id'] == "DOM"), "lambda"] = 0.6
data.loc[(data['ihme_loc_id'] == "JOR"), "lambda"] = 0.5
data.loc[(data['ihme_loc_id'] == "PAK"), "lambda"] = 0.8
data.loc[(data['ihme_loc_id'] == "IRN"), "lambda"] = 0.3

# Save
data.to_csv("{}/data/calculated_data_density.csv".format(output_45q15_dir), index=False)
