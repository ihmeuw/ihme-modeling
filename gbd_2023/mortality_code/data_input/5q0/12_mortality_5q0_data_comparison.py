import os
import argparse
import getpass
import pandas as pd
import numpy as np

from gbd5q0py.config import Config5q0

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The current version_id for 5q0')
parser.add_argument('--gbd_year',type=int,required=True, action='store',
                    help= 'GBD Year')
parser.add_argument('--end_year', type=int, required=True, action='store',
                    help='last year we produce estimates for')
parser.add_argument('--code_dir', type=str, required=True, action='store',
                    help='Directory where child-mortality code is cloned')
parser.add_argument('--conda_env', type=str,
                    action='store',
                    help = 'Conda environment to use for py wrapper')
parser.add_argument('--gbd_round_id', type=int,
                    action='store',
                    help = 'GBD round ID')
parser.add_argument('--best_old_version_id', type=int,
                    action='store',
                    help = 'Previous round best 5q0 estimate')
parser.add_argument('--pre_gbd_year', type=int,
                    action='store',
                    help = 'Previous GBD round ID')

args = parser.parse_args()
version_id = args.version_id
gbd_year = args.gbd_year
end_year = args.end_year
code_dir = args.code_dir
conda_env = args.conda_env
gbd_round_id= args.gbd_round_id
pre_gbd_year= args.pre_gbd_year
best_old_version_id = args.best_old_version_id


# Need to manually specify to use wrappers
import os
os.environ['R_HOME'] = "FILEPATH"
os.environ['R_USER'] = "FILEPATH"

from mort_wrappers.call_mort_function import call_mort_function

def merge_gpr(data, gpr_data):
    keep_gpr_cols = ['ihme_loc_id', 'year', 'source1', 'ptid',
                     'logit_var']
    gpr_data = gpr_data.loc[gpr_data['data'] == 1, keep_gpr_cols]
    data.loc[data['ptid'].notnull(), 'ptid'] = data.loc[data['ptid'].notnull(), 'ptid']
    gpr_data.loc[gpr_data['ptid'].notnull(), 'ptid'] = gpr_data.loc[gpr_data['ptid'].notnull(), 'ptid']
    data = pd.merge(data, gpr_data,
                    on=['ihme_loc_id', 'year', 'source1', 'ptid'],
                    how='outer')
    return data


def percent_diff(current, previous, absolute_value=False):
    return((current - previous) / previous)


# Read in config file
output_dir = "FILEPATH"
output_dir_old= "FILEPATH"

config_file_old = "FILEPATH"
config_old = Config5q0.from_json(config_file_old)

config_file_current = "FILEPATH"
config_current = Config5q0.from_json(config_file_current)

location_data = pd.read_csv("FILEPATH")
location_data_old = pd.read_csv("FILEPATH")


# Generate comparison folder
comparison_output_dir = "FILEPATH"
os.makedirs(comparison_output_dir, exist_ok=True)


# Generate dictionary to rename some columns
new_names = {
    'mort': 'adjusted_5q0',
    'mort2': 'raw_5q0',
    'adjre_fe': 'source_adjustment',
    'log10.sd.q5': 'log10_sd_q5'
}

# Import GBD 2020
gbd_current_output_dir = "FILEPATH"
gbdcurrent_scurrent_file = "FILEPATH"
gbdcurrent_scurrent_gpr_file = "FILEPATH"
gbdcurrent_scurrent = pd.read_csv(gbdcurrent_scurrent_file)
gbdcurrent_scurrent_gpr = pd.read_csv(gbdcurrent_scurrent_gpr_file)
gbdcurrent_scurrent = merge_gpr(gbdcurrent_scurrent, gbdcurrent_scurrent_gpr)
gbdcurrent_all = gbdcurrent_scurrent.loc[gbdcurrent_scurrent['location_id'].isin(location_data.location_id)].copy(deep=True)
gbdcurrent_all['variance'] = gbdcurrent_all['logit_var'] / ((1/(gbdcurrent_all['mort'] * (1 - gbdcurrent_all['mort'])))**2)


# Import GBD 2019
gbd_old_output_dir = "FILEPATH"
gbdold_sold_file = "FILEPATH"
gbdold_sold_gpr_file = "FILEPATH"
gbdold_sold = pd.read_csv(gbdold_sold_file)
gbdold_sold_gpr = pd.read_csv(gbdold_sold_gpr_file)
gbdold_sold = merge_gpr(gbdold_sold, gbdold_sold_gpr)
gbdold_all = gbdold_sold.loc[gbdold_sold['location_id'].isin(location_data_old.location_id)].copy(deep=True)
gbdold_all['variance'] = gbdold_all['logit_var'] / ((1/(gbdold_all['mort'] * (1 - gbdold_all['mort'])))**2)



# Get reference groups
keep_ref_cols = ['ihme_loc_id', 'year', 'source.type', 'source', 'data', 'mort',
                 'mort2', 'reference', 'source1', 'graphing.source', 'adjre_fe',
                 'mse', 'log10.sd.q5', 'variance']




gbdcurrent_all_ref = gbdcurrent_all.loc[gbdcurrent_all['data'] == 1].copy(deep=True)
gbdcurrent_all_ref = gbdcurrent_all_ref[keep_ref_cols]
gbdcurrent_all_ref = gbdcurrent_all_ref.rename(columns=new_names)
gbdcurrent_all_ref['source'] = gbdcurrent_all_ref['source'].str.lower()
gbdcurrent_all_ref['source1'] = gbdcurrent_all_ref['source1'].str.lower()

gbdold_all_ref = gbdold_all.loc[gbdold_all['data'] == 1].copy(deep=True)
gbdold_all_ref = gbdold_all_ref[keep_ref_cols]
gbdold_all_ref = gbdold_all_ref.rename(columns=new_names)
gbdold_all_ref['source'] = gbdold_all_ref['source'].str.lower()
gbdold_all_ref['source1'] = gbdold_all_ref['source1'].str.lower()


compare_ref = pd.merge(gbdcurrent_all_ref, gbdold_all_ref,
                       on=['ihme_loc_id', 'year', 'source.type', 'source', 'source1', 'data'],
                       how='outer', suffixes=['_{year}'.format(year=gbd_year), '_{pre}'.format(pre=pre_gbd_year)])
compare_ref = compare_ref.sort_values(['ihme_loc_id', 'year', 'source.type'])

compare_ref = compare_ref.loc[~(compare_ref['ihme_loc_id'].str.contains("SAU_"))]

# Generate comparison column for reference group
compare_ref.loc[compare_ref['reference_{year}'.format(year=gbd_year)] == compare_ref['reference_{pre}'.format(pre=pre_gbd_year)], 'same_reference'] = 1

# Format reference comparison
ref_order_cols = ['ihme_loc_id', 'year', 'source.type', 'source', 'data',
                  'source1', 'raw_5q0_{year}'.format(year=gbd_year), 'adjusted_5q0_{year}'.format(year=gbd_year),
                  'reference_{year}'.format(year=gbd_year), 'graphing.source_{year}'.format(year=gbd_year),
                  'source_adjustment_{year}'.format(year=gbd_year), 'mse_{year}'.format(year=gbd_year), 'log10_sd_q5_{year}'.format(year=gbd_year),
                  'variance_{year}'.format(year=gbd_year), 'raw_5q0_{pre}'.format(pre=pre_gbd_year), 'adjusted_5q0_{pre}'.format(pre=pre_gbd_year),
                  'reference_{pre}'.format(pre=pre_gbd_year), 'graphing.source_{pre}'.format(pre=pre_gbd_year),
                  'source_adjustment_{pre}'.format(pre=pre_gbd_year), 'mse_{pre}'.format(pre=pre_gbd_year), 'log10_sd_q5_{pre}'.format(pre=pre_gbd_year),
                  'variance_{pre}'.format(pre=pre_gbd_year), 'same_reference']
compare_ref = compare_ref[ref_order_cols]

# Save
compare_ref.to_csv("FILEPATH", index=False)
for l in compare_ref['ihme_loc_id'].drop_duplicates():
    save_file_path = "FILEPATH"
    compare_ref.loc[compare_ref['ihme_loc_id'] == l].to_csv(save_file_path, index=False)
