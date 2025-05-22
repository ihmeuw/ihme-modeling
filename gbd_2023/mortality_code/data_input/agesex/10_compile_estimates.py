import glob
from multiprocessing import Pool

import pandas as pd

from db_queries import get_location_metadata


def read_summary_file(file_path):
    return pd.read_csv(file_path)

def get_summary_files(output_dir):
    pattern = "FILEPATH"
    summary_files = glob.glob(pattern)
    return summary_files


version_id = x
output_dir = "FILEPATH"
output_summary_estimates_file = "FILEPATH"
output_summary_change_file = "FILEPATH"

# Get the location hierarchy
location_hierarchy = get_location_metadata(location_set_id = 82, gbd_round_id = 5)


# Get a list of the summary files
summary_files = get_summary_files(output_dir)

# Multiprocess reading in summary files
pool = Pool(5)
summary_data = pool.map(read_summary_file, summary_files)
pool.close()
pool.join()

# Append all the data together
summary_data = pd.concat(summary_data)

# Merge on location_id
summary_data = pd.merge(summary_data,
                        location_hierarchy[['location_id', 'ihme_loc_id']],
                        on='ihme_loc_id', how='left')


# Save estimates
change_columns = [col for col in summary_data.columns if 'change' in col]
keep_estimate_columns = list(set(summary_data.columns) - set(change_columns))
summary_data[keep_estimate_columns].to_csv(output_summary_estimates_file, index=False)


# Save rates of change
estimate_columns = [col for col in summary_data.columns if 'q_' in col]
keep_change_columns = list(set(summary_data.columns) - set(estimate_columns))
summary_data[keep_change_columns].to_csv(output_summary_change_file, index=False)
