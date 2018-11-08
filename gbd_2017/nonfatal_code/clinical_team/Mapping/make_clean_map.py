import pandas as pd
import numpy as np


# Update every new iteration
last_map = 18

path = "FILEPATH"
maps = pd.read_csv(path + "/clean_map_{}.csv".format(last_map)) 

maps['map_version'] = last_map + 1

# Dropping all mappings except two cause code mappings of 116
keep = ['4329', 'I629']

# fix the baby sequelae and mapping for bundle 116
# first fix the mapping
maps.loc[maps.bundle_id == 116, 'bundle_id'] = np.nan
maps.loc[(maps.cause_code.isin(keep)) & (maps.level == 2), 'bundle_id'] = 116
# now create a new baby sequelae for these 2 icd codes
maps.loc[maps.cause_code.isin(keep), 'nonfatal_cause_name'] =\
    maps.loc[maps.cause_code.isin(keep), 'nonfatal_cause_name'] + '_special_to_split'

# fix the baby sequelae for bundle 206
maps.loc[maps.cause_code == '28244', 'nonfatal_cause_name'] = 'hemog, beta-thal major_icd9'


maps.to_csv(path + "/clean_map_{}.csv".format(last_map + 1), index = False)
