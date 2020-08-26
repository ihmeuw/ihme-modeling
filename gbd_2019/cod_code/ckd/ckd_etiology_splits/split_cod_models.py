### Quick split cod model
### USERNAME
### Purpose: Run split cod from python due to error in R version

import pandas as pd
import numpy as np
from split_models.split_cod import split_cod_model


path_to_map = "FILEPATH/etiology_cod_split_mapping.csv" # Update the mapping parameters
mapping = pd.read_csv(path_to_map)

source_id = mapping['source_me_id'][1]
target_ids = mapping['target_me_id'].tolist()
target_meids = mapping['proportion_me_id'].tolist()
gbd_round = mapping['gbd_round_id'][1]
step = mapping['decomp_step'][1]
project = mapping['project'][1]
directory_to_save = mapping['directory_to_save'][1]



df = split_cod_model(source_cause_id = source_id,
                    target_cause_ids = target_ids,
                        target_meids = target_meids,
                             project = project,
                        gbd_round_id = gbd_round,
                         decomp_step = step)