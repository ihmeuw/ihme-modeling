### Split cod model using central function

# Python Code:
import pandas as pd
import numpy as np
from split_models import split_cod_model

source_id = 589
target_ids = [592,591,593,997,998]
target_meids = [27977,27978,27979,18725,18726]
project = 'proj_birds'

description = 'codem_refresh3_drop_fpg_dm_cov'
output_path = 'FILEPATH'
release_id = 16
out_dir = f'{output_path}/{release_id}/{description}'

print('Running splits ...')
df = split_cod_model(source_cause_id = source_id,
                    target_cause_ids = target_ids,
                    target_meids = target_meids,
                    project = project,
                    prop_meas_id = 18,
                    release_id = release_id,
                    output_dir = out_dir)
                    
print(f'Split completed and results are in {out_dir}')