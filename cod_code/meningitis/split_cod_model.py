"""
Small script to use the split_cod_model shared function
"""


import os
import pandas as pd
from transmogrifier.draw_ops import split_cod_model


# load in the info table and create a me_id list
username = 'USERNAME'
work_dir = os.path.dirname(os.path.realpath(__file__))
info_df = pd.read_csv(work_dir + '/info_table.csv')
target_meid_list = info_df['me_id'].tolist()
target_cause_list = info_df['child_causes'].tolist()
parent_cause = 332


# create output directory
out_dir = "OUT_DIR".format(username)
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# plug things into shared function
output_dir = split_cod_model(
    source_cause_id=parent_cause,
    target_cause_ids=target_cause_list,
    target_meids=target_meid_list,
    output_dir=out_dir)
