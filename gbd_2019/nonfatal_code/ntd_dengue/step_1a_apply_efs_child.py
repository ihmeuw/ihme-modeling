import pandas as pd
import argparse
import os
from get_draws.api import get_draws


parser = argparse.ArgumentParser()
parser.add_argument("--params_dir", help="Directory containing params", type=str)
parser.add_argument("--draws_dir", help="Directory containing draws", type=str)
parser.add_argument("--interms_dir", help="Directory containing interms", type=str)
parser.add_argument("--logs_dir", help="Directory containing logs", type=str)
parser.add_argument("--location_id", help="Location id to run for", type=str)
args = vars(parser.parse_args())

params_dir = args["params_dir"]
draws_dir = args["draws_dir"]
interms_dir = args["interms_dir"]
logs_dir = args["logs_dir"]
location_id = args["location_id"]

# Forward endemic draws from ST-GPR to parallel save location
draws = pd.read_csv(f"FILEPATH")
draws["measure_id"] = 6
draws["metric_id"] = 3
draws["modelable_entity_id"] = ADDRESS

draw_cols = draws.columns[draws.columns.str.contains("draw")]
draws = draws[~draws.age_group_id.isin([2,3,4])].reset_index(drop=True)
age_5 = draws[draws.age_group_id == 5].copy().reset_index(drop=True)
age_2 = age_5.copy()
age_3 = age_5.copy()
age_4 = age_5.copy()
age_2['age_group_id'] = 2
age_2[draw_cols] = age_2[draw_cols] * 0.0
age_3['age_group_id'] = 3
age_4['age_group_id'] = 4

draws = draws.append(age_2)
draws = draws.append(age_3)
draws = draws.append(age_4)

draws.to_csv(f"FILEPATH")
