import pandas as pd
import argparse
import os
from ADDRESS import get_draws


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

prev_df = pd.read_csv(f"FILEPATH")
prev_df["metric_id"] = 3
prev_df["modelable_entity_id"] = ADDRESS

# Maintain highest estimates
prev_df["sort_col"] = prev_df["draw_500"].abs()
prev_df = prev_df.sort_values("sort_col").drop_duplicates(subset=["age_group_id", "year_id", "sex_id", "measure_id"], keep='last')
draw_cols = prev_df.columns[prev_df.columns.str.contains("draw")]
# prev_df[(prev_df[draw_cols] < 0)] = 0
prev_df = prev_df.drop(["sort_col"], axis=1)

# Add missing 0 draws for new age_group_id == ADDRESS
age_fix = prev_df[prev_df.age_group_id==ADDRESS].copy()
age_fix = age_fix[~age_fix.year_id.isin([2015, 2017, 2019])]
age_fix["age_group_id"] = ADDRESS
age_fix[draw_cols] = 0.0
prev_df = prev_df.append(age_fix)

prev_df.to_csv(f"FILEPATH")
