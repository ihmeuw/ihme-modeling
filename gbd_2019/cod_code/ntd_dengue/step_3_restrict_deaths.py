import pandas as pd
import argparse
import os
from get_draws.api import get_draws


parser = argparse.ArgumentParser()
parser.add_argument("--params_dir", help="Directory containing draws", type=str)
parser.add_argument("--draws_dir", help="Directory containing draws", type=str)
parser.add_argument("--interms_dir", help="Directory containing draws", type=str)
parser.add_argument("--logs_dir", help="Directory containing draws", type=str)
args = vars(parser.parse_args())

params_dir = args["params_dir"]
draws_dir = args["draws_dir"]
interms_dir = args["interms_dir"]
logs_dir = args["logs_dir"]

# Pull draws from codem
male_draws = get_draws(gbd_id_type='cause_id', gbd_id=ADDRESS, source='codem', version_id=ADDRESS, gbd_round_id=6, decomp_step='iterative')
female_draws = get_draws(gbd_id_type='cause_id', gbd_id=ADDRESS, source='codem', version_id=ADDRESS, gbd_round_id=6, decomp_step='iterative')
all_draws = male_draws.append(female_draws)
del female_draws
del male_draws

if not os.path.exists(f"{draws_dir}/deaths"):
    os.mkdir(f"{draws_dir}/deaths")

# Save 0 draws or codem draws based on endemicity file
dengue_lgr = pd.read_csv(f"FILEPATH")
dengue_lgr = dengue_lgr[dengue_lgr.year_start == 2017]

draw_cols = all_draws.columns[all_draws.columns.str.contains("draw")]
loc_draws = all_draws[all_draws.location_id == 118].copy()
loc_draws[draw_cols] = 0.0
for loc_id in dengue_lgr[dengue_lgr.value_endemicity == 0]["location_id"].unique():
    loc_draws['location_id'] = loc_id
    loc_draws.to_csv(f"FILEPATH")

for loc_id in dengue_lgr[dengue_lgr.value_endemicity == 1]["location_id"].unique():
    loc_draws = all_draws[all_draws.location_id == loc_id].copy()
    loc_draws.to_csv(f"FILEPATH")