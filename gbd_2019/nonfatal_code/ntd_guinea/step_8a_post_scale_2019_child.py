import numpy as np
import pandas as pd
import argparse
import os
from get_draws.api import get_draws

### Parse qsub args
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


### Script Logic
def fetch_draws(meid, measures, location_id):
    draws = get_draws(source="epi",
                      gbd_id_type="modelable_entity_id",
                      gbd_id=meid,
                      measure_id=measures,
                      location_id=location_id,
                      gbd_round_id=6,
                      decomp_step="iterative",
                      version_id=ADDRESS)
    return draws


def adj_19_draws(df, location_id, year):
    year_df = df[df.year_id == year].copy().reset_index(drop=True)
    year_df["year_id"] = year
    df = df[~(df.year_id == year)]
    draw_cols = year_df.columns[year_df.columns.str.contains("draw")]

    year_df[draw_cols] = year_df[draw_cols] * 0.3
    df = df.append(year_df)
    # Set 2019 to 2017 modded values, if 2017 modded
    if (year == 2017):
        df = df[~(df.year_id == 2019)]
        year_df["year_id"] = 2019
        df = df.append(year_df)
    return df


if __name__ == "__main__":
    meids = {
        ADDRESS: [5,6]
    }


for meid, measures in meids.items():
    draws_dir = FILEPATH
    loc_draws = pd.read_csv(f"{draws_dir}/FILEPATH/{location_id}.csv")
    print(loc_draws[loc_draws.year_id==2019]["draw_990"].mean())
    new_draws = adj_19_draws(loc_draws, location_id, 2015)
    new_draws = adj_19_draws(new_draws, location_id, 2017)
    print(new_draws[new_draws.year_id==2019]["draw_990"].mean())
    new_draws.to_csv(f"{draws_dir}/{meid}/{location_id}.csv", index=False)
