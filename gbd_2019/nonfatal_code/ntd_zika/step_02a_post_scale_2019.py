import numpy as np
import pandas as pd
import argparse
import os
from ADDRESS import get_draws

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
                      status="best")
    return draws


def get_scalar(params_dir, loc_id):
    paho_scalars = pd.read_csv(f"FILEPATH")    ## Zika scaling 2017 to 2018 for estimating 2019
    paho_scalars = paho_scalars[["location_id", "ratio_nonzero_not1"]]
    paho_scalars = paho_scalars[~paho_scalars.ratio_nonzero_not1.isna() & ~paho_scalars.location_id.isna()]
    paho_scalars["location_id"] = paho_scalars["location_id"].astype(int)
    if int(loc_id) in list(paho_scalars.location_id.unique()):
        scalar = paho_scalars.loc[paho_scalars.location_id == int(loc_id), "ratio_nonzero_not1"].values[0]
        scalar = float(scalar)
    else:
        scalar = 0.0
    if scalar > 1.0:
        scalar = 1.0
    print(scalar)
    return scalar


def adj_19_draws(df, params_dir, location_id):
    df_19 = df[df.year_id == 2017].copy().reset_index(drop=True)
    df_19["year_id"] = 2019
    df = df[~(df.year_id==2019)]
    draw_cols = df_19.columns[df_19.columns.str.contains("draw")]

    paho_scalar = get_scalar(params_dir, location_id)

    df_19[draw_cols] = df_19[draw_cols] * paho_scalar
    df = df.append(df_19)
    return df


if __name__ == "__main__":
    meids = {
        ADDRESS1: [6],
        ADDRESS2: [5, 6],
        ADDRESS3: [5],
        ADDRESS4: [5, 6],
        ADDRESS5: [5, 6]
    }


for meid, measures in meids.items():
    print(location_id)
    print(params_dir)
    print(meid)
    loc_draws = fetch_draws(meid, measures, location_id)
    new_draws = adj_19_draws(loc_draws, params_dir, location_id)
    print(new_draws[new_draws.year_id==2017]["draw_990"].mean())
    print(new_draws[new_draws.year_id==2019]["draw_990"].mean())
    new_draws.to_csv(f"FILEPATH", index=False)
