"""
Script to interpolate and extrapolate gbd17 years to gbd19 years.
Must pull draws in archive env and save to processing draws dir ex:
draws = get_draws(source="epi",
                  gbd_id_type="modelable_entity_id", 
                  gbd_id=ADDRESS, 
                  version_id=ADDRESS, 
                  measure_id=ADDRESS,
                  gbd_round_id=ADDRESS)
draws.to_hdf(f'FILEPATH', key='draws', mode='w')
"""
import numpy as np
import pandas as pd
import scipy as sp
import argparse
import os
import time
import math
from get_draws.api import get_draws
from core_maths.interpolate import pchip_interpolate
from sklearn import linear_model

### Parse jobmon/qsub args
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


def _predict_lm_2019(y):
    """
    Fits 2015/2017 returns 2019 extrap. value, cieling 5%
    """
    model = linear_model.LinearRegression()
    model.fit([[2015], [2017]], y)
    val_19 = model.predict([[2019]])
    return val_19[0]


def linear_extrap(df):
    """
    Extend 2015-2017 to 2019 via a linear extrap on log() mean / CI
    """
    # Initialize year 2019 demographics
    df_19 = df[df.year_id == 2015].copy().reset_index(drop=True)
    df_19["year_id"] = 2019
    draw_cols = df.columns[df.columns.str.contains("draw")]

    # Log transform each year mean/CI
    for year in [2015, 2017]:
        year_df = df[df.year_id == year].copy().reset_index(drop=True)
        year_df[f"log_mean_{year}"] = year_df[draw_cols].mean(axis=1)
        year_df[f"log_upper_{year}"] = year_df[draw_cols].quantile(0.95, axis=1)
        year_df[f"log_lower_{year}"] = year_df[draw_cols].quantile(0.05, axis=1)
        df_19 = df_19.merge(year_df[["age_group_id", "sex_id", "measure_id", f"log_mean_{year}", f"log_upper_{year}", f"log_lower_{year}"]], on=["age_group_id", "sex_id", "measure_id"], how="left")

    # Regress linear model per demographic.
    df_19["log_mean_2019"] = df_19.apply(lambda x: _predict_lm_2019( [x["log_mean_2015"], x["log_mean_2017"]]), axis=1 )
    df_19["log_lower_2019"] = df_19.apply(lambda x: _predict_lm_2019( [x["log_lower_2015"], x["log_lower_2017"]]), axis=1 )
    df_19["log_upper_2019"] = df_19.apply(lambda x: _predict_lm_2019( [x["log_upper_2015"], x["log_upper_2017"]]), axis=1 )

    # Expand draws as log-normal distribution / exponentiate to normal space
    df_19[draw_cols] = df_19.apply(lambda x: pd.Series( np.random.normal( x["log_mean_2019"],  abs(x["log_upper_2019"] - x["log_lower_2019"]) * 1.05, (1000) ) ), axis=1)
    df_19[(df_19[draw_cols] < 0)] = 0
    return df_19


def carry_19(df):
    """
    Move 2017.
    """
    df_19 = df[df.year_id == 2017].copy().reset_index(drop=True)
    df_19["year_id"] = 2019
    draw_cols = df.columns[df.columns.str.contains("draw")]
    df_19[draw_cols] = df_19[draw_cols] * 1.00001
    return df_19


if __name__ == "__main__":
    # Draws pulled via gbd17 archive env prior (see docs), draws dir hardcode for now.
    meid = ADDRESS

    print(f"Reading csv {time.time()}")
    draws = pd.read_csv(f'FILEPATH')
    print(f"read csv {time.time()}")
    draws_15 = pchip_interpolate(draws,
                ['measure_id', 'modelable_entity_id',
                'location_id', 'sex_id', 'age_group_id'],
                ['draw_{}'.format(x) for x in range(1000)],
                time_vals=[2015])
    print(f"Interp complete: {time.time()}")
    draws = draws.append(draws_15)
    # draws_19 = linear_extrap(draws)
    draws_19 = carry_19(draws)
    print(f"extrap complete: {time.time()}")
    draws = draws.append(draws_19)
    print(f"draws complete: {time.time()}")
    draws["metric_id"] = 3

    draws.to_csv(f"{draws_dir}/{meid}/{location_id}.csv", index=False)
