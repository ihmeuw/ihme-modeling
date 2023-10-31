import pandas as pd
import scipy.stats as sp
import os
from pathlib import Path
from argparse import ArgumentParser, Namespace
from get_draws.api import get_draws
from db_queries import get_demographics


idcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
years = [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022]
ages = [2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 34, 32, 235, 238, 388, 389]
sexes = [1, 2]
run_dir = 'FILEPATH'


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--loc_id", help="location to use", type=int)
    parser.add_argument("--gbd_round_id", type = int)
    parser.add_argument("--decomp_step", type = str)
    return parser.parse_args()


def get_exposure(loc_id, gbd_round_id, decomp_step):
    exp_df = get_draws('modelable_entity_id', 10487, 'epi', location_id=loc_id, year_id=years, age_group_id = ages, sex_id = sexes,
    gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    exp_df.drop(['measure_id', 'metric_id', 'model_version_id', 'modelable_entity_id'], axis=1, inplace=True)
    out_dir = os.path.join(run_dir, 'iron_deficiency', 'exposure')
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    exp_df.to_csv(f'{out_dir}/{loc_id}.csv', index = False)

if __name__ == "__main__":
    args = parse_args()
    get_exposure(
        loc_id = args.loc_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step)
