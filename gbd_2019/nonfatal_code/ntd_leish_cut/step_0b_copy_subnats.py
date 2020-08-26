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
import glob
import db_queries as db

### Parse jobmon/qsub args
parser = argparse.ArgumentParser()
parser.add_argument("--params_dir", help="Directory containing params", type=str)
parser.add_argument("--draws_dir", help="Directory containing draws", type=str)
parser.add_argument("--interms_dir", help="Directory containing interms", type=str)
parser.add_argument("--logs_dir", help="Directory containing logs", type=str)
args = vars(parser.parse_args())

params_dir = args["params_dir"]
draws_dir = args["draws_dir"]
interms_dir = args["interms_dir"]
logs_dir = args["logs_dir"]

def copy_draws(draws_dir, meid):
     locs = []
     for f in glob.glob(f"{draws_dir}/{meid}/*.csv"):
         locs.append(int( f.rsplit("/")[-1][:-4] ) )
     study_locs = db.get_demographics("epi")["location_id"]
     loc_h = db.get_location_metadata(35)
     missing = [l for l in study_locs if l not in locs]

     zero_draws = pd.read_csv(f'{draws_dir}/{meid}/101.csv')
     draw_cols = zero_draws.columns[zero_draws.columns.str.contains("draw")]
     zero_draws[draw_cols] = zero_draws[draw_cols] * 0.0

     print(len(missing))
     for place in missing:
         if loc_h.loc[loc_h.location_id == place, "level"].values[0] == 3:
             zero_draws['location_id'] = place
             zero_draws.to_csv(f'{draws_dir}/{meid}/{place}.csv')
         elif loc_h.loc[loc_h.location_id == place, "level"].values[0] == 4:
             parent = loc_h.loc[loc_h.location_id == place, "parent_id"].values[0]
             draws = pd.read_csv(f'{draws_dir}/{meid}/{parent}.csv')
             draws['location_id'] = place
             draws.to_csv(f'{draws_dir}/{meid}/{place}.csv')
         print(place)
     return None

if __name__ == "__main__":
    meid = ADDRESS
    draws_dir = draws_dir
    copy_draws(draws_dir, meid)
