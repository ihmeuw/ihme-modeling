"""
Trim 2019 model values to only the remaining reported locations for the year.
Additionally, add in recent Angola case for GBD study year 2019.
Additionally, Ensure Nigeria and subnationals are 0 from 2010 onwards.
"""

### ======================= BOILERPLATE ======================= ###

import os
import argparse
import pandas as pd
import db_queries as db

code_root <-"FILEPATH"
data_root <- "FILEPATH"
cause = "ntd_guinea"

if os.getenv("EXEC_FROM_ARGS"):
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
else:
    params_dir  <- "FILEPATH"
    draws_dir   <- "FILEPATH"
    interms_dir <- "FILEPATH"
    logs_dir    <- "FILEPATH"

### Define Constants
gbd_round_id <- "ADDRESS"
decomp_step <- "ADDRESS"
loc_h = db.get_location_metadata(ADDRESS)

### ======================= MAIN EXECUTION ======================= ###

### LOAD DRAWS 
draws_df = pd.read_csv(f"FILEPATH")
draw_cols = draws_df.columns[draws_df.columns.str.contains('draw')]


### TRIM 2019 TO ENDEMIC LOCS
eth_subs = loc_h.loc[loc_h.parent_id == 179, ['location_id', 'location_name']]
end_locations = loc_h.loc[loc_h.location_name.isin(['Chad', 'Mali', 'South Sudan', 'Gambella']), 
                                                ['location_id', 'location_name']]
end_locations = end_locations.append(eth_subs)
draws_df.loc[(~draws_df.location_id.isin(end_locations.location_id.unique())
              & (draws_df.year_id >= 2019)
              ), draw_cols] = 0

# Endemic Location Draws > 2019 set equal to 2019 values
for year in range(2020, 2023):
    draws_df.loc[(draws_df.location_id.isin(end_locations.location_id.unique())
                & (draws_df.year_id == year)
                ), draw_cols] = draws_df.loc[(draws_df.location_id.isin(end_locations.location_id.unique())
                                                & (draws_df.year_id == 2019)), draw_cols].values


### ADD IN ANGOLA CASE, AS DRAW RATE
case_pop = db.get_population(age_group_id=14, location_id=168, year_id=2019, sex_id=2, decomp_step=decomp_step, gbd_round_id=gbd_round_id)
draws_df.loc[((draws_df.year_id == 2019)
                  & (draws_df.sex_id == 2)
                  & (draws_df.age_group_id == 14)
                  & (draws_df.location_id == 168)
                  ), draw_cols] = (1 / case_pop.population.values[0])


### Zero Nigeria + Subnats for 2010 onward
nigeria_locs = loc_h.loc[loc_h.location_name.isin(['Nigeria']), 
                                                ['location_id', 'location_name']]
nigeria_subs = loc_h.loc[loc_h.parent_id == 214, ['location_id', 'location_name']]
nigeria_locs = nigeria_locs.append(nigeria_subs)
draws_df.loc[(draws_df.location_id.isin(nigeria_locs.location_id.unique())
              & (draws_df.year_id >= 2010)
              ), draw_cols] = 0

draws_df.to_csv(f'FILEPATH')
