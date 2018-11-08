# Author: 
# Date: 03/30/2018
# Purpose: Draws of scalar for NAFLD

import pandas as pd 
import db_queries as db 
from get_draws.api import get_draws
import csv
import glob
import numpy as np
import sys
import datetime

#argument from parent script
location = sys.argv[1].split()[0]
#only works if this date is same as date in scratch folder
date = datetime.datetime.now()
date = date.strftime("%Y_%m_%d")

#read in data 
data_inp = pd.read_csv("FILEPATH")
data_inp = data_inp.drop(["bound", "current_drinkers", "abstainers", "prev_light_drinker"], axis = 1)

#make data wide
data_inp = pd.pivot_table(data_inp, values="abstainers_and_light_drinkers", index=["location_id", "year_id", "sex_id", "age_group_id"], columns="draw").reset_index()

#get draws NAFLD
nafld = get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 18662, source = "epi", measure_id= 5, location_id=location)

#getting correct ages
age_group_ids = data_inp.age_group_id.unique()
nafld = nafld[nafld.age_group_id.isin(age_group_ids)].reset_index()
nafld = nafld.drop(["measure_id", "metric_id", "model_version_id", "modelable_entity_id"], axis = 1)

draws = ["draw_" + str(i) for i in xrange (0,1000)]

#multiplying scalar by prevalence
for draw in draws:
	nafld[draw] = nafld[draw]*data_inp[draw]

nafld.to_csv("FILEPATH".format(date, location), index = False)



