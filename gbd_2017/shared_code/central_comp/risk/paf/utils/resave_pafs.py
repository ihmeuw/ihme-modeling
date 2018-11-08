# preprocessing step -- take all pafs for a given rei/location
# and save as csv by rei location for the paf compiler to use
import sys
import os
from get_draws.api import get_draws
from db_queries import get_demographics
import pandas as pd

# parse rei_id, location_id
task_id = int(os.environ.get("SGE_TASK_ID")) - 1
params = pd.read_csv("FILEPATH/params.csv")
params = params.loc[task_id]
rei_id = int(params.rei_id.item())
location_id = int(params.location_id.item())
model_version_id = int(params.model_version_id.item())

demo = get_demographics(gbd_team="epi")
df = get_draws(gbd_id_type="rei_id", gbd_id=rei_id, location_id=location_id,
               sex_id=demo['sex_id'], age_group_id=demo['age_group_id'],
               source="paf", version_id=model_version_id)

dir_name = "FILEPATH/{}/".format(rei_id)
if not os.path.exists(dir_name):
    try:
        os.makedirs(dir_name)
    except Exception:
        pass
df.to_csv("FILEPATH/{}/{}.csv".format(rei_id, location_id), index=False)
