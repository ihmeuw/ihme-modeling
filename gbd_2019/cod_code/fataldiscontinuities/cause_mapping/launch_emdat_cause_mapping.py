import getpass
import pandas as pd
import sys
USER = getpass.getuser()
SHOCK_REPO = "FILEPATH".format(USER)
sys.path.append(SHOCK_REPO)

from shock_world import ShockWorld

# will pull in df from the raw_data table
sw = ShockWorld(step="cause_mapping", source="SOURCE",
                source_update=True, upload=True, conn_def='ADDRESS')

df = sw.df.copy()

cause_map = pd.read_csv(("FILEPATH"))

cause_map = cause_map.rename(columns={"event_type": "event_code"})
cause_map["source_event_id"] = cause_map["source_event_id"].astype(str)
original_shape = df.copy().shape[0]
original_deaths = df.copy().best.sum()
df = pd.merge(left=df, right=cause_map[['source_event_id', 'event_code']], on='source_event_id', how='left')

assert original_shape == df.shape[0]
assert original_deaths == df.best.sum()

map_dir = r"FILEPATH"
map_data = pd.read_excel(map_dir)

original_shape = df.copy().shape[0]
print(original_shape)
original_deaths = df.copy().best.sum()
df = pd.merge(left=df, right=map_data[['event_code', 'cause_id']], on='event_code')
print(df.shape[0])
assert original_shape == df.shape[0]
assert original_deaths == df.best.sum()

sw.set_df(df)

sw.launch_step_uploader()
