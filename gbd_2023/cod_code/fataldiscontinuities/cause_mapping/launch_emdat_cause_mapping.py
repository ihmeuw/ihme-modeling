import getpass
import pandas as pd
import sys
USER = getpass.getuser()
SHOCK_REPO = FILEPATH.format(USER)
sys.path.append(SHOCK_REPO)

from shock_world import ShockWorld

def ask_table_name():
    table = input("test or production? ").strip().lower()
    if table not in ["test", "production"]:
        print("Your answer was not one of the options.")
        return ask_table_name()
    else:
        return table

def ask_upload():
    load = input("do you want to upload? Yes or No ").strip().lower()
    if load not in ["yes", "no"]:
        print("Your answer was not one of the options.")
        return ask_upload()
    else:
        if load == "yes":
            load = True
        elif load == "no":
            load = False
        return load

table = ask_table_name()
load = ask_upload()

sw = ShockWorld(step="cause_mapping", source="EMDAT",
                source_update=True, upload=load, conn_def=table)

df = sw.df.copy()

cause_map = pd.read_csv((FILEPATH))

cause_map = cause_map.rename(columns={"event_type": "event_code"})
cause_map["source_event_id"] = cause_map["source_event_id"].astype(str)
original_shape = df.copy().shape[0]
original_deaths = df.copy().best.sum()
df = pd.merge(left=df, right=cause_map[['source_event_id', 'event_code']], on='source_event_id', how='left')

assert original_shape == df.shape[0]
assert original_deaths == df.best.sum()

map_dir = FILEPATH
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
