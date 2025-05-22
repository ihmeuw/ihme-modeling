from db_queries import get_location_metadata
import numpy as np
import pandas as pd
import os

LOCATION_FOLDER = FILEPATH
LOCATION_INPUT_FOLDER = os.path.join(LOCATION_FOLDER, "inputs")
LOCATION_OUTPUT_FOLDER = os.path.join(LOCATION_FOLDER, "outputs")

def obtain_group_dict():
  group_data = pd.read_excel(
      FILEPATH, index_col = False)
  group_data["location_id"] = group_data["location_id"].fillna(-99)
  group_data = group_data.loc[group_data["location_id"] != -99]
  d = {}
  for i in group_data['this_side'].unique():
    d[i] = [group_data['location_id'][j] for j in group_data[group_data['this_side'] == i].index]
  return d

def match_df_groups_with_location_ids(df, d):
  df = df.rename(columns={"side_b": "side_b_string", "side_a": "side_a_string"})
  print(df.columns)
  df["side_b"] = ""
  for row in range(0, len(df)):
    if type(df["side_b_string"].iloc[row]) == str and len(df["side_b_string"].iloc[row]) > 0:
      groups = df["side_b_string"].iloc[row].split(",")
      groups = list(map(str.strip, groups))
      df["side_b"].iloc[row] = []
      for group in groups:
        if group in d.keys():
          df["side_b"].iloc[row] += d[group]
          df["side_b"].iloc[row] = list(set(df["side_b"].iloc[row]))
  df["side_b"] = df["side_b"].apply(lambda x: str(
      x).replace("[", "").replace("]", "") if type(x) == list else x)
  return df

def generate_group_matched_file(df, source, version='test'):
  input_folder = LOCATION_INPUT_FOLDER.format(source=source)
  input_archive_folder = os.path.join(input_folder, "archive")
  assert os.path.exists(input_folder), ("you are missing the source input folder")
  d = obtain_group_dict()
  df = match_df_groups_with_location_ids(df, d)
  df = df[df['side_b'].map(lambda d: len(d)) > 0]
  COLS = ['source_event_id', 'location_id', 'side_a', 'side_b', "side_a_string", "side_b_string",
          'country', 'admin1', 'admin2', 'admin3', 'urban_rural']
  for col in COLS:
    if col not in df.columns:
      df[col] = ""
  df = df[COLS]
  map_path = os.path.join(input_folder, "FILEPATH".format(source))
  map_path_archive = os.path.join(input_archive_folder,
                                  "FILEPATH".format(source, version))
  if df.shape[0] >= 1:
    df.to_csv(map_path, index=False)
    df.to_csv(map_path_archive, index=False)
