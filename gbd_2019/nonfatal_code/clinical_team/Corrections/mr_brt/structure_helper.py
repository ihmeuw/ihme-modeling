"""
Need to keep the mr brt outputs exactly in line what what we'd done before

read in all the draw files and prep them like the ones we used for R modeling
this lets us retain the 'mr-brt' correction factor model type across an entire run
"""
import numpy as np
import pandas as pd
import glob
import sys
import warnings
from getpass import getuser

sys.path.append(FILEPATH)
import clinical_mapping

warnings.warn("This is hardcoded to map version 20 and run_id 3")

run_id = 3

def add_UI(df, drop_draws=True):
    draw_columns = df.filter(regex="^draw").columns.tolist()
    draws_df = df[draw_columns].values
    
    df['mean'] = df[draw_columns].mean(axis=1)

    df['lower'] = np.percentile(draws_df, 2.5, axis=1)
    df['upper'] = np.percentile(draws_df, 97.5, axis=1)

    if drop_draws:
        df.drop(draw_columns, axis=1, inplace=True)
    return df

# this is what we need our data to look like. each file is a cf type
example_df = pd.read_csv(FILEPATH)
# cols = ['cf_location_id', 'sex_id', 'age_start', 'mean_incidence', 'icg_name', 'icg_id']

files = glob.glob(FILEPATH)

# get the draw data
back = pd.concat([pd.read_csv(f) for f in files], sort=False, ignore_index=True)

df = add_UI(back.copy())

icg_df = clinical_mapping.get_clinical_process_data("icg_durations", map_version=20)[['icg_id', 'icg_name']]

df = df.merge(icg_df, how='left', on='icg_id')
df.drop(['upper', 'lower', 'age_group_id'], axis=1, inplace=True)
for col in ['age_start', 'sex_id']:
    df[col] = pd.to_numeric(df[col], downcast='integer')

for cft in df.cf_type.unique():
    if cft == 'cf1':
        cfname = 'indvcf'
    elif cft == 'cf2':
        cfname = 'incidence'
    elif cft == 'cf3':
        cfname = 'prevalence'
    else:
        assert False, 'the hell?!'

    # do something
    tmp = df[df['cf_type'] == cft].copy()
    tmp.drop('cf_type', axis=1, inplace=True)
    tmp.rename(columns={'mean': 'mean_{}'.format(cfname)}, inplace=True)
    tmp.drop_duplicates(subset=['age_start', 'sex_id', 'icg_id', 'icg_name'], inplace=True)

    write_path = FILEPATH.format(r=run_id, cf=cfname)
    tmp.to_csv(write_path, index=False)

