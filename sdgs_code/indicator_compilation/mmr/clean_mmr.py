# Do minimal cleaning necessary for MMR draws
# Save to input_data folder

import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

# get all the things
mmr_files = os.listdir(dw.MMR_DIR)
mmr_files = [mmr for mmr in mmr_files if 'draws_' in mmr]
mmr_files = [mmr for mmr in mmr_files if '366' in mmr]
dfs = []
print('assembling mmr files')
n = len(mmr_files)
i = 0
for mmr_file in mmr_files:
    df = pd.read_hdf(os.path.join(dw.MMR_DIR, mmr_file))
    df = df.query('age_group_id == 169')
    dfs.append(df)
    i = i + 1
    if i % (n / 10) == 0:
        print('{i}/{n} mmr_files complete'.format(i=i, n=n))
df = pd.concat(dfs, ignore_index=True)
locsdf = qry.get_sdg_reporting_locations()
df = df[df['location_id'].isin(locsdf['location_id'].values)]

# make sure it looks like we expect
assert set(df.cause_id) == {366}, 'unexpected cause ids'
assert set(df.sex_id) == {2}, 'unexpected sex ids'
assert set(df.age_group_id) == {169}, 'unexpected age group ids'
assert set(df.year_id) == set(range(1990, 2017)), 'unexpected year_ids'

# standardize columns
df = df[dw.MMR_ID_COLS + dw.DRAW_COLS]

try:
    if not os.path.exists(dw.MMR_OUTDIR):
        os.makedirs(dw.MMR_OUTDIR)
except OSError:
    pass
df.to_hdf("{d}/366.h5".format(d=dw.MMR_OUTDIR), key="data",
          format="table",
          data_columns=['location_id', 'year_id', 'age_group_id'])
