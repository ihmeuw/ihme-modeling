import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

# get countries
locsdf = qry.get_sdg_reporting_locations()

# compile countries
n = 0
N = len(locsdf['ihme_loc_id'])
dfs = []
for iso in locsdf['ihme_loc_id']:
    n = n + 1
    if n % (N / 10) == 0:
        print str(n) + " of " + str(N) + " countries"
    # read in country file, aggregate to all ages, both sexes
    df = pd.read_csv(os.path.join(dw.ART_DIR, iso + '.csv'))
    df = df.query('year_id >= 1990')
    df = df.groupby(['location_id', 'year_id', 'run_num'], as_index=False)['pop_art', 'pop_hiv'].sum()
    df['value'] = df['pop_art'] / df['pop_hiv']
    df.loc[df.pop_hiv == 0, 'value'] = 0 # set floor value
    assert len(df.loc[df.value.isnull()]) == 0, 'NaNs in dataset'
    df['run_num'] = df['run_num'] - 1
    df['run_num'] = 'draw_' + df['run_num'].astype(str)
    df = df.pivot_table(index=['location_id','year_id'],
                        columns='run_num',
                        values='value')
    df = df.reset_index()
    dfs.append(df)
df = pd.concat(dfs)
df['measure_id'] = 18
df['metric_id'] = 2
df['age_group_id'] = 22
df['sex_id'] = 3
df = df[dw.ART_GROUP_COLS + dw.DRAW_COLS]

# save
try:
    if not os.path.exists(dw.ART_OUTDIR):
        os.makedirs(dw.ART_OUTDIR)
except OSError:
    pass
df.to_hdf(dw.ART_OUTDIR + "/art_clean.h5",
          key="data",
          format="table", data_columns=['location_id', 'year_id'])
