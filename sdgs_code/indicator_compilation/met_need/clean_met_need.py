import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

# get locations
locsdf = qry.get_sdg_reporting_locations()

# get main dataset
df = pd.read_csv(dw.MET_NEED_FILE)
df = df.query('year_id >= 1990')
df = df.query('location_id in {}'.format(list(locsdf['location_id'])))
df['metric_id'] = 2
df['measure_id'] = 18

# get weights
if 'mod_contra' in dw.MET_NEED_VERS:
    agesdf = qry.get_pops()
    agesdf = agesdf.loc[agesdf.age_group_id.isin(df.age_group_id.unique())]
    agesdf['totpop'] = agesdf.groupby(['location_id', 'year_id', 'sex_id'], as_index=False)['population'].transform('sum')
    agesdf['weights'] = agesdf['population'] / agesdf['totpop']
else:
    agesdf = pd.read_csv(dw.MET_NEED_WEIGHTS_FILE)
    agesdf = agesdf.query('location_id in {}'.format(list(locsdf['location_id'])))
    agesdf = agesdf.query('year_id >= 1990')
    agesdf['weights'] = agesdf[['weight_' + str(i) for i in range(0, 1000)]].mean(axis=1)
agesdf = agesdf[['location_id', 'year_id', 'age_group_id', 'sex_id', 'weights']]

# aggregate
df = df.merge(agesdf, how='left', on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
assert df.weights.notnull().values.all(), 'merge with pops fail'
assert df.metric_id.notnull().values.all(), 'merge with pops fail'
df = pd.concat([df[dw.MET_NEED_GROUP_COLS],
                df[dw.DRAW_COLS].apply(lambda x: x * df['weights'])],
               axis=1
               )

df['age_group_id'] = 24
df = df.groupby(dw.MET_NEED_GROUP_COLS, as_index=False)[dw.DRAW_COLS].sum()

#sdg_test.all_sdg_locations(df)
# make sure the directory exists (probably create it)
try:
    if not os.path.exists(dw.MET_NEED_OUTDIR):
        os.makedirs(dw.MET_NEED_OUTDIR)
except OSError:
    pass
df.to_hdf(os.path.join(dw.MET_NEED_OUTDIR, 'met_need_clean.h5'), format="table", key="data",
          data_columns=['location_id', 'year_id'])
