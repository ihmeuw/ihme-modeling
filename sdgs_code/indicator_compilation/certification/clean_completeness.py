import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.tests as sdg_test

# get time series for completeness
locsdf = qry.get_sdg_reporting_locations()
print('assembling completeness files')
dfs = []
n = len(locsdf['location_id'])
i = 0
for loc in locsdf['location_id']:
    df = pd.read_csv(os.path.join(dw.COMPLETENESS_DIR, str(loc) + '.csv'))
    dfs.append(df)
    i = i + 1
    if i % (n / 10) == 0:
        print('{i}/{n} completeness files complete'.format(i=i, n=n))
df = pd.concat(dfs, ignore_index=True)

df = df.query('year_id >= 1990')
df['age_group_id'] = 22
df['sex_id'] = 3

# only keep countries for which we have at least one data point (as identified by the VR file)
completenessdf = pd.read_csv(dw.COMPLETENESS_DATA_FILE)
completenessdf = completenessdf.query('data_type_id == 9')
datalocs = list(locsdf.query('location_id in {}'.format(list(completenessdf['location_id'].unique())))['location_id'])
datalocsdf = df.query('location_id in {}'.format(datalocs))
nodatalocsdf = df.query('location_id not in {}'.format(datalocs))
nodatalocsdf = pd.concat([nodatalocsdf[dw.COMPLETENESS_GROUP_COLS],
                          nodatalocsdf[dw.DRAW_COLS].apply(lambda x: x * 0)
                         ], axis=1)
df = datalocsdf.append(nodatalocsdf)

df = df[dw.COMPLETENESS_GROUP_COLS + dw.DRAW_COLS]
df[dw.COMPLETENESS_GROUP_COLS] = df[dw.COMPLETENESS_GROUP_COLS].apply(pd.to_numeric, downcast='integer')

try:
    if not os.path.exists(dw.COMPLETENESS_OUT_DIR):
        os.makedirs(dw.COMPLETENESS_OUT_DIR)
except OSError:
    pass

out_file = dw.COMPLETENESS_OUT_DIR + "/completeness.h5"
df.to_hdf(out_file, key="data", format="table",
          data_columns=['location_id', 'year_id'])
