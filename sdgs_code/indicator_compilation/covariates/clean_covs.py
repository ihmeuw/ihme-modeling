import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

locsdf = qry.get_sdg_reporting_locations()

def cov_grabber(cov_id):
    print('assembling {} files'.format(cov_id))
    dfs = []
    n = len(locsdf['location_id'])
    i = 0
    if cov_id != 'vacc_full':
        path = os.path.join(dw.COV_PATH, cov_id)
    else:
        path = os.path.join(dw.VACC_PATH)
    for loc in locsdf['location_id']:
        df = pd.read_csv(os.path.join(path, str(loc) + '.csv'))
        dfs.append(df)
        i = i + 1
        if i % (n / 10) == 0:
            print('{i}/{n} {cid} files complete'.format(i=i, n=n, cid=cov_id))
    df = pd.concat(dfs, ignore_index=True)

    assert len(df.age_group_id.unique()) + len(df.sex_id.unique()) == 2

    df = df.query('year_id >= 1990')

    # set measure_id to proportion
    if 'measure_id' not in df.columns:
        df['measure_id'] = 18

    # set metric to proportion
    if 'metric_id' not in df.columns:
        df['metric_id'] = 2

    # save id columns
    id_cols = ['location_id', 'year_id', 'age_group_id',
               'sex_id', 'metric_id', 'measure_id']
    # keep necessary variables
    df = df[id_cols + dw.DRAW_COLS]
    # make sure the directory exists (probably create it)
    try:
        if not os.path.exists(dw.COV_OUTDIR):
            os.makedirs(dw.COV_OUTDIR)
    except OSError:
        pass
    # convert to hdf
    df.to_hdf(
        os.path.join(dw.COV_OUTDIR, cov_id + '.h5'),
        format="table", key="data",
        data_columns=['location_id', 'year_id']
    )

for cov_id in dw.COV_IDS:
    cov_grabber(cov_id)
