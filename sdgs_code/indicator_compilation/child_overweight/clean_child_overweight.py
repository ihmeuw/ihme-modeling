import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

def collapse_sex(df):
    """Convert prevalence to cases"""
    pops = qry.get_pops(both_sexes=False)
    df = df.merge(pops, how = 'left', on = ['location_id','age_group_id','sex_id','year_id'])

    draws = [col for col in df.columns if 'draw_' in col]
    id_cols = dw.CHILD_OVERWEIGHT_GROUP_COLS
    # make sex 3 to collapse to both
    df['sex_id'] = 3
    # convert to cases by multiplying each draw by the population value
    df = pd.concat([df[id_cols],
                    df[draws].apply(lambda x: x * df['population']),
                    df['population']
                    ], axis=1
                   )
    # sum sexes together
    df = df.groupby(id_cols, as_index=False)[draws + ['population']].sum()
    # Turn back to proportion
    df = pd.concat([
        df[id_cols],
        df[draws].apply(lambda x: x / df['population'])
    ], axis=1
    )
    return df


locsdf = qry.get_sdg_reporting_locations()
print('assembling childhood overweight files')
dfs = []
n = len(locsdf['location_id'])
i = 0
for loc in locsdf['location_id']:
    df = pd.read_csv(os.path.join(dw.CHILD_OVERWEIGHT_DIR, str(loc) + '.csv'))
    df = df.query('age_group_id == 5 and year_id >= 1990')
    dfs.append(df)
    i = i + 1
    if i % (n / 10) == 0:
        print('{i}/{n} childhood overweight files complete'.format(i=i, n=n))
df = pd.concat(dfs, ignore_index=True)

df['metric_id'] = 2

df = collapse_sex(df)

df = df[dw.CHILD_OVERWEIGHT_GROUP_COLS + dw.DRAW_COLS]

#sdg_test.all_sdg_locations(df)
# make sure the directory exists (probably create it)
try:
    if not os.path.exists(dw.CHILD_OVERWEIGHT_OUTDIR):
        os.makedirs(dw.CHILD_OVERWEIGHT_OUTDIR)
except OSError:
    pass
df.to_hdf(os.path.join(dw.CHILD_OVERWEIGHT_OUTDIR, 'child_overweight_clean.h5'), format="table", key="data",
          data_columns=['location_id', 'year_id'])
