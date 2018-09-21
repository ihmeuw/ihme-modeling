import os
import sys

import pandas as pd
import numpy as np

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.tests as sdg_test

from transmogrifier.maths import interpolate


def add_england_aggregate(df, locsdf):
    """Append england aggregate to the dataframe.

    Take the pop-weighted mean, by draw, of each china subnational.
    """

    # subset to just UTLAs
    utlas = locsdf[locsdf['path_to_top_parent'].str.contains(',4749,')].query('most_detailed == 1')['location_id'].values
    eng = df[df['location_id'].isin(utlas)]

    # merge with populations
    pops = qry.get_pops(both_sexes=True)
    pops = pops.query('sex_id==3 & age_group_id == 22')
    eng = eng.merge(pops[['location_id', 'year_id', 'population']])
    assert eng.population.notnull().values.all(), 'merge with pops failed'

    # calculate the pop-weighted average of each draw column
    g = eng.groupby(['year_id'])
    eng = g.apply(lambda x: x[dw.DRAW_COLS].apply(
                    lambda y: np.average(y, weights=x['population'])
                )
    ).reset_index()
    eng['location_id'] = 4749

    # add the national observation to df
    df = df.append(eng, ignore_index=True)
    return df


def custom_interpolate(df):
    """Interpolate attributable burden draws"""
    draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
    id_cols = list(set(df.columns) - (set(draw_cols + ['year_id'])))
    dfs = []
    for year_range in [[1990, 1995], [1995, 2000], [2000, 2005], [2005, 2010]]:
        start_df = (df.ix[df.year_id == year_range[0]].sort_values(id_cols)
                    .reset_index(drop=True))
        end_df = (df.ix[df.year_id == year_range[1]].sort_values(id_cols)
                  .reset_index(drop=True))
        ydf = interpolate(start_df, end_df, id_cols, 'year_id', draw_cols,
                          year_range[0], year_range[1])
        dfs.append(ydf.query('year_id < {}'.format(year_range[1])))
    df = pd.concat(dfs)
    return df



def main():
    """read, standardize columns, add location id, add china aggregate"""
    # get all the things
    locsdf = qry.get_sdg_reporting_locations()
    dfs = []
    print('assembling pm25 files')
    n = len(locsdf['ihme_loc_id'])
    i = 0
    for ihme_loc_id in locsdf['ihme_loc_id']:
        if os.path.isfile(os.path.join(dw.MEAN_PM25_INDIR, ihme_loc_id + '.csv')):
            df = pd.read_csv(os.path.join(dw.MEAN_PM25_INDIR, ihme_loc_id + '.csv'))
            df['ihme_loc_id'] = ihme_loc_id
            dfs.append(df)
        else:
            print("Does not exist:" + os.path.join(dw.MEAN_PM25_INDIR, ihme_loc_id + '.csv'))
        i = i + 1
        if i % (n / 10) == 0:
            print('{i}/{n} files complete'.format(i=i, n=n))
    df = pd.concat(dfs, ignore_index=True)
    df = df.merge(locsdf[['ihme_loc_id', 'location_id']])
    df = df.rename(columns={'year': 'year_id'})
    df = df.rename(columns={'draw_1000': 'draw_0'})
    df = df[['location_id', 'year_id'] + dw.DRAW_COLS]
    df = add_england_aggregate(df, locsdf)

    # standardize column structure again
    df['metric_id'] = 3
    df['measure_id'] = 19
    df['age_group_id'] = 22
    df['sex_id'] = 3
    df = df[dw.MEAN_PM25_GROUP_COLS + dw.DRAW_COLS]

    # interpolate pre-2010
    post2010df = df.query('year_id >= 2010')
    df = custom_interpolate(df)
    df = df.append(post2010df)

    # save
    try:
        if not os.path.exists(dw.MEAN_PM25_OUTDIR):
            os.makedirs(dw.MEAN_PM25_OUTDIR)
    except OSError:
        pass
    df.to_hdf(dw.MEAN_PM25_OUTDIR + "/air_pm_draws_clean.h5",
              format="table", key="data",
              data_columns=['location_id', 'year_id'])

if __name__ == "__main__":
    main()
