# collect prevalence draws from dw.PREV_REI_IDS
import sys
import os
from getpass import getuser
import sqlalchemy as sql
import pandas as pd
import numpy as np

from transmogrifier.draw_ops import get_draws
from transmogrifier.maths import interpolate

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry


def data_formatter(year_id, locsdf):
    dfs = []
    for location_id in locsdf['location_id']:
        df = pd.read_csv(os.path.join(dw.RISK_STAND_DIR, str(year_id), str(location_id) + '.csv'))
        dfs.append(df)
    df = pd.concat(dfs)
    df['measure_id'] = 1
    df['metric_id'] = 3
    df['draw'] = 'draw_' + df['draw'].astype(str)
    df = df.pivot_table(index=['location_id','year_id','age_group_id','sex_id','measure_id','metric_id','cause_id'],
                        columns='draw',
                        values='rsval')
    df = df.reset_index()
    return df

def load_RSM(for_interp, interpd):
    # compile risk-standardized mort
    if not os.path.exists(for_interp):
        dfs = []
        for year_id in range(1990, 2011, 5) + [2016]:
            print('Collecting ' + str(year_id))
            df = data_formatter(year_id, locsdf)
            dfs.append(df)
        df = pd.concat(dfs)
        df.to_hdf(for_interp,
                  key="data",
                  format="table",
                  data_columns=['location_id', 'year_id'])
    else:
        df = pd.read_hdf(for_interp)

    # interpolate
    draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
    id_cols = ['location_id','age_group_id','sex_id','measure_id','metric_id','cause_id']
    dfs = []
    for year_range in [[1990, 1995], [1995, 2000], [2000, 2005], [2005, 2010], [2010, 2016]]:
        print(year_range)
        start_df = (df.ix[df.year_id == year_range[0]].sort_values(id_cols)
                    .reset_index(drop=True))
        end_df = (df.ix[df.year_id == year_range[1]].sort_values(id_cols)
                  .reset_index(drop=True))
        ydf = interpolate(start_df, end_df, id_cols, 'year_id', draw_cols,
                          year_range[0], year_range[1])
        ydf = ydf.query('year_id < {} or year_id == 2016'.format(year_range[1]))
        dfs.append(ydf)
    df = pd.concat(dfs)
    df.to_hdf(interpd,
              key="data",
              format="table",
              data_columns=['location_id', 'year_id'])

    return df


def load_covs():
    # load covariates
    cov_dfs = []
    for cov in dw.UHC_COV_IDS:
        cov_df = pd.read_hdf('FILEPATH')
        cov_df['indicator'] = cov
        cov_dfs.append(cov_df)
    cov_df = pd.concat(cov_dfs)

    return cov_df


if __name__ == "__main__":
    locsdf = qry.get_sdg_reporting_locations()

    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'indicator']

    # get risk-standardized mort
    interpd = 'FILEPATH'
    if not os.path.exists(interpd):
        for_interp = 'FILEPATH'
        rsm_df = load_RSM(for_interp, interpd)
    else:
        rsm_df = pd.read_hdf(interpd)
    rsm_df = rsm_df.rename(index=str, columns={'cause_id': 'indicator'})
    rsm_df['indicator'] = 'rsm_' + rsm_df['indicator'].astype(str)
    assert len(rsm_df.age_group_id.unique()) == 1 and len(rsm_df.sex_id.unique()) == 1, \
        'Multiple ages/sexes present in rsm_df'

    # get covs
    cov_df = load_covs()
    assert len(cov_df.age_group_id.unique()) == 1 and len(cov_df.sex_id.unique()) == 1, \
        'Multiple ages/sexes present in cov_df'

    # get ART
    art_df = pd.read_hdf(os.path.join(dw.ART_OUTDIR, dw.UHC_ART_ID + '.h5'))
    art_df['indicator'] = 'art'
    assert len(art_df.age_group_id.unique()) == 1 and len(art_df.sex_id.unique()) == 1, \
        'Multiple ages/sexes present in art_df'

    # get met need
    met_need_df = pd.read_hdf(os.path.join(dw.MET_NEED_OUTDIR, dw.UHC_MET_NEED_ID + '.h5'))
    met_need_df['indicator'] = 'met_need'
    assert len(met_need_df.age_group_id.unique()) == 1 and len(met_need_df.sex_id.unique()) == 1, \
        'Multiple ages/sexes present in met_need_df'

    # compile
    df = pd.concat([rsm_df[index_cols + dw.DRAW_COLS],
                    cov_df[index_cols + dw.DRAW_COLS],
                    art_df[index_cols + dw.DRAW_COLS],
                    met_need_df[index_cols + dw.DRAW_COLS]])

    # save for R scaling
    try:
        if not os.path.exists(dw.UHC_TEMP_DIR):
            os.makedirs(dw.UHC_TEMP_DIR)
    except OSError:
        pass
    df.to_csv(dw.UHC_TEMP_DIR + '/unscaled_inputs.csv', index=False)
