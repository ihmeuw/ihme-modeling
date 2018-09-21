import pandas as pd
import sys
import os
from getpass import getuser
from transmogrifier.maths import interpolate

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

def custom_age_weights(age_group_ids, gbdrid=4):
    """Get age weights scaled to age group start and end"""
    t = qry.get_age_weights(gbdrid)

    t = t.query('age_group_id in {}'.format(age_group_ids))
    # scale weights to 1
    t['age_group_weight_value'] =  \
        t['age_group_weight_value'] / \
        t['age_group_weight_value'].sum()

    return t[['age_group_id', 'age_group_weight_value']]


def age_standardize(df):
    """Make each draw in the dataframe a rate, then age standardize.
    """

    age_weights = custom_age_weights(list(df.age_group_id.unique()), 4)

    df = df.merge(age_weights, on=['age_group_id'], how='left')
    assert df.age_group_weight_value.notnull().values.all(), 'age weights merg'

    # make weighted product and sum
    df = pd.concat(
        [
            df[dw.SEV_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x * df['age_group_weight_value'])
        ],
        axis=1
    )
    df['age_group_id'] = 27
    df = df.groupby(dw.SEV_GROUP_COLS, as_index=False)[dw.DRAW_COLS].sum()
    return df


def custom_interpolate(df):
    """Interpolate SEV draws"""
    draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
    id_cols = list(set(df.columns) - (set(draw_cols + ['year_id'])))
    dfs = []
    for year_range in [[1990, 1995], [1995, 2000], [2000, 2005], [2005, 2010], [2010, 2016]]:
        start_df = (df.ix[df.year_id == year_range[0]].sort_values(id_cols)
                    .reset_index(drop=True))
        end_df = (df.ix[df.year_id == year_range[1]].sort_values(id_cols)
                  .reset_index(drop=True))
        ydf = interpolate(start_df, end_df, id_cols, 'year_id', draw_cols,
                          year_range[0], year_range[1])
        dfs.append(ydf.query('year_id < {} or year_id == 2016'.format(year_range[1])))
    df = pd.concat(dfs)

    return df


def process_sev_rei_id(rei_id, location_ids):
    """Read, filter, and save a sev, expressed with rei_id."""

    print 'reading'
    # read from the global sev path
    df = pd.read_stata(dw.SEV_PATH.format(rei_id=rei_id))

    print 'cleaning'
    # location_id is float for some reason
    df['location_id'] = df['location_id'].astype(int)
    # only need reporting locations
    df = df.query('location_id in {}'.format(location_ids))
    # only need age-standardized and both sexes
    df = df.query('age_group_id in {} & sex_id==3'.format(range(2, 21) + range(30, 33) + [235]))
    # only need age-standardized and both sexes
    df = df.query('year_id in {}'.format(range(1990, 2011, 5) + [2016]))
    # rename to standard draw col names
    df = df.rename(columns=lambda x: x.replace('sev_', 'draw_'))
    # rename risk id to db standard rei_id
    df = df.rename(columns={'risk_id': 'rei_id'})
    # sev is the measure
    df['measure_id'] = 29
    # rate
    df['metric_id'] = 3
    # keep the right columns
    df = df[
        dw.SEV_GROUP_COLS +
        dw.DRAW_COLS
    ]

    print 'interpolating'
    df = custom_interpolate(df)

    print 'age-standardizing'
    df = age_standardize(df)

    print 'saving'
    # write the output to standard sdg file structure
    out_dir = "{d}/sev/{v}/".format(d=dw.INPUT_DATA_DIR, v=dw.SEV_VERS)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_path = "{od}/{r}.h5".format(od=out_dir, r=rei_id)
    df.to_hdf(out_path, key="data", format="table",
              data_columns=['location_id', 'year_id'])


if __name__ == "__main__":
    locsdf = qry.get_sdg_reporting_locations()
    for rei_id in dw.SEV_REI_IDS:
        print 'running', rei_id, '...'
        try:
            process_sev_rei_id(rei_id, list(locsdf['location_id']))
        except IOError:
            print 'No data for {} with this batch of SEVs'.format(rei_id)
