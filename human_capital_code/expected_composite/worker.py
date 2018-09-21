import os
import sys
import getpass

import argparse

import pandas as pd
import numpy as np

sys.path.append(
    '/PATH/{}/health_learning_adjusted_years/'.format(getpass.getuser())
)
import support

ATT_DIR = '/PATH'
LEARN_DIR = '/PATH'
HEALTH_FILE = '/PATH'
LT_DIR = '/PATH'


def fetch_attainment(location_id, ihme_loc_id, index_cols, draw_cols):
    '''
    Retrieve attainment data for a given location.
    '''
    df = pd.read_csv('{}/{}.csv'.format(ATT_DIR, ihme_loc_id))

    df['age_group_id'] = df['age_groups'].apply(
        lambda x: int(x.split('_')[1])
    )
    df['location_id'] = location_id
    df.rename(
        index=str, inplace=True, columns=dict(
            zip(
                ['draw' + str(n) for n in range(1, 1001)],
                ['draw_' + str(n) for n in range(0, 1000)]
            )
        )
    )
    df = df.loc[
        (df.age_group_id.isin([6, 7, 8, 9])) &
        (df.sex_id.isin([1, 2]))
    ]

    df['year_id'] = df['cohort'] + (df['age_group_id'] - 5) * 5 + 2
    df = df.loc[
        df.year_id.isin(range(1990, 2017)),
        index_cols + draw_cols
    ]
    df = df.sort_values(index_cols)

    draw_vals = df[draw_cols].values
    draw_vals[draw_vals < 0] = 0
    df = pd.concat(
        [
            df[index_cols].reset_index(drop=True),
            pd.DataFrame(dict(zip(draw_cols, draw_vals.transpose())))
        ],
        axis=1
    )

    df = df[index_cols + draw_cols].sort_values(index_cols)
    df = df.reset_index(drop=True)

    return df


def fetch_learning(location_id, index_cols, draw_cols):
    '''
    Retrieve learning data for a given location.
    '''
    direct_df = pd.read_csv(
        '{}/publication/draws/{}.csv'.format(LEARN_DIR, location_id)
    )
    stream_df = pd.read_csv(
        '{}/draws/{}.csv'.format(LEARN_DIR, location_id)
    )

    df = pd.concat(
        [
            direct_df.loc[
                (direct_df.year_id.isin(range(1990, 2017))) &
                (direct_df.age_group_id.isin([6, 7, 8])) &
                (direct_df.sex_id.isin([1, 2])),
                index_cols + draw_cols
            ],
            stream_df.loc[
                (stream_df.year_id.isin(range(1990, 2017))) &
                (stream_df.age_group_id == 9) &
                (stream_df.sex_id.isin([1, 2])),
                index_cols + draw_cols
            ]
        ]
    )

    df = pd.concat(
        [
            df[index_cols],
            df[draw_cols] / 600.
        ],
        axis=1
    )

    df = df.sort_values(index_cols)
    df = df.reset_index(drop=True)

    return df


def fetch_health_status(location_id, index_cols, draw_cols):
    '''
    Retrieve health status draws.
    '''
    df = pd.read_csv(HEALTH_FILE)

    df = df.loc[
        (df.location_id == location_id) &
        (df.year_id.isin(range(1990, 2017))) &
        (df.age_group_id.isin(range(9, 18))) &
        (df.sex_id.isin([1, 2])),
        index_cols + draw_cols
    ]

    df[draw_cols] = 1 - df[draw_cols]

    df = df.sort_values(index_cols)
    df = df.reset_index(drop=True)

    return df


def learning_sex_agg(att_df, learn_df, index_cols, draw_cols, version_number):
    '''
    Aggregate learning across sexes.
    '''
    df = support.draw_math(
        [att_df.loc[att_df.sex_id.isin([1, 2])], learn_df],
        index_cols, draw_cols, '*'
    )
    df = support.pop_weight_agg(
        df, 'sex_id', [1, 2], 3, index_cols, draw_cols, version_number
    )

    df = support.draw_math(
        [df, att_df.loc[att_df.sex_id == 3]], index_cols, draw_cols, '/'
    )
    df = pd.concat(
        [
            df[index_cols],
            df[draw_cols].fillna(0)
        ],
        axis=1
    )

    learn_df = learn_df.append(df[list(learn_df)])

    learn_df = learn_df[index_cols + draw_cols].sort_values(index_cols)
    learn_df = learn_df.reset_index(drop=True)

    return learn_df


def education_age_agg(att_df, learn_df, index_cols, draw_cols):
    '''
    Aggregate education components across ages.
    '''
    df = support.draw_math(
        [att_df, learn_df], index_cols, draw_cols, '*'
    )
    df['age_group_id'] = 189
    df = df.groupby(index_cols, as_index=False)[draw_cols].sum()

    agg_att_df = att_df.copy()
    agg_att_df['age_group_id'] = 189
    agg_att_df = agg_att_df.groupby(index_cols, as_index=False)[draw_cols].sum()

    agg_learn_df = support.draw_math(
        [df, agg_att_df], index_cols, draw_cols, '/'
    )
    agg_learn_df = pd.concat(
        [
            agg_learn_df[index_cols],
            agg_learn_df[draw_cols].fillna(0)
        ],
        axis=1
    )

    att_df = att_df.append(agg_att_df[list(att_df)])
    learn_df = learn_df.append(agg_learn_df[list(learn_df)])

    df['age_group_id'] = 163
    df = pd.concat(
        [
            df[index_cols],
            df[draw_cols] / 18.
        ],
        axis=1
    )

    return df, att_df, learn_df


def fetch_adj_data(location_id, ihme_loc_id,
                   index_cols, draw_cols, version_number):
    '''
    Combine attainment, learning, and health components to calculate adjustment.
    '''
    att_df = fetch_attainment(location_id, ihme_loc_id, index_cols, draw_cols)

    learn_df = fetch_learning(location_id, index_cols, draw_cols)

    health_df = fetch_health_status(location_id, index_cols, draw_cols)

    att_df = support.pop_weight_agg(
        att_df, 'sex_id', [1, 2], 3, index_cols, draw_cols,
        version_number, return_all=True
    )
    learn_df = learning_sex_agg(
        att_df, learn_df, index_cols, draw_cols, version_number
    )
    health_df = support.pop_weight_agg(
        health_df, 'sex_id', [1, 2], 3, index_cols, draw_cols,
        version_number, return_all=True
    )

    health_df = support.pop_weight_agg(
        health_df, 'age_group_id', range(9, 18), 163, index_cols, draw_cols,
        version_number, return_all=True
    )
    adj_att_df, att_df, learn_df = education_age_agg(
        att_df, learn_df, index_cols, draw_cols
    )

    return adj_att_df, att_df, learn_df, health_df


def calc_years_lived(age_args, nLx_df, lx_df,
                     index_cols, draw_cols, adj_df=None):
    '''
    Create adjusted years lived based on [adjustment factor].

    Use birth cohort total (i.e., 10000).
    '''
    age_group_id, agg_ages = age_args
    nLx_df = nLx_df.loc[nLx_df.age_group_id.isin(agg_ages)].copy()

    if adj_df is not None:
        print('Adjusting data')
        adj_df = adj_df.loc[adj_df.age_group_id.isin(agg_ages)].copy()
        nLx_df = support.draw_math([nLx_df, adj_df], index_cols, draw_cols, '*')

    nLx_df['age_group_id'] = age_group_id
    nLx_df = nLx_df.groupby(index_cols, as_index=False)[draw_cols].sum()

    df = pd.concat(
        [
            nLx_df[index_cols],
            nLx_df[draw_cols] / 100000.
        ],
        axis=1
    )
    df = support.ids_to_int(df, index_cols)

    return df


def fetch_life_table(location_id, index_cols, draw_cols):
    '''
    Retrieve life table for a given location.
    '''
    df = pd.read_csv(
        '{}/lt_{}.csv'.format(LT_DIR, location_id)
    )

    df = df.loc[
        (df.year_id.isin(range(1990, 2017))) &
        (df.age_group_id.isin(
            [28] + range(5, 21) + range(30, 34) + [44,  45, 148]
        )) &
        (df.sex_id.isin([1, 2, 3]))
    ]
    df = support.ids_to_int(df, index_cols)

    df = df[index_cols + ['draw', 'nLx', 'lx']]
    df['draw'] = 'draw_' + df['draw'].astype(str)

    nLx_df = pd.pivot_table(
        df, index=index_cols, values='nLx', columns='draw'
    ).reset_index().rename_axis(None)

    lx_df = pd.pivot_table(
        df, index=index_cols, values='lx', columns='draw'
    ).reset_index().rename_axis(None)

    nLx_df = nLx_df[index_cols + draw_cols].sort_values(index_cols)
    nLx_df = nLx_df.reset_index(drop=True)
    lx_df = lx_df[index_cols + draw_cols].sort_values(index_cols)
    lx_df = lx_df.reset_index(drop=True)

    return nLx_df, lx_df


def fetch_years_lived(health_df, location_id, index_cols, draw_cols):
    '''
    Retrieve life table for a given location.
    '''
    nLx_df, lx_df = fetch_life_table(location_id, index_cols, draw_cols)

    df = calc_years_lived(
        (9, range(9, 18)),
        nLx_df.loc[nLx_df.year_id.isin(range(1990, 2015, 5) + [2016])],
        lx_df.loc[lx_df.year_id.isin(range(1990, 2015, 5) + [2016])],
        index_cols, draw_cols
    )
    df['age_group_id'] = 163

    adj_df = calc_years_lived(
        (9, range(9, 18)),
        nLx_df.loc[nLx_df.year_id.isin(range(1990, 2015, 5) + [2016])],
        lx_df.loc[lx_df.year_id.isin(range(1990, 2015, 5) + [2016])],
        index_cols, draw_cols,
        health_df.loc[health_df.year_id.isin(range(1990, 2015, 5) + [2016])]
    )
    adj_df['age_group_id'] = 163

    return adj_df, df


def summary_store(summ_df, index_cols, draw_cols,
                  summ_type, version_number, location_id):
    '''
    Store summary statistics for dataframe.
    '''
    summ_df['mean'] = summ_df[draw_cols].mean(axis=1)
    summ_df['lower'] = summ_df[draw_cols].quantile(q=0.025, axis=1)
    summ_df['upper'] = summ_df[draw_cols].quantile(q=0.975, axis=1)

    summ_df = summ_df[index_cols + ['mean', 'lower', 'upper']]

    summ_df.to_csv(
        '/PATH/{}/summaries/{}/{}.csv'.format(
            version_number, summ_type, location_id
        ),
        index=False
    )


def main():
    '''
    Calculate expected human capital.
    '''
    parser = argparse.ArgumentParser('Process location draws for HLAYs.')
    parser.add_argument('--location_id', help='ID variable', type=int)
    parser.add_argument('--ihme_loc_id', help='Adapted ISO-3', type=str)
    parser.add_argument('--version_number', help='Version to store', type=str)
    args = parser.parse_args()

    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    draw_cols = ['draw_' + str(n) for n in xrange(1000)]

    adj_att_df, att_df, learn_df, health_df = fetch_adj_data(
        args.location_id, args.ihme_loc_id,
        index_cols, draw_cols, args.version_number
    )

    adj_ex_df, ex_df = fetch_years_lived(
        health_df, args.location_id, index_cols, draw_cols
    )

    df = support.draw_math(
        [
            adj_ex_df.loc[
                adj_ex_df.year_id.isin(range(1990, 2015, 5) + [2016])
            ],
            adj_att_df.loc[
                adj_att_df.year_id.isin(range(1990, 2015, 5) + [2016])
            ]
        ],
        index_cols, draw_cols, '*'
    )

    df.to_csv(
        '/PATH/{}/draws/{}.csv'.format(
            args.version_number, args.location_id
        ),
        index=False
    )

    for store_df, store_name in [(df, 'hlay'),
                                 (ex_df, 'ex'), (health_df, 'health'),
                                 (adj_ex_df, 'adj_ex'),
                                 (att_df, 'att'), (learn_df, 'learn'),
                                 (adj_att_df, 'adj_att')]:
        summary_store(store_df, index_cols, draw_cols,
                      store_name, args.version_number, args.location_id)


if __name__ == '__main__':
    main()