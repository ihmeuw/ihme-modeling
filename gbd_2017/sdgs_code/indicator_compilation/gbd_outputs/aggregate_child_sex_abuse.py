import pandas as pd
import sys
from getpass import getuser
from os import rename, listdir

if getuser() == 'USER':
    SDG_REPO = 'FILEPATH'
sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
import sdg_utils.queries as qry


POP_FILE = 'FILEPATH'
component_ids = [20, 23]
group_cols_past = ['location_id', 'year_id', 'measure_id', 'metric_id', 'age_group_id', 'sex_id']
group_cols_future = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'scenario']
NATS = qry.get_sdg_reporting_locations(level_3=True)


def add_denom_rows(df, agg_var, agg_dim, agg_parent, agg_children=None):
    '''
    Create expanded demographics dataframe for denominators.
    '''
    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'scenario']

    agg_df = df.copy()
    if agg_children is not None:
        agg_df = agg_df.loc[agg_df[agg_dim].isin(agg_children)]
    agg_df[agg_dim] = agg_parent
    agg_df = agg_df.groupby(
        index_cols,
        as_index=False
    )[agg_var].sum()
    df = df.append(agg_df[list(df)])

    return df


def load_population():
    df = pd.read_csv(POP_FILE)
    # add on global
    df = add_denom_rows(df, 'population', 'location_id', 1)

    # add on both sexes
    df = add_denom_rows(df, 'population', 'sex_id', 3)

    # add on all ages
    df = add_denom_rows(df, 'population', 'age_group_id', 22)

    # add on under-5
    df = add_denom_rows(df, 'population', 'age_group_id', 1, [2, 3, 4, 5])

    #add 10-19
    df = add_denom_rows(df, 'population', 'age_group_id', 162, [7,8])
    
    # add on 10-24
    df = add_denom_rows(df, 'population', 'age_group_id', 159, [7, 8, 9])
    
    # add on 10-plus
    df = add_denom_rows(df, 'population', 'age_group_id', 194, [7, 8, 9, 10, 11,12, 13, 14, 15,16, 17, 18, 19, 20, 30, 31, 32, 235])
    return df


def aggregate_locations_to_global(df, group_cols, denominator='population'):
    """multiply by population column and aggregate sexes and ages"""
    assert denominator in df.columns, '{d} column not in dataframe'.format(d=denominator)
    assert df[denominator].notnull().values.all(), 'merge with {d} failed'.format(d=denominator)
    print("aggregatings locations")

    # select only level 3 locations
    locs = qry.get_sdg_reporting_locations(level_3 = True)
    df = df[df.location_id.isin(locs.location_id)]

    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x * df[denominator]),
            df[denominator]
        ],
        axis=1
        )

    # set location_id, groupby group_cols, and sum
    df['location_id'] = 1
    df = df.groupby(group_cols, as_index=False)[dw.DRAW_COLS + [denominator]].sum()

    # return to appropriate metric
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x / df[denominator]),
            df[denominator]
        ],
        axis=1
        )

    df.drop('population', axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def sex_aggregate_components(group_cols, version):
    """multiply by population column and aggregate sexes"""
    path = '{dd}dismod/{v}'.format(dd=dw.INPUT_DATA_DIR, v=version)

    dfs = []
    for component_id in component_ids:
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather('{p}/{id}.feather'.format(p=path, id=component_id))
        dfs.append(df)

    # concat dfs and merge population
    df = pd.concat(dfs, ignore_index=True)
    
    pops = qry.get_pops()
    df = df.merge(pops, how='left')
    assert df.population.notnull().values.all(), 'merge with pops fail'
    
    print("aggregatings sexes")
    df = df[group_cols + dw.DRAW_COLS + ['population']]
    df['sex_id'] = 3
    
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x * df['population']),
            df['population']
        ],
        axis=1
        )

    # sums the rows by sex
    df = df.groupby(group_cols, as_index=False)[dw.DRAW_COLS + ['population']].sum()

    # return to appropriate metric
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x / df['population'])
        ],
        axis=1
        )

    print("outputting feather")
    df.to_feather('{p}/1064_age_disagg.feather'.format(p=path))


def age_location_aggregate(past_future, group_cols, version):
    """multiply by population column and aggregate ages"""
    if past_future == 'past':
        path = '{dd}dismod/{v}'.format(dd=dw.INPUT_DATA_DIR, v=version)
        pops = qry.get_pops()
    elif past_future == 'future':
        path = '{dd}dismod/{v}'.format(dd=dw.FORECAST_DATA_DIR, v=version)
        pops = load_population()
    else:
        raise ValueError('The past_future arg must be set to "past" or "future".')

    print("reading file")
    df = pd.read_feather(path + '/' + '1064_age_disagg.feather')

    df = df.merge(pops, how='left')
    assert df.population.notnull().values.all(), 'merge with pops fail'
    
    print("aggregatings ages")
    df.loc[:,'age_group_id'] = 202
    
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x * df['population']),
            df['population']
        ],
        axis=1
        )

    # sums the rows by sex
    df = df.groupby(group_cols, as_index=False)[dw.DRAW_COLS + ['population']].sum()

    # return to appropriate metric
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x / df['population']),
            df['population']
        ],
        axis=1
        )

    df_global = df.copy(deep=True)

    print("outputting feather")
    df.drop('population', axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_feather('{p}/1064.feather'.format(p=path))

    # global
    df_global = aggregate_locations_to_global(df_global, group_cols=group_cols)
    df_global.to_feather('{p}/1064_global.feather'.format(p=path))

    return df


if __name__ == '__main__':
    sex_aggregate_components(group_cols = group_cols, version = dw.DISMOD_VERS)
    age_location_aggregate(past_future='past', group_cols=group_cols_past, version=dw.DISMOD_VERS)
    age_location_aggregate(past_future='future', group_cols=group_cols_future, version=dw.DISMOD_VERS)