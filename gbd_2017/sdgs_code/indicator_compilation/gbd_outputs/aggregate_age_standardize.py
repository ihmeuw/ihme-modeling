import pandas as pd
import sys
from getpass import getuser

if getuser() == 'USER':
    SDG_REPO = 'FILEPATH'
    
sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

def age_sex_aggregate(df, group_cols, denominator='population'):
    """multiply by population column and aggregate ages"""
    assert denominator in df.columns, '{d} column not in dataframe'.format(d=denominator)
    assert df[denominator].notnull().values.all(), 'merge with {d} failed'.format(d=denominator)
    print("aggregatings age groups and/or sexes")

    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(lambda x: x * df[denominator]),
            df[denominator]
        ],
        axis=1
        )

    # groupby group_cols, and sum
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

    return df


def get_custom_age_weights(age_group_years_start, age_group_years_end):
    """Get age weights scaled to age group start and end"""
    t = qry.get_age_weights()

    t = t.query(
        'age_group_years_start >= {start} & age_group_years_end <= {end}'.format(
            start=age_group_years_start, end=age_group_years_end)
    )
    # scale weights to 1
    t['age_group_weight_value'] =  t['age_group_weight_value'] / t['age_group_weight_value'].sum()

    return t[['age_group_id', 'age_group_weight_value']]


def age_standardize(df, group_cols, age_group_years_start, age_group_years_end, age_group_id=27):
    print("age standardizing")

    wghts = get_custom_age_weights(age_group_years_start, age_group_years_end)
    df = df.merge(wghts, on='age_group_id', how='left')
    assert df.age_group_weight_value.notnull().values.all(), 'merge w wghts failed'

    # multiply by age weights
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(
            lambda x: x * df['age_group_weight_value']
                )
        ],
        axis=1
    )

    # set age_group_id and sum
    df['age_group_id'] = age_group_id
    df = df.groupby(group_cols, as_index=False)[dw.DRAW_COLS].sum()

    return df


def aggregate_locations_to_global(df, group_cols, denominator='population',
                                  age_standardized=False,
                                  age_group_years_start=None,
                                  age_group_years_end=None,
                                  age_group_id=None):
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

    if age_standardized == True:
        standardize_args = [age_group_years_start, age_group_years_end, age_group_id]
        assert all(isinstance(arg, int) for arg in standardize_args), 'age-standardization parameters must be integers'
        df = age_standardize(df, group_cols, age_group_years_start, age_group_years_end, age_group_id)

    return df