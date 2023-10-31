import pandas as pd
import numpy as np

from shock_prep.utils import dataframe_utils as df_utils
from FILEPATH import apply_map_hierarchy as amh


def _has_side_b(df):
    return (
        df
        .loc[df['source_col'] == "side_b", ['source_event_id']]
        .drop_duplicates()
        .assign(has_side_b=True)
    )


def _sides_equal(df):
    side_a = df['source_col'] == "side_a"
    side_b = df['source_col'] == "side_b"
    return (
        pd.merge(
            df.loc[side_a, ['source_event_id', 'location_id']].drop_duplicates(),
            df.loc[side_b, ['source_event_id', 'location_id']].drop_duplicates(),
            how='outer',
            on=['source_event_id', 'location_id'],
            indicator='sides_equal',
        )
        .groupby('source_event_id', as_index=False)
        .agg({'sides_equal': lambda s: all(s == "both")})
    )


def _assign_dest_col(df):
    return (
        df
        .loc[:, ['source_event_id', 'source_col', 'has_side_b', 'sides_equal']]
        .drop_duplicates()
        .assign(dest_col=lambda d:
                np.where(~d.has_side_b | d.sides_equal,
                         'location_id',
                         d.source_col.map({
                             'location_id': 'side_a',
                             'side_a': 'side_a',
                             'side_b': 'side_b',
                         })
                         )
                )
    )


def resolve_location_and_sides(df):

    def make_func(f):
        if 'use_osm' in f.__code__.co_varnames:
            return lambda d: f(d, use_col='dest_col', use_osm=False)
        else:
            return lambda d: f(d, use_col='dest_col')

    out = df_utils.cascade(
        df,
        dict(keep={'map_type_hierarchy_kept': True}),
        dict(property_map=_has_side_b,
             defaults={'has_side_b': False}),
        dict(property_map=_sides_equal,
             defaults={'sides_equal': False}),
        dict(property_map=_assign_dest_col),
        dict(property_map=make_func(amh._non_duplicated_over_map_types),
             keep_label='_non_dup',
             add_keep_label=True),
        dict(property_map=make_func(amh._has_multiple_unique_locations),
             keep_label='_mul_locs',
             add_keep_label=True),
        dict(property_map=make_func(amh._highest_priority_locations),
             keep_label='_highest_priority',
             add_keep_label=True),
    )

    assert str(out['map_type'].dtype) == "category", (
        "Categorical data type of map_type was dropped"
    )

    return (
        out
        .assign(
            location_side_merge_kept=lambda d:
            d['_non_dup'].fillna(False) &
            (
                ~d['_mul_locs'].fillna(False) |
                d['_highest_priority'].fillna(False)
            )
        )
        .drop(columns=['_non_dup', '_mul_locs', '_highest_priority'])
    )
