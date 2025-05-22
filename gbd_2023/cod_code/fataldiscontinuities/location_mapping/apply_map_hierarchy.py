from shock_prep.utils import trees, dataframe_utils as df_utils


def _get_highest_priority_locations(df, use_osm=True):
    if not use_osm and df['map_type'].nunique() > 1:
        df = df.loc[df['map_type'] != "open_street_maps", :]

    best_map_type = df['map_type'].min()
    best_idxs = df['map_type'] == best_map_type
    best_locs = df.loc[best_idxs, 'location_id']
    if best_map_type == "collaborator" or best_map_type == "manual":
        return set(best_locs)

    kept = set(best_locs)
    for _, group in df.groupby('map_type', sort=True):
        # optimization
        if group.empty:
            continue
        new_locs = trees.filter_locs_for_ancestors_present(group.location_id, kept)
        kept = trees.keep_only_detailed_locs(kept | new_locs)
    return kept


def _locations_are_most_detailed(df, within):
    return (
        df
        .groupby(within, as_index=False)
        .agg({'location_id': lambda l: trees.keep_only_detailed_locs(set(l))})
        .pipe(df_utils.unnest, 'location_id')
    )


def _detailed_locations_within_map_type(d, use_col='source_col'):
    return _locations_are_most_detailed(d, within=['source_event_id', use_col, 'map_type'])


def _non_duplicated_over_map_types(d, use_col='source_col'):
    return (d
            .sort_values('map_type')
            .drop_duplicates(['source_event_id', use_col, 'location_id'],
                             keep='first'))


def _has_multiple_unique_locations(d, use_col='source_col'):
    return (d
            .groupby(['source_event_id', use_col],
                     as_index=False)
            .agg({'location_id': 'count'})
            .query('location_id > 1')
            .loc[:, ['source_event_id', use_col]]
            )


def _highest_priority_locations(df, use_osm=True, use_col='source_col'):

    if df.empty:
        return df[['source_event_id', use_col, 'location_id']]
    return (
        df
        .groupby(['source_event_id', use_col])
        .apply(lambda g: _get_highest_priority_locations(g, use_osm))
        .to_frame(name='location_id')
        .reset_index()
        .pipe(df_utils.unnest, 'location_id')
    )


def apply_mapping_hierarchy(df):
    out = df_utils.cascade(
        df,
        dict(property_map=_detailed_locations_within_map_type,
             keep_label='detailed_locations_within_map_type',
             add_keep_label=True),
        dict(property_map=_non_duplicated_over_map_types,
             keep_label='non_duplicated_over_map_types',
             add_keep_label=True),
        dict(property_map=_has_multiple_unique_locations,
             keep_label='has_multiple_unique_locations',
             add_keep_label=True),
        dict(property_map=_highest_priority_locations,
             keep_label='highest_priority_locations',
             add_keep_label=True),
    )

    return out.assign(
        map_type_hierarchy_kept=lambda d:
        d['detailed_locations_within_map_type'].fillna(False) &
        d['non_duplicated_over_map_types'].fillna(False) &
        (
            ~d['has_multiple_unique_locations'].fillna(False) |
            d['highest_priority_locations'].fillna(False)
        )
    )
