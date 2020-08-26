import copy
import pandas as pd
import os

from functools import lru_cache, reduce

import numpy as np

from db_tools import ezfuncs
from gbd.constants import GBD_COMPARE_AGES

from core_maths.aggregate import aggregate
from ihme_dimensions import dimensionality, gbdize


@lru_cache()
def get_age_spans(age_group_set_id=None):
    if age_group_set_id:
        query = """
            SELECT age_group_id, age_group_years_start, age_group_years_end
            FROM shared.age_group_set_list
            JOIN shared.age_group USING(age_group_id)
            WHERE age_group_set_id={}""".format(age_group_set_id)
    else:
        query = """
            SELECT age_group_id, age_group_years_start, age_group_years_end
            FROM shared.age_group"""
    ags = ezfuncs.query(query, conn_def='cod')

    return ags


def combine_sexes_indf(df, pop):
    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))
    index_cols.remove('sex_id')
    csdf = df.merge(
        pop,
        on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
    csdf = aggregate(
        csdf[index_cols + draw_cols + ['pop_scaled']],
        draw_cols,
        index_cols,
        'wtd_sum',
        weight_col='pop_scaled')
    csdf['sex_id'] = 3
    return csdf


def make_asr(df, index_cols, draw_cols, aw):
    """ Generate age-standardized rates given a DataFrame"""
    # Merge on age weights
    wc = 'age_group_weight_value'
    aadf = df.merge(aw, on='age_group_id', how='left')
    # Assign new age group id
    aadf['age_group_id'] = 27
    # rescale age weights
    aadf.loc[:, "weight_total"] = (aadf.groupby(index_cols)["age_group_weight_value"].transform(sum))
    aadf['age_group_weight_value'] = aadf['age_group_weight_value'] / aadf['weight_total']
    aadf = aadf.drop(['weight_total'], axis = 1)
    assert len(aadf) == len(df), "Uh oh, some pops are missing..."
    # Aggregate with weighted sum of age weights
    aggregation_type = "wtd_sum"
    aadf = aggregate(
        aadf[index_cols + draw_cols + [wc]],
        draw_cols,
        index_cols,
        aggregation_type,
        weight_col=wc,
        normalize=None)
    # Assign new metric_id
    aadf['metric_id'] = 3
    return aadf


def combine_ages(df, pop, aw, gbd_compare_ags=False,
                 age_groups={22: (0, 200), 27: (0, 200)}):
    """
    Returns all ages (22) and age standardized (27) aggregates
    Optionally add gbd compare age groups.

    Returns:
        Dataframe of age aggregated data
    """
    if age_groups is None and not gbd_compare_ags:
        raise ValueError("No aggregate age groups selected. Either pass "
                         "an age_groups dict, and/or select gbd_compare_ags")
    if age_groups is None:
        this_age_groups = {}
    else:
        this_age_groups = copy.deepcopy(age_groups)
    if gbd_compare_ags:
        gbd_ag_df = get_age_spans()
        extra_ages = GBD_COMPARE_AGES + [37, 39, 155, 160, 197, 228, 230, 232,
            243, 284, 285, 286, 287, 288, 289, 420, 430]
        gbd_ag_df = gbd_ag_df.loc[gbd_ag_df.age_group_id.isin(extra_ages)]
        gbd_ag_dict = gbd_ag_df.to_dict('split')
        gbd_ag_dict = {int(x): (float(y), float(z)) for (x, y, z) in gbd_ag_dict['data']}
        this_age_groups.update(gbd_ag_dict)
    ag_basis = get_age_spans()

    index_cols = ['location_id', 'year_id', 'measure_id', 'sex_id']
    if 'cause_id' in df.columns:
        index_cols.append('cause_id')
    if 'sequela_id' in df.columns:
        index_cols.append('sequela_id')
    if 'rei_id' in df.columns:
        index_cols.append('rei_id')
    if 'star_id' in df.columns:
        index_cols.append('star_id')
    draw_cols = list(df.filter(like='draw').columns)

    results = []
    for age_group_id, span in this_age_groups.items():

        # Skip if age group id exists in data
        if age_group_id in df.age_group_id.unique():
            continue

        # Get aggregate age cases
        if age_group_id != 27:
            # Filter to the age groups included in the span
            aadf = df.merge(ag_basis)
            aadf = aadf[
                (span[0] <= aadf.age_group_years_start) &
                (span[1] >= aadf.age_group_years_end)]
            aadf.drop(
                ['age_group_years_start', 'age_group_years_end'],
                axis=1,
                inplace=True)

            # Determine aggregation type and weight column
            wc = 'pop_scaled'
            aggregation_type = "wtd_sum"

            # Merge on population
            len_in = len(aadf)
            aadf = aadf.merge(
                pop,
                on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
            assert len(aadf) == len_in, "Uh oh, some pops are missing..."

            # Aggregate
            aadf = aggregate(
                aadf[index_cols + draw_cols + [wc]],
                draw_cols,
                index_cols,
                aggregation_type,
                weight_col=wc)

            # Assign new age group id and metric_id
            aadf['age_group_id'] = age_group_id
            aadf['metric_id'] = 3
        else:
            aadf = make_asr(df, index_cols + ['age_group_id'], draw_cols,
                            aw)

        results.append(aadf)
    results = pd.concat(results, sort=True)
    return results
