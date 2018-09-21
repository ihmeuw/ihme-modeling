import pandas as pd
import numpy as np


def agg_hierarchy(tree, df, index_cols, data_cols, dimension):
    # keep only the most detailed
    thisdf = df.copy()
    thisdf = thisdf[index_cols + data_cols]
    leaves = [node.id for node in tree.leaves()]
    thisdf = thisdf[thisdf[dimension].isin(leaves)]

    # loop through levels from the most detailed
    group_cols = [col for col in index_cols if col != dimension]
    md = tree.max_depth()
    lvl = md - 1
    while lvl >= 0:
        aggs = []
        for node in tree.level_n_descendants(lvl):
            child_ids = [c.id for c in node.children]
            if len(child_ids) > 0:
                agg = thisdf[thisdf[dimension].isin(child_ids)]
                group_cols = [col for col in index_cols if col != dimension]
                if not group_cols:
                    group_cols = [dimension]
                    agg[dimension] = node.id
                agg = agg.groupby(group_cols).sum().reset_index()
                agg[dimension] = node.id
                aggs.append(agg)
        aggs = pd.concat(aggs)
        thisdf = pd.concat([thisdf, aggs])
        lvl = lvl - 1

    # combine
    thisdf = thisdf.groupby(index_cols).sum().reset_index()
    return thisdf


def maximize_hierarchy(tree, df, index_cols, data_cols, dimension):
    # keep only the most detailed
    thisdf = df.copy()
    thisdf = thisdf[index_cols + data_cols]

    # loop through levels from the most detailed
    md = tree.max_depth()
    lvl = md - 1
    while lvl >= 0:
        aggs = []
        for node in tree.level_n_descendants(lvl):
            child_ids = [c.id for c in node.children]
            if len(child_ids) > 0:
                child_df = thisdf[thisdf[dimension].isin(child_ids)]
                group_cols = [col for col in index_cols if col != dimension]
                if not group_cols:
                    group_cols = [dimension]
                    child_df[dimension] = node.id
                child_df = child_df.groupby(group_cols).max().reset_index()
                child_df[dimension] = node.id

                # maximize
                child_df.sort_values(index_cols, inplace=True)
                child_df.set_index(index_cols, inplace=True)
                parent_df = thisdf[thisdf[dimension] == node.id]

                if not parent_df.empty:
                    parent_df.sort_values(index_cols, inplace=True)
                    parent_df.set_index(index_cols, inplace=True)
                    maxi_df = parent_df.where((child_df < parent_df), child_df)
                else:
                    maxi_df = child_df
                aggs.append(maxi_df.reset_index())

        aggs = pd.concat(aggs)
        thisdf = thisdf[~thisdf[dimension].isin(aggs[dimension].unique())]
        thisdf = pd.concat([thisdf, aggs])
        lvl = lvl - 1
    return thisdf


def apply_restrictions(restrictions, data_df, draw_cols):
    """apply cause level restrictions"""
    restricted = data_df.merge(restrictions, on="cause_id")
    r_bool = (
        (restricted.age_group_id < restricted.yld_age_start) |
        (restricted.age_group_id > restricted.yld_age_end) |
        ((restricted.sex_id == 1) & (restricted.male == 0)) |
        ((restricted.sex_id == 2) & (restricted.female == 0)))
    restricted.ix[r_bool, draw_cols] = 0
    return restricted


def draw_from_beta(mean, se, size=1000):
    sample_size = mean * (1 - mean) / se**2
    alpha = mean * sample_size
    beta = (1 - mean) * sample_size
    draws = np.random.beta(alpha, beta, size=size)
    return draws


def cap_val(val_list, ref_list):
    """Given a list of numerical values val_list,
    find the upper and lower nearest neighbors
    and any interior neighbors in ref_list"""
    assert (len(val_list) > 0) and (len(ref_list) > 1), (
        "val_list must have at least 1 value, ref_list at least 2")
    lower = [ref for ref in ref_list if all(val >= ref for val in val_list)]
    if len(lower) > 0:
        lower = max(lower)
    else:
        raise ValueError(
            "The lowest value {min_reflist} is greater "
            "than the lowest value in {min_valist}. "
            "Your reference list may need to be updated".format(
                min_reflist=min(ref_list),
                min_valist=min(val_list)))
    upper = [ref for ref in ref_list if all(val <= ref for val in val_list)]
    if len(upper) > 0:
        upper = min(upper)
    else:
        raise ValueError(
            "The greatest value {max_reflist} is less "
            "than the greatest value in {max_valist}. "
            "Your reference list may need to be updated".format(
                max_reflist=max(ref_list),
                max_valist=max(val_list)))
    inner_list = [
        ref for ref in ref_list
        if ref > min(val_list) and ref < max(val_list)]
    return [lower, upper] + inner_list
