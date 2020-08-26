import sys
import os
from copy import deepcopy
import pandas as pd
import numpy as np
import tblib.pickling_support

from dataframe_io.io_control.h5_io import read_hdf
from gbd.constants import sex

tblib.pickling_support.install()


def name_task(base_name, unique_params):
    unique_name = "_".join(["{" + key + "}" for key in unique_params.keys()
                            ])
    return base_name + "_" + unique_name.format(**unique_params)


class ExceptionWrapper:

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        raise self.ee.with_traceback(self.tb)


def propagate_hierarchy(tree, df, dimension):
    # keep only the most detailed
    thisdf = df.copy()
    leaves = [node.id for node in tree.leaves()]
    thisdf = thisdf[thisdf[dimension].isin(leaves)]
    md = tree.max_depth()
    lvl = md - 1
    while lvl >= 0:
        aggs = []
        for node in tree.level_n_descendants(lvl):
            child_ids = [c.id for c in node.children]
            if child_ids:
                agg = thisdf[thisdf[dimension].isin(child_ids)]
                agg[dimension] = node.id
                agg = agg[~agg.duplicated()]
                aggs.append(agg)
        aggs = pd.concat(aggs)
        thisdf = pd.concat([thisdf, aggs])
        lvl = lvl - 1

    return thisdf


def agg_hierarchy(tree, df, index_cols, data_cols, dimension):
    # keep only the most detailed
    thisdf = deepcopy(df)
    thisdf = thisdf[index_cols + data_cols]
    leaves = [node.id for node in tree.leaves()]
    thisdf = thisdf[thisdf[dimension].isin(leaves)]

    # loop through levels from the most detailed
    md = tree.max_depth()
    lvl = md - 1
    while lvl >= 0:
        aggs = []
        for node in tree.level_n_descendants(lvl):
            child_ids = [c.id for c in node.children]
            if child_ids:
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

            if child_ids:
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
                    maxi_df = parent_df.where((child_df < parent_df),
                                              child_df)
                else:
                    maxi_df = child_df
                aggs.append(maxi_df.reset_index())

        aggs = pd.concat(aggs)
        thisdf = thisdf[~thisdf[dimension].isin(aggs[dimension].unique())]
        thisdf = pd.concat([thisdf, aggs])
        lvl = lvl - 1
    return thisdf


def get_population(cv, location_id=None, year_id=None, age_group_id=None,
                   sex_id=None):
    hdf_filters = {}
    if location_id:
        hdf_filters["location_id"] = location_id
    if year_id:
        hdf_filters["year_id"] = year_id
    if age_group_id:
        hdf_filters["age_group_id"] = age_group_id
    if sex_id:
        hdf_filters["sex_id"] = sex_id

    return read_hdf(fpath=os.path.join(cv.como_dir, "info", "population.h5"),
                    key="draws",
                    hdf_filters=hdf_filters)


def apply_restrictions(restrictions, data_df, draw_cols):
    """apply cause level restrictions"""
    restricted = data_df.merge(restrictions, on="cause_id")
    r_bool = (
        (restricted.age_group_id < restricted.yld_age_start) |
        (restricted.age_group_id > restricted.yld_age_end) |
        ((restricted.sex_id == sex.MALE) & (restricted.male == 0)) |
        ((restricted.sex_id == sex.FEMALE) & (restricted.female == 0)))
    restricted.loc[r_bool, draw_cols] = 0
    return restricted


def draw_from_beta(mean, se, size=1000):
    sample_size = mean * (1 - mean) / se**2
    alpha = mean * sample_size
    beta = (1 - mean) * sample_size
    draws = np.random.beta(alpha, beta, size=size)
    return draws


def cap_val(val_list, ref_list):
    """Given a list of numerical values val_list, find the upper and lower
    nearest neighbors and any interior neighbors in ref_list"""
    assert val_list and (len(ref_list) > 1), (
        "val_list must have at least 1 value, ref_list at least 2")
    lower = [ref for ref in ref_list if all(val >= ref for val in val_list)]
    if lower:
        lower = max(lower)
    else:
        raise ValueError(
            f"The lowest value {min(ref_list)} is greater "
            f"than the lowest value in {min(val_list)}. "
            "Your reference list may need to be updated")
    upper = [ref for ref in ref_list if all(val <= ref for val in val_list)]
    if upper:
        upper = min(upper)
    else:
        raise ValueError(
            f"The greatest value {max(ref_list)} is less "
            f"than the greatest value in {max(val_list)}. "
            "Your reference list may need to be updated")
    inner_list = [
        ref for ref in ref_list
        if min(val_list) < ref < max(val_list)]
    return [lower, upper] + inner_list


def broadcast(broadcast_onto_df, broadcast_df, index_cols,
              operator="*"):
    if operator not in ["*", "+", "-", "/"]:
        raise ValueError(
            "operator must be one of the strings ('*', '+', '-', '/'")
    if not set(broadcast_df.columns).issuperset(set(index_cols)):
        raise ValueError("'broadcast_df' must contain all 'index_cols'")
    if not set(broadcast_onto_df.columns).issuperset(set(index_cols)):
        raise ValueError("'broadcast_onto_df' must contain all 'index_cols'")
    if any(broadcast_df.duplicated(index_cols)):
        raise ValueError(
            "'broadcast_df' must be unique by the columns in declared in"
            "'index_cols'")

    data_cols = [col for col in broadcast_df.columns if col not in index_cols]

    if not set(broadcast_onto_df.columns).issuperset(set(data_cols)):
        raise ValueError(
            "'broadcast_onto_df' must contain all non 'index_cols' from "
            "'broadcast_df'")

    # sort and index the dataframes
    broadcast_onto_df.sort_values(index_cols, inplace=True)
    broadcast_onto_df.set_index(index_cols, inplace=True, drop=True)
    broadcast_df.sort_values(index_cols, inplace=True)
    broadcast_df.set_index(index_cols, inplace=True, drop=True)

    # subset the broadcast_df
    broadcast_df = broadcast_df[
        broadcast_df.index.isin(broadcast_onto_df.index)]

    # Broadcast the accross the data.
    if operator == "*":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] * broadcast_df[data_cols])
    if operator == "+":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] + broadcast_df[data_cols])
    if operator == "-":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] - broadcast_df[data_cols])
    if operator == "/":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] / broadcast_df[data_cols])

    return broadcast_onto_df.reset_index()
