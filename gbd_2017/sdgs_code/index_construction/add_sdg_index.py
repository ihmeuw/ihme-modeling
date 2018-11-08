import pandas as pd
import sys
import math
import numpy as np
import os
from scipy.stats import gmean
from scipy.special import (logit, expit)
from scipy import stats
import time
from getpass import getuser

sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
INDICATOR_ID_COLS = ['indicator_id', 'location_id', 'year_id', 'level']
import feather
from db_queries import get_location_metadata as glm


SDG_VERS = dw.SDG_VERS
draw_cols = ['draw_{}'.format(i) for i in range(1000)]
indicator_map = "FILEPATH"
#list of indicators, (integers)
indicators = []

def add_composite_index(df, indicator_ids, index_id, floor=0.01,
                        method='geom_by_target'):
    """Calculate and append an index, using given method

    Parameters
    ----------
        df: pandas DataFrame
            has given structure:
            [id_cols] : [value_cols]
            [dw.INDICATOR_ID_COLS] : [draw_cols]
        indicator_ids : array-like
            A collection of indicator_ids that can be transformed into a set.
            All of these must be present in df, and these will be combined
            using the given method.
        index_id : int
               The indicator_id to assign to the new composite.
        method : str
            method to use, with the following accepted values:
                geom_by_indicator: geometric mean of all indicators
                geom_by_target: hierarchical geometric mean of
                    indicators then targets
                arith_by_indicator: arithmetic mean of all indicators
        floor : float
            Replace any values lower than this with this floor before
            calculating the index.

    Returns
    -------
        out: pandas DataFrame
            all original data and new observations
            that contain the index value by location-year
    """

    # filter to the given indicator ids to calculate index from
    ids_missing_from_data = set(indicator_ids) - set(df.indicator_id)
    assert len(ids_missing_from_data) == 0, \
        'need these to calculate: {}'.format(ids_missing_from_data)
    idx_df = df.copy().loc[df['indicator_id'].isin(indicator_ids)]

    # add the targets if method is geom_by_target
    if method == 'geom_by_target':
        indicator_targets = pd.read_csv(indicator_map)[['indicator_id', 'indicator_target']]  
        indicator_targets = indicator_targets.drop_duplicates()
        idx_df = idx_df.merge(indicator_targets, how='left')
        assert idx_df.indicator_target.notnull().values.all(), \
            'indicator target merge fail'

    # set the indicator_id
    idx_df['indicator_id'] = index_id

    # Establish a floor for calculation purposes
    idx_df.loc[:, draw_cols] = idx_df.loc[:, draw_cols].clip(lower = 0.01)

    # calculate the index based on method
    if method == 'geom_by_indicator':
        idx_df = idx_df.groupby(
            INDICATOR_ID_COLS , as_index=False
        ).agg(gmean)
    elif method == 'geom_by_target':
        # first get geometric means within the targets
        idx_df = idx_df.groupby(
            INDICATOR_ID_COLS  + ['indicator_target'], as_index=False
        ).agg(gmean)
        idx_df = idx_df.drop('indicator_target', axis=1)
        idx_df = idx_df.groupby(
            INDICATOR_ID_COLS ,  as_index=False
        ).agg(gmean)
        # then calculate the geometric means of those targets
    elif method == 'arith_by_indicator':
        idx_df = idx_df.groupby(
            INDICATOR_ID_COLS , as_index=False
        ).mean()
    else:
        raise ValueError(
            'unimplemented index calculation method: {}'.format(method)
        )
    df = df.append(idx_df, ignore_index=True)
    return df[INDICATOR_ID_COLS + draw_cols]

def compile_sdg_index(sdg_version = SDG_VERS, all_indicators = True, indicators_list = indicators):
    """adds another row of indicators representing the SDG Index.  Must define indicators_list if not using all indicators saved in scaled"""
    path = "{idd}/gbd2017/".format(idd=dw.INDICATOR_DATA_DIR)
    input_path = path + "all_indicators_scaled_v{}.feather".format(sdg_version)
    output_path = path + "all_indicators_scaled_v{}_SDG_index.feather".format(sdg_version)
    df = pd.read_feather('/share/scratch/projects/sdg/indicators/gbd2017/all_indicators_unscaled_v{}.feather'.format(SDG_VERS))
    current_ind_id = list(set(new_df.indicator_id))
    if all_indicators:
        sdg_index_df = add_composite_index(df, current_ind_id, 1054)
    else:
        assert len(indicators_list) > 0, "indicators_list must be a list of indicator ids (integer)"
        sdg_index_df = add_composite_index(df, indicators_list, 1054)
    sdg_index_df.to_feather(out_file)

    
