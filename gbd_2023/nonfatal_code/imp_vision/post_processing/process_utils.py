"""
Project: GBD Vision Loss
Purpose: Functions for streamlining and organizing manipulations within the
vision loss post processing pipeline.
"""

# ---IMPORTS-------------------------------------------------------------------

import numpy as np
import pandas as pd
from get_draws.api import get_draws

from modeling.etiology_squeeze import config as cfg

# ---FUNCTIONS-----------------------------------------------------------------

def get_vision_draws(meid,
                     measure_id,
                     location_id,
                     age_group_id,
                     release_id):
    """
    Pull draws for vision loss models.

    Arguments:
        meid (int): modelable entity id
        measure_id (int): measure
        location_id (int): location
        age_group_id (intlist): The age_group_ids to use.
        release_id (int): The release of GBD to use.

    Returns:
        A dataframe of draws
    """
    draws = get_draws(gbd_id_type='modelable_entity_id',
                      gbd_id=meid,
                      source="epi",
                      measure_id=measure_id,
                      location_id=location_id,
                      age_group_id=age_group_id,
                      release_id=release_id)
    return draws


def rename_draw_cols(df, append_str):
    """
    Renames draw columns by appending an identifier on to the new column name.

    Arguments:
        df (pandas DataFrame): a dataframe of draws
        append_str (str): string to append to "draw_{draw_num}" in the draw
            columns.

    Returns:
        A dataframe with new name for draw columns.
    """

    df = df.copy()
    draws = [x for x in df.columns if 'draw' in x]
    renames = ["draw_{i}_{r}".format(i=i, r=append_str) for i in range(1000)]
    df.rename(columns=dict(list(zip(sorted(draws, key=lambda x: x[5]),
              sorted(renames, key=lambda x: x[5])))), inplace=True)
    return df
