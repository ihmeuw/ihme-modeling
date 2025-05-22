"""
Project: GBD Hearing Loss
Purpose: Functios for making etiology level adjustments.
"""

import sys

import numpy as np
import pandas as pd

from modeling import config as cfg

from core.transform import (transform_draws, fill_draws)
from core.quality_checks import (check_df_for_nulls,
                            check_shape,
                            merge_check,
                            check_occurances_for_df)


def apply_meningitis_prop(meningitis_df, prop, merge_cols):
    """

    Arguments:
        meningitis_df (Dataframe): Pandas dataframe containing draws of
            meningitis estimates to crosswalk and split into mild and moderate
        prop (Dataframe): Dataframe of proportion estimates used to crosswalk
            and split mild and moderate Meningitis.
        merge_cols (strlist): The column or columns to merge two dataframe of
            draws on for draw level transformations.

    Returns:
        Dataframe of crosswalked meningitis estimates that have been split for
        mild and moderate.

    """
    meng_sevs = []

    # Split out mild (meningitis * (1 - prop)
    transformation_variable = fill_draws(df=meningitis_df, fill_val=1)
    transformation_variable['entity'] = 'meng'
    mld_prop = transform_draws(operator_name='sub',
                               df1=transformation_variable,
                               df2=prop,
                               merge_cols=merge_cols)
    meng_mld = transform_draws(operator_name='mul',
                               df1=meningitis_df,
                               df2=mld_prop,
                               merge_cols=merge_cols)
    meng_mld['severity'] = 'mld'
    meng_sevs.append(meng_mld)

    # Split out moderate (meningitis * prop).
    meng_mod = transform_draws(operator_name='mul',
                               df1=meningitis_df,
                               df2=prop,
                               merge_cols=merge_cols)
    meng_mod['severity'] = 'mod'
    meng_sevs.append(meng_mod)

    # Assign moderate+severe, severe, profound and complete aggregate to
    # cross walked moderate estimates.

    for fill_sev in ['mod_sev', 'sev', 'pro', 'com']:
        higher_sevs = meng_mod.copy()
        higher_sevs['severity'] = fill_sev
        meng_sevs.append(higher_sevs)

    xwalked_meng = pd.concat(meng_sevs, axis=0)

    return xwalked_meng


def split_otitis(ot_df, ot_prop, mld_ot_merge_cols, mod_ot_merge_cols):
    """
    Apply beta distribution to Otitis to split out mild and moderate
    Otitis from the Otitis etiology.

    Arguments:
        ot_df (Dataframe): Pandas dataframe containing draws of
            Otitis estimates to split into mild and moderate
        ot_prop (Dataframe): Dataframe of proportion estimates used to
            split mild and moderate Otitis.
        mld_ot_merge_cols (strlist): The column or columns to merge two
            dataframe of draws on for draw level transformations to mild Otitis.
        mod_ot_merge_cols (strlist): The column or columns to merge two
            dataframe of draws on for draw level transformations to moderate
            Otitis.

    Returns:
        Dataframe of crosswalked meningitis estimates that have been split for
        mild and moderate.

    """

    ot_sevs = []

    # Split out mild Otitis from the Otitis etiology (etiology * prop)
    ot_mld = transform_draws(operator_name='mul',
                             df1=ot_df,
                             df2=ot_prop,
                             merge_cols=mld_ot_merge_cols)
    ot_mld['severity'] = 'mld'
    ot_sevs.append(ot_mld)

    # Split out moderate Otitis (etiology * (1 - prop)).
    transformation_variable = fill_draws(df=ot_df, fill_val=1)
    ot_mod_prop = transform_draws(operator_name='sub',
                                  df1=transformation_variable,
                                  df2=ot_prop,
                                  merge_cols=mod_ot_merge_cols)
    ot_mod = transform_draws(operator_name='mul',
                             df1=ot_df,
                             df2=ot_mod_prop,
                             merge_cols=mod_ot_merge_cols)
    ot_mod['severity'] = 'mod'
    ot_sevs.append(ot_mod)
    split_ot = pd.concat(ot_sevs, axis=0)

    split_ot = check_df_for_nulls(df=split_ot, entity_name='split otitis')
    check_occurances_for_df(df=split_ot,
                            cols=['age_group_id',
                                  'sex_id',
                                  'location_id',
                                  'year_id'])

    return split_ot
