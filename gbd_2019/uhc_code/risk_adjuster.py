import pandas as pd

import argparse

from get_draws.api import get_draws

from uhc_estimation import (misc, specs)


def scale_amen_paf(df, paf_ceiling=0.9):
    '''
    Takes PAFs and rescaled such that highest mean_value is equal to the
    specified `paf_ceiling` value.
    '''
    df['mean_paf'] = df[specs.DRAW_COLS].mean(axis=1)
    df['max_paf'] = df.groupby(
        ['location_id', 'year_id', 'age_group_id', 'sex_id'],
        as_index=False
    ).mean_paf.transform('max')
    df['scaler'] = paf_ceiling / df['max_paf']
    df.loc[df.scaler > 1, 'scaler'] = 1
    df[specs.DRAW_COLS] = (
        df[specs.DRAW_COLS].values.transpose() * df['scaler'].values
    ).transpose()
    df = df.drop(['mean_paf', 'max_paf', 'scaler'], axis=1)

    return df


def delete_risk(df, paf_df, index_cols, draw_cols):
    '''
    Remove effect of local risk exposure.
    '''
    # get local unattribtable
    paf_df[draw_cols] = 1 - paf_df[draw_cols]
    df = misc.draw_math(
        [df, paf_df], index_cols, draw_cols, '*'
    )

    return df


def add_global_risk(df, gpaf_df, index_cols, draw_cols):
    '''
    Add effect of global risk exposure.
    '''
    # get global unattribtable
    assert index_cols == specs.ID_COLS, 'Assumes location and year as index'

    # expand by location-year, average over year, then add year col back on
    gpaf_df = gpaf_df.drop('location_id', axis=1)
    gpaf_df = gpaf_df.merge(df[index_cols])
    gpaf_df = gpaf_df.groupby('location_id', as_index=False)[specs.DRAW_COLS].mean()
    gpaf_df = gpaf_df.merge(df[index_cols])
    gpaf_df[draw_cols] = 1 - gpaf_df[draw_cols]
    df = misc.draw_math(
        [df, gpaf_df], index_cols, draw_cols, '/'
    )

    return df


def cause_age_sex_agg(death_df, true_paf_df, amen_paf_df, draw_parameters, uhc_version_dir):
    '''
    Aggregate PAFs over age and sex, and collapse deaths.
    '''
    # convert to count space
    true_paf_df = misc.draw_math(
        [death_df, true_paf_df],
        specs.ID_COLS + ['age_group_id', 'sex_id', 'cause_id'],
        specs.DRAW_COLS, '*'
    )
    amen_paf_df = misc.draw_math(
        [death_df, amen_paf_df],
        specs.ID_COLS + ['age_group_id', 'sex_id', 'cause_id'],
        specs.DRAW_COLS, '*'
    )

    # agg cause and sex
    true_paf_df = true_paf_df.groupby(
        specs.ID_COLS + ['age_group_id'], as_index=False
    )[specs.DRAW_COLS].sum()
    amen_paf_df = amen_paf_df.groupby(
        specs.ID_COLS + ['age_group_id'], as_index=False
    )[specs.DRAW_COLS].sum()
    death_df = death_df.groupby(
        specs.ID_COLS + ['age_group_id'], as_index=False
    )[specs.DRAW_COLS].sum()

    # now that everything is both sex space, reassign sex_id
    true_paf_df['sex_id'] = 3
    amen_paf_df['sex_id'] = 3
    death_df['sex_id'] = 3

    # age-standardize
    # set counts = False even though we are passing in counts... don't want to convert these to rates (yet)
    true_paf_df = misc.age_standardize(
        true_paf_df, specs.ID_COLS, specs.DRAW_COLS,
        draw_parameters, uhc_version_dir, counts=False
    )
    amen_paf_df = misc.age_standardize(
        amen_paf_df, specs.ID_COLS, specs.DRAW_COLS,
        draw_parameters, uhc_version_dir, counts=False
    )
    death_df = misc.age_standardize(
        death_df, specs.ID_COLS, specs.DRAW_COLS,
        draw_parameters, uhc_version_dir, counts=False
    )

    # convert back to PAF space
    true_paf_df = misc.draw_math(
        [true_paf_df, death_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )
    amen_paf_df = misc.draw_math(
        [amen_paf_df, death_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )

    return death_df, true_paf_df, amen_paf_df