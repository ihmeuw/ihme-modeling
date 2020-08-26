import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import gbd
from get_draws.api import get_draws

from hale.common import path_utils
from hale.common.constants import age_groups, columns


def get_ylds(
        hale_version: int,
        como_version: int,
        location_id: int,
        year_ids: List[int],
        age_group_ids: List[int],
        under_one_age_group_ids: List[int],
        draws: int,
        gbd_round_id: int,
        decomp_step_id: int
) -> pd.DataFrame:
    """
    Pulls YLDs from COMO for given location and years. This involves:
        - Reading the cached population
        - Using get_draws to read COMO draws
        - Aggregating neonatal/birth age groups
        - Subsetting to HALE ages
    """
    # Read cached population.
    population_path = path_utils.get_population_path(hale_version, location_id)
    logging.info(f'Reading cached population from {population_path}')
    population_df = pd.read_feather(population_path)

    # Read YLDs.
    logging.info('Pulling YLDs from COMO draws')
    get_draws_args: Dict[str, Any] = {
        'gbd_id_type': 'cause_id',
        'gbd_id': gbd.constants.cause.ALL_CAUSE,
        'source': 'como',
        'measure_id': gbd.constants.measures.YLD,
        'location_id': location_id,
        'year_id': year_ids,
        'sex_id': [
            gbd.constants.sex.MALE,
            gbd.constants.sex.FEMALE
        ],
        'gbd_round_id': gbd_round_id,
        'decomp_step': gbd.decomp_step.decomp_step_from_decomp_step_id(
            decomp_step_id
        ),
        'version_id': como_version,
        'n_draws': draws,
        'downsample': True
    }
    yld_df = get_draws(**get_draws_args)
    logging.info('Pulled YLDs from COMO draws')

    # Aggregate and subset.
    draw_cols = [col for col in yld_df.columns if 'draw' in col]
    return yld_df\
        .pipe(lambda df: _aggregate(
            df, population_df, draw_cols, under_one_age_group_ids))\
        .reset_index(drop=True)\
        .loc[:, columns.DEMOGRAPHICS + draw_cols]


def _aggregate(
        yld_df: pd.DataFrame,
        pop_df: pd.DataFrame,
        draw_cols: List[str],
        under_one_age_group_ids: List[int]
) -> pd.DataFrame:
    """Aggregates neonatal and birth age groups into <1 year age group"""
    # Multiply by population before aggregating.
    yld_df = yld_df.merge(pop_df, on=columns.DEMOGRAPHICS)
    yld_df[draw_cols] = np.multiply(
        yld_df[draw_cols], yld_df[[columns.POPULATION]]
    )

    # Aggregate neonatal/birth age groups in place.
    yld_df.loc[
        yld_df[columns.AGE_GROUP_ID].isin(under_one_age_group_ids),
        columns.AGE_GROUP_ID
    ] = age_groups.UNDER_ONE
    yld_df = yld_df.groupby(columns.DEMOGRAPHICS, as_index=False).sum()

    # Aggregate sexes while keeping values for specific sex.
    sex_agg_cols = [columns.AGE_GROUP_ID, columns.LOCATION_ID, columns.YEAR_ID]
    sex_agg = yld_df\
        .groupby(sex_agg_cols)\
        .sum()\
        .reset_index()\
        .assign(**{columns.SEX_ID: gbd.constants.sex.BOTH})
    yld_df = yld_df.append(sex_agg, sort=False)

    # Divide by population now that aggregation is over.
    yld_df[draw_cols] = np.divide(
        yld_df[draw_cols], yld_df[[columns.POPULATION]]
    )

    return yld_df
