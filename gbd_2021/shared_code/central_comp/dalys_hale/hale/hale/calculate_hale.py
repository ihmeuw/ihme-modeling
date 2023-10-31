import concurrent.futures
import logging
from typing import List

import numpy as np
import pandas as pd

import gbd

from hale import life_tables, metadata, summarize, ylds
from hale.common import path_utils
from hale.common.constants import age_groups, columns


def calculate(hale_version: int, location_id: int) -> None:
    """Pulls life table and YLDs to calculate HALE and HALE summaries"""
    hale_meta = metadata.load_metadata(hale_version)

    life_table_df = None
    yld_df = None
    with concurrent.futures.ProcessPoolExecutor(2) as executor:
        life_table_future = executor.submit(
            life_tables.get_life_table,
            hale_meta.life_table_run_id,
            location_id,
            hale_meta.year_ids,
            hale_meta.age_group_ids,
            hale_meta.draws
        )
        yld_future = executor.submit(
            ylds.get_ylds,
            hale_version,
            hale_meta.como_version,
            location_id,
            hale_meta.year_ids,
            hale_meta.age_group_ids,
            hale_meta.under_one_age_group_ids,
            hale_meta.draws,
            hale_meta.gbd_round_id,
            hale_meta.decomp_step_id
        )
        life_table_df, yld_df = life_table_future.result(), yld_future.result()

    hale_df = _calculate_hale(
        life_table_df,
        yld_df,
        hale_version,
        hale_meta.draws,
        location_id,
        hale_meta.year_ids,
        hale_meta.age_group_ids
    )
    summarize.make_summaries(
        hale_df, hale_version, location_id, hale_meta.draws
    )


def _calculate_hale(
        life_table_df: pd.DataFrame,
        yld_df: pd.DataFrame,
        hale_version: int,
        draws: int,
        location_id: int,
        year_ids: List[int],
        age_group_ids: List[int]
) -> pd.DataFrame:
    """
    Calculates HALE from life tables and YLDs. HALE calculation involves:
        - Using Tx instead of nLx for age group 95+
        - Adjusting Lx to nLx * (1 - YLDs)
        - Calculating adjusted Tx by summing adjusted Lx for prior age groups
        - Calculating HALE as adjusted Tx / Lx
        - Copying HALE for age group 28 to age group 22
        - Writing the HALE draws by location and year
    """
    logging.info('Calculating HALE')

    # Make lists of draw columns for each value used in HALE calculation.
    tx_cols = [f'{columns.TX}_{draw}' for draw in range(draws)]
    nlx_cols = [f'{columns.NLX}_{draw}' for draw in range(draws)]
    lx_cols = [f'{columns.LX}_{draw}' for draw in range(draws)]
    adj_lx_cols = [f'{columns.ADJ_LX}_{draw}' for draw in range(draws)]
    adj_tx_cols = [f'{columns.ADJ_TX}_{draw}' for draw in range(draws)]
    yld_cols = [f'{columns.DRAW}_{draw}' for draw in range(draws)]
    hale_cols = [f'{columns.DRAW}_{draw}' for draw in range(draws)]

    # Merge life table with YLDs for easier operations on the same DataFrame.
    hale_df = life_table_df.merge(yld_df, on=columns.DEMOGRAPHICS)

    # For terminal age group, use Tx instead of nLx.
    terminal = hale_df[columns.AGE_GROUP_ID] == age_group_ids[-1]
    hale_df.loc[terminal, nlx_cols] = hale_df.loc[terminal, tx_cols].values

    # Adjust Lx.
    hale_df[adj_lx_cols] = np.multiply(
        hale_df[nlx_cols], 1 - hale_df[yld_cols]
    )

    # Sort dataframe in descending order by age. age_group_ids is sorted
    # in ascending order, so use its reversed indices to sort the dataframe.
    age_group_sort_order = list(range(len(age_group_ids), 0, -1))
    hale_df[columns.AGE_GROUP_SORT_ORDER] = hale_df[
        columns.AGE_GROUP_ID
    ].map(dict(zip(age_group_ids, age_group_sort_order)))
    hale_df.sort_values(by=columns.AGE_GROUP_SORT_ORDER, inplace=True)

    # Calculate HALE.
    # Set adjusted Tx to the cumulative sum of adjusted lx, then divide by lx.
    hale_df[adj_tx_cols] = hale_df\
        .groupby(
            [columns.LOCATION_ID, columns.YEAR_ID, columns.SEX_ID],
            as_index=False)[adj_lx_cols]\
        .cumsum()
    hale_df[hale_cols] = np.divide(hale_df[adj_tx_cols], hale_df[lx_cols])

    # Prep columns and age groups for output.
    hale_df[columns.CAUSE_ID] = gbd.constants.cause.ALL_CAUSE
    all_age = hale_df[hale_df[columns.AGE_GROUP_ID] == age_groups.UNDER_ONE]
    all_age[columns.AGE_GROUP_ID] = gbd.constants.age.ALL_AGES
    hale_df = pd.concat([hale_df, all_age], sort=False)

    # Write HALE output by location and year.
    hale_draws_root = path_utils.get_draws_root(hale_version, location_id)
    logging.info(f'Writing HALE outputs to {hale_draws_root}')
    for year_id in year_ids:
        hale_df.loc[
            hale_df[columns.YEAR_ID] == year_id,
            columns.DEMOGRAPHICS + [columns.CAUSE_ID] + hale_cols
        ].to_csv(
            path_utils.get_hale_draws_path(hale_version, location_id, year_id),
            index=False
        )

    return hale_df
