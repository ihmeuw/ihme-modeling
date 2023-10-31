import logging
from typing import List, Tuple

import pandas as pd

from core_maths import summarize as cm_summarize

from hale import metadata
from hale.common import path_utils
from hale.common.constants import columns


def make_summaries(
        hale_df: pd.DataFrame,
        hale_version: int,
        location_id: int,
        draws: int
) -> None:
    """
    Calculates and writes HALE summaries. This involves:
        - Calculating mean, upper, and lower
        - Calculating percent change
        - Writing the summaries by location
    """
    draw_cols = [f'{columns.DRAW}_{draw}' for draw in range(draws)]

    # Save summary statistics.
    summary_path = path_utils.get_hale_summary_path(hale_version, location_id)
    logging.info(f'Calculating summary and writing it to {summary_path}')
    hale_df\
        .copy()[columns.DEMOGRAPHICS + draw_cols]\
        .pipe(lambda df: _calc_mean_upper_lower(df, draw_cols))\
        .loc[:, columns.DEMOGRAPHICS + columns.SUMMARY]\
        .to_csv(summary_path, index=False)

    # Save percent change.
    hale_meta = metadata.load_metadata(hale_version)
    pct_change_path = path_utils.get_hale_percent_change_path(
        hale_version, location_id
    )
    logging.info(
        f'Calculating percent change and writing it to {pct_change_path}'
    )
    hale_df\
        .copy()[columns.DEMOGRAPHICS + draw_cols]\
        .pipe(lambda df: _calc_percent_change(
            df, draw_cols, hale_meta.percent_change_years))\
        .to_csv(pct_change_path, index=False)


def _calc_mean_upper_lower(
        df: pd.DataFrame,
        draw_cols: List[str]
) -> pd.DataFrame:
    return df\
        .pipe(lambda df: cm_summarize.get_summary(df, draw_cols))\
        .rename(columns={
            columns.MEAN: columns.HALE_MEAN,
            columns.LOWER: columns.HALE_LOWER,
            columns.UPPER: columns.HALE_UPPER})\
        .drop(columns=columns.MEDIAN)


def _calc_percent_change(
        df: pd.DataFrame,
        draw_cols: List[str],
        percent_change_years: List[Tuple[int, int]]
) -> pd.DataFrame:
    """Calculates the percent change in HALE between two years"""
    all_pct_change_df = pd.DataFrame(
        columns=columns.PERCENT_CHANGE_DEMO + columns.SUMMARY
    )
    for year_start, year_end in percent_change_years:
        # summarize.pct_change returns a DataFrame with pct_change_means,
        # year_start_id, year_end_id, and percent change columns by draw.
        # Take the percent change mean as-is, then calculate lower and upper
        # on the percent change draws.
        pct_change_df = cm_summarize\
            .pct_change(df, year_start, year_end, columns.YEAR_ID, draw_cols)\
            .pipe(lambda df: _calc_mean_upper_lower(df, draw_cols))\
            .drop(columns=columns.HALE_MEAN)\
            .rename(columns={'pct_change_means': columns.HALE_MEAN})\
            .loc[:, columns.PERCENT_CHANGE_DEMO + columns.SUMMARY]
        all_pct_change_df = all_pct_change_df.append(pct_change_df, sort=False)

    return all_pct_change_df
