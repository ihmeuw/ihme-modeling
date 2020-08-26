import logging
from typing import List

import pandas as pd

from ihme_dimensions import dfutils

from hale.common import path_utils
from hale.common.constants import age_groups, columns


def get_life_table(
        life_table_run_id: int,
        location_id: int,
        year_ids: List[int],
        age_group_ids: List[int],
        draws: int
) -> pd.DataFrame:
    """
    Pulls life table for given location and years. This involves:
        - Reading life table draws for a given mortality life table run
        - Reshaping life table draws from long to wide
        - Downsampling (if necessary)
        - Replacing the 95-99 age group (33) with 95+ (235)
        - Subsetting to HALE ages and given years
    """
    life_table_path = path_utils.get_life_table_path(
        life_table_run_id, location_id
    )
    terminal_id = age_group_ids[-1]
    logging.info(f'Pulling life table from {life_table_path}')
    life_table_df = pd\
        .read_csv(life_table_path)\
        .pipe(_long_to_wide)\
        .pipe(lambda df: _downsample(df, draws))\
        .assign(**{
            columns.AGE_GROUP_ID: lambda df: df[columns.AGE_GROUP_ID].replace(
                {age_groups.FROM_95_TO_99: terminal_id})})\
        .query(f'{columns.AGE_GROUP_ID} in @age_group_ids')\
        .query(f'{columns.YEAR_ID} in @year_ids')\
        .reset_index(drop=True)
    logging.info('Pulled life table')
    return life_table_df


def _long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts long life table dataframe to wide. Result is a DataFrame with
    columns for each life table draw in the form of
    {life_table_column}_{draw_number}. See this SO post for implementation
    details: https://stackoverflow.com/questions/14507794/
    """
    df = df[
        columns.DEMOGRAPHICS + [columns.DRAW] + columns.LIFE_TABLE
    ].pivot_table(
        index=columns.DEMOGRAPHICS,
        columns=columns.DRAW,
        values=columns.LIFE_TABLE
    )
    df.columns = [f'{col}_{draw}' for col, draw in df.columns.values]
    return df.reset_index()


def _downsample(df: pd.DataFrame, draws: int) -> pd.DataFrame:
    """
    If this run is for fewer draws than the number of draws in the life tables,
    downsample to number of draws HALE is running at.
    """
    life_table_draws = len([col for col in df.columns if columns.TX in col])
    if life_table_draws == draws:
        return df
    elif life_table_draws < draws:
        raise RuntimeError(
            f'Trying to run at {draws} draws but life table only has '
            f'{life_table_draws} draws'
        )
    for col in columns.LIFE_TABLE:
        draw_prefix = col + '_'
        df = dfutils.resample(df, draws, draw_prefix)
    return df
