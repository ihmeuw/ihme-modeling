from typing import List

import pandas as pd

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import Columns


def append_shocks(
        measure_ids: List[int],
        location_id: int,
        sex_id: int,
        version: MachineParameters
) -> None:
    """
    Add yll and death shocks (location-aggregated) to re/scaled ylls and
    re/scaled deaths (also location-aggregated).
    Draws are stored broken down by location and sex for parallel execution.

    Arguments:
        measure_ids (list): the measure_ids included in this run
        location_id (int): draws' location_id
        sex_id (int): draws' sex_id
        version: MachineParameters version
    """
    for measure_id in measure_ids:
        # Read in non-shocks and shocks separately
        scaled = version.file_system.read_aggregated_rescaled_draws(
            location_id, sex_id=sex_id, measure_id=measure_id
        )
        shocks = version.file_system.read_aggregated_shock_draws(
            location_id, sex_id=sex_id, measure_id=measure_id
        )

        # Append shocks and save
        new_scaled = _append_shocks(scaled, shocks, version.draw_cols)
        version.file_system.save_appended_shock_draws(
            new_scaled, location_id, sex_id, measure_id
        )


def _append_shocks(
        scaled_data: pd.DataFrame,
        shock_data: pd.DataFrame,
        draw_cols: List[str]
) -> pd.DataFrame:
    """Given a deaths/yll dataframe, add shocks to it."""
    df = pd.concat([scaled_data, shock_data]).reset_index(drop=True)
    df = (
        df[Columns.INDEX + [Columns.MEASURE_ID] + draw_cols]
        .groupby(Columns.INDEX + [Columns.MEASURE_ID])
        .sum()
        .reset_index()
    )

    negatives = df[(df[draw_cols] < 0).any(axis=1)]
    if not negatives.empty:
        raise RuntimeError(
            f"After appending shocks, there are negative draws :\n{negatives}"
        )

    return df
