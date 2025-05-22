import logging

import pandas as pd

from gbd.constants import measures

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import Columns

logger = logging.getLogger(__name__)


def calculate_ylls(
        location_id: int,
        sex_id: int,
        version: MachineParameters
) -> None:
    """
    Use scaled draws and predicted life expectancy to calculate YLLs.
    Draws are stored broken down by location and sex for parallel execution.

    Arguments:
        location_id (int): draws location_id
        sex_id (int): draws sex_id
        version: the machinery parameters set controling the run

    Raises:
        ValueError: if draws do not have matching predicted life expectancy
    """
    # Read in pred ex and aggregated rescaled + shock draws
    pred_ex = version.file_system.read_pred_ex(location_id, sex_id)
    draws = version.file_system.read_aggregated_rescaled_draws(location_id, sex_id)
    shocks = version.file_system.read_aggregated_shock_draws(location_id, sex_id)

    yll_index_cols = [col for col in Columns.INDEX if col != Columns.CAUSE_ID]

    logger.info("Calculating YLLs for non-shocks")
    ylls = _perform_calculation(pred_ex, draws, yll_index_cols, version.draw_cols)

    logger.info("Calculating YLLs for shocks")
    yll_shocks = _perform_calculation(pred_ex, shocks, yll_index_cols, version.draw_cols)

    logger.info("Saving non-shock and shock YLLs")
    version.file_system.save_all_ylls(ylls, yll_shocks, location_id, sex_id)


def _perform_calculation(pred_ex, draws, yll_index_cols, draw_cols):
    """Multiplies pred_ex by every death draw, indexed by loc/year/age/sex."""
    merged = pd.merge(draws, pred_ex, on=yll_index_cols, how='left')
    nan_draws = merged[merged.isnull().any(axis=1)]
    if not nan_draws.empty:
        raise ValueError(
            f'Found draws without matching predicted life expectancy '
            f'for draws:\n{nan_draws[yll_index_cols]}'
        )

    merged[draw_cols] = merged[draw_cols].mul(
        merged[Columns.PRED_EX], axis=0
    )

    merged.drop(Columns.PRED_EX, axis=1, inplace=True)
    merged[Columns.MEASURE_ID] = measures.YLL
    return merged
