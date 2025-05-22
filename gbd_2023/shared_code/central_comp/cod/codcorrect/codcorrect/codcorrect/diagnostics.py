"""Functions for creating before-correction death diagnostics."""

import logging

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import Columns, Diagnostics

logger = logging.getLogger(__name__)


def create_diagnostics(location_id: int, sex_id: int, params: MachineParameters) -> None:
    """Create 'before' diagnostics for CodCorrect, pre-correction.

    Only creates diagnostics for estimation years, DEATHS only.

    Args:
        location_id: location id
        sex_id: sex id
        params: CodCorrect parameters for the run
    """
    # If location is most detailed, there are diagnostics already created for it
    # in the aggregate causes step. If not, we'll read in the aggregated unscaled draws and
    # calculate it here.
    if location_id in params.most_detailed_location_ids:
        logger.info("Reading in existing diagnostics")
        df = params.file_system.read_diagnostics(location_id, sex_id)
    else:
        # Read in unscaled draws and create diagnostic
        logger.info("Reading in aggregated unscaled draws.")
        df = params.file_system.read_aggregated_unscaled_draws(location_id, sex_id)

        df[Columns.DIAGNOSTICS_BEFORE] = df[params.draw_cols].mean(axis=1)

    # Format for upload and save
    df[Columns.CODCORRECT_VERSION_ID] = params.version_id
    df = df[Diagnostics.DataBase.COLUMNS]

    logger.info("Saving diagnostic file")
    params.file_system.save_diagnostics(df, location_id, sex_id)
