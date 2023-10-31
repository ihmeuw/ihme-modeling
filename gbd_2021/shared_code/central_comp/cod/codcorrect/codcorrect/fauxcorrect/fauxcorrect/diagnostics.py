"""Functions for creating before-correction death diagnostics."""
import logging
import os
import subprocess
from typing import List

import pandas as pd

from draw_sources.draw_sources import DrawSource
from gbd import constants as gbd_constants

from fauxcorrect.utils.constants import (
    FilePaths, Columns, Diagnostics
)
from fauxcorrect.parameters.machinery import MachineParameters


def create_diagnostics(location_id: int, sex_id: int, params: MachineParameters) -> None:
    """Create 'before' diagnostics for CodCorrect, pre-correction.

    Only creates diagnostics for estimation years, DEATHS only.

    Args:
        location_id: location id
        sex_id: sex id
        params: CodCorrect parameters for the run
    """
    diagnostic_file = os.path.join(
        params.parent_dir,
        FilePaths.DIAGNOSTICS_DIR,
        FilePaths.DIAGNOSTICS_DETAILED_FILE_PATTERN.format(
            location_id=location_id, sex_id=sex_id
        )
    )

    # If location is most detailed, there are diagnostics already created for it
    # in the append causes step. If not, we'll read in the aggregated unscaled draws and
    # calculate it here.
    if location_id in params.most_detailed_location_ids:
        logging.info(f"Reading in existing diagnostics at {diagnostic_file}")
        df = pd.read_csv(diagnostic_file)
        df = df.loc[df[Columns.YEAR_ID].isin(gbd_constants.ESTIMATION_YEARS)]
    else:
        # Read in unscaled draws and create diagnostic
        logging.info(f"Reading in aggregated unscaled draws.")
        df = read_aggregated_unscaled(
            params.parent_dir, location_id, sex_id, gbd_constants.ESTIMATION_YEARS
        )
        df[Columns.DIAGNOSTICS_BEFORE] = df[params.draw_cols].mean(axis=1)

    format_for_upload_and_save(df, diagnostic_file, params)


def read_aggregated_unscaled(
    parent_dir: str,
    location_id: int,
    sex_id: int,
    year_ids: List[int]
) -> pd.DataFrame:
    """Read in location aggregates of unscaled draws for deaths only."""
    unscaled_params = {
        'draw_dir': os.path.join(
            parent_dir,
            FilePaths.AGGREGATED_DIR,
            FilePaths.UNSCALED_DIR
        ),
        'file_pattern': FilePaths.AGGREGATED_UNSCALED_FILE_PATTERN
    }
    ds = DrawSource(unscaled_params)
    unscaled_draws = ds.content(filters={
        "location_id": location_id,
        "sex_id": sex_id,
        "year_id": year_ids,
        "measure_id": gbd_constants.measures.DEATH
    })
    return unscaled_draws


def format_for_upload_and_save(
    df: pd.DataFrame,
    file_path: str,
    params: MachineParameters
) -> None:
    """Formats df for diagnostic upload and saves to file_path."""
    # Format for upload
    df[Columns.CODCORRECT_VERSION_ID] = params.version_id
    df = df[Diagnostics.DataBase.COLUMNS]

    # Save, overwriting if already exists
    logging.info("Saving diagnostic file")
    df.to_csv(file_path, index=False)

    # Change permissions and double check
    permissions_change = ['chmod', '775', file_path]
    subprocess.check_output(permissions_change)

    logging.info('All done!')
