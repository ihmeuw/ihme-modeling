"""Produce COVID mortality scalars.

Defined as final deaths divided by corrected deaths (before adding shocks). Only
run for causes with an indirect COVID model.

Scalars are output
"""
import functools
import glob
import os
from typing import List, Union

import numpy as np
import pandas as pd

import db_queries

from fauxcorrect.job_swarm import task_templates
from fauxcorrect.parameters.machinery import MachineParameters
from fauxcorrect.utils import io
from fauxcorrect.utils.constants import Columns, FilePaths, Jobmon


def calculate_covid_scalars(
    parent_dir: str,
    covid_cause_ids: List[int],
    sex_id: int,
    location_id: int,
    draw_cols: List[str],
    test: bool = False,
) -> None:
    """Calculate COVID mortality scalars.

    Divide final draws by corrected draws and save,
    splitting files out further by cause, resulting in files by
    cause, sex and location that include all age groups and all
    relevant years (2020 and on).

    Also creates draw summaries to be compiled later.

    Args:
        parent_dir: CodCorrect run directory
        covid_cause_ids: all cause ids with an indirect COVID model
        sex_id: sex id, what this task is parallelized by
        location_id: location id, what this task is parallelized by
        draw_cols: list of name of the draw columns
    """
    # COVID currently only applicable in 2020, test draws in 2010
    year_ids = [2020] if not test else [2010]

    num_workers = task_templates.CovidScalarsCalculation.executor_parameters[Jobmon.NUM_CORES]

    # Read in draws, converting draw columns to more distinguishable names
    corrected_draws = _read_corrected_draws(
        parent_dir, covid_cause_ids, sex_id, location_id, year_ids, num_workers
    ).set_index(Columns.INDEX)
    final_draws = _read_final_draws(
        parent_dir, covid_cause_ids, sex_id, location_id, year_ids, num_workers
    ).set_index(Columns.INDEX)

    # Calculate COVID scalars
    scalar_draws = final_draws.divide(corrected_draws).reset_index()

    # Make sure we don't have any missing draws
    missing_draws = scalar_draws[scalar_draws.isnull().any(axis=1)]
    if not missing_draws.empty:
        raise RuntimeError(
            f"There are {len(missing_draws)} rows missing a match between the corrected "
            f"and final draws. First 5 missing rows:\n{missing_draws[:5]}"
        )

    scalar_draws = scalar_draws[Columns.INDEX + draw_cols]

    # Split out files further by cause resulting in cause-sex-location specific files
    io.sink_draws(
        draw_dir=os.path.join(
            parent_dir,
            FilePaths.COVID_SCALARS
        ),
        file_pattern=FilePaths.COVID_SCALARS_FILE_PATTERN,
        draws=scalar_draws
    )

    # Create summaries for review - do be collated later
    scalar_draws[Columns.MEAN] = np.mean(scalar_draws[draw_cols].values, axis=1)
    scalar_draws[Columns.LOWER] = np.percentile(scalar_draws[draw_cols].values, q=2.5, axis=1)
    scalar_draws[Columns.UPPER] = np.percentile(scalar_draws[draw_cols].values, q=97.5, axis=1)

    scalar_draws = scalar_draws[Columns.INDEX + [Columns.MEAN, Columns.LOWER, Columns.UPPER]]
    io.sink_draws(
        draw_dir=os.path.join(
            parent_dir,
            FilePaths.COVID_SCALARS,
            FilePaths.SUMMARY_DIR
        ),
        file_pattern=FilePaths.COVID_SCALARS_SUMMARIES_FILE_PATTERN.format(
            sex_id=sex_id, location_id=location_id
        ),
        draws=scalar_draws
    )


def compile_covid_scalars_summaries(parent_dir: str, version_id: int):
    """Read in all COVID scalars summaries.

    Includes all COVID-affected causes, most detailed locations,
    all age groups, both sexes for year 2020.
    """
    num_workers = task_templates.CovidScalarsCompilation.executor_parameters[Jobmon.NUM_CORES]

    summaries = io.read_from_draw_source(
        draw_dir=os.path.join(
            parent_dir,
            FilePaths.COVID_SCALARS,
            FilePaths.SUMMARY_DIR
        ),
        file_pattern=FilePaths.COVID_SCALARS_SUMMARIES_FILE_PATTERN,
        num_workers=num_workers
    )

    # Order columns
    summaries = summaries[Columns.INDEX + [Columns.MEAN, Columns.LOWER, Columns.UPPER]]

    # Write out a single CSV
    summaries.to_csv(
        os.path.join(
            parent_dir,
            FilePaths.COVID_SCALARS,
            FilePaths.SUMMARY_DIR,
            FilePaths.COVID_SCALARS_SUMMARY_FILE.format(version_id=version_id)
        ),
        index=False
    )

    # Clean up files that have now been condensed
    files = glob.glob(os.path.join(
        parent_dir,
        FilePaths.COVID_SCALARS,
        FilePaths.SUMMARY_DIR,
        "*"
    ))
    for file in files:
        if FilePaths.COVID_SCALARS_SUMMARY_FILE.format(version_id=version_id) not in file:
            os.remove(file)


def _read_corrected_draws(
    parent_dir: str,
    cause_ids: List[int],
    sex_id: int,
    location_id: int,
    year_id: List[int],
    num_workers: int = 1
) -> pd.DataFrame:
    """Read in corrected (no shocks) CodCorrect draws for creating COVID scalars.

    Reads in draws for COVID causes for a sex, location, and year or list of years
    from draw files from correction step. Will include all most-detailed ages.
    """
    draw_dir = os.path.join(
        parent_dir,
        FilePaths.UNAGGREGATED_DIR,
        FilePaths.RESCALED_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.RESCALED_DRAWS_FILE_PATTERN.format(
        sex_id=sex_id, location_id=location_id
    )
    return io.read_from_draw_source(
        draw_dir=draw_dir,
        file_pattern=file_pattern,
        num_workers=num_workers,
        filters={Columns.CAUSE_ID: cause_ids, Columns.YEAR_ID: year_id}
    )


def _read_final_draws(
    parent_dir: str,
    cause_ids: List[int],
    sex_id: int,
    location_id: int,
    year_id: List[int],
    num_workers: int = 5
) -> pd.DataFrame:
    """Read in final CodCorrect draws for creating COVID scalars.

    Reads in draws for COVID causes for a sex, location, and year or list of years
    from draw files from append shocks step. Will include all most-detailed ages.
    """
    draw_dir = os.path.join(
        parent_dir,
        FilePaths.DRAWS_DIR,
        FilePaths.DEATHS_DIR
    )
    file_pattern = FilePaths.APPEND_SHOCKS_FILE_PATTERN.format(
        sex_id=sex_id, location_id=location_id
    )
    return io.read_from_draw_source(
        draw_dir=draw_dir,
        file_pattern=file_pattern,
        num_workers=num_workers,
        filters={Columns.CAUSE_ID: cause_ids, Columns.YEAR_ID: year_id}
    ).drop(columns=Columns.MEASURE_ID)
