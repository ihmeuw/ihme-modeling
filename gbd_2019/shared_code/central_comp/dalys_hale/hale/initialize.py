import logging
import os
import shutil
from typing import List, Optional

import db_queries
import gbd
from gbd.constants import gbd_metadata_type, gbd_process
import gbd_outputs_versions
from gbd_outputs_versions.db import DBEnvironment

import hale
from hale import metadata
from hale.common import path_utils, query_utils
from hale.common.constants import (
    age_groups, columns, connection_definitions, groups, process_ids, years
)


def initialize(
        conn_def: str,
        population_run_id: Optional[int],
        life_table_run_id: Optional[int],
        como_version: Optional[int],
        gbd_round_id: int,
        decomp_step: str,
        location_set_ids: List[int],
        year_ids: Optional[List[int]],
        draws: int
) -> None:
    """
    Runs all initialization tasks needed to prepare for a HALE run:
        - Pull upstream life table, COMO, and population versions.
        - Create a new GBD process version.
        - Create needed directories.
        - Save run metadata.
        - Cache populations.

    See run_initialization.py for details on arguments.
    """
    # Key off of decomp step ID for the rest of this run.
    decomp_step_id = gbd.decomp_step.decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id
    )

    # Set up default year IDs if not present.
    year_ids = year_ids or list(range(
        years.DEFAULT_YEAR_START,
        int(gbd.gbd_round.gbd_round_from_gbd_round_id(gbd_round_id)) + 1
    ))

    # Pull version IDs that need to be saved to GBD process version metadata.
    population_run_id = population_run_id or query_utils.get_run_id(
        gbd_round_id, decomp_step_id, process_ids.POPULATION
    )
    life_table_run_id = life_table_run_id or query_utils.get_run_id(
        gbd_round_id, decomp_step_id, process_ids.LIFE_TABLE_NO_SHOCKS
    )
    como_version = como_version or query_utils.get_como_version(
        gbd_round_id, decomp_step_id
    )

    # Create new GBD process version. This is the HALE version that will be
    # used for this HALE run.
    hale_version = _create_new_process_version(
        gbd_round_id,
        decomp_step,
        population_run_id,
        life_table_run_id,
        como_version,
        conn_def
    )

    # Create the base directories needed for this HALE run and set their
    # permissions appropriately.
    _create_base_directories(hale_version)

    # Save metadata for reproducability and to load from a file for the rest of
    # this run.
    hale_meta = metadata.save_metadata(
        hale_version,
        gbd_round_id,
        decomp_step_id,
        location_set_ids,
        year_ids,
        draws,
        population_run_id,
        life_table_run_id,
        como_version,
        conn_def
    )

    # Create the draw and summary directories needed for this HALE run.
    _create_draw_directories(hale_version, hale_meta.location_ids)

    # Store the population in files to avoid querying the database repeatedly.
    _cache_population(
        hale_version,
        hale_meta.population_run_id,
        hale_meta.location_ids,
        hale_meta.year_ids,
        hale_meta.age_group_ids,
        hale_meta.under_one_age_group_ids,
        hale_meta.gbd_round_id,
        hale_meta.decomp_step_id
    )


def _create_new_process_version(
        gbd_round_id: int,
        decomp_step: str,
        population_run_id: int,
        life_table_run_id: int,
        como_version: int,
        conn_def: str
) -> int:
    """
    Creates a new GBD process version (for HALE, the GBD process version and
    HALE version are one and the same).
    """
    logging.info('Creating new HALE version')
    env = (
        DBEnvironment.DEV if conn_def == connection_definitions.GBD_TEST
        else DBEnvironment.PROD
    )
    hale_version = gbd_outputs_versions.GBDProcessVersion.add_new_version(
        gbd_process_id=gbd_process.HALE,
        gbd_process_version_note=(
            f'HALE, Life table with shocks v{life_table_run_id}, COMO '
            f'v{como_version}, Population v{population_run_id}'
        ),
        code_version=hale.__version__,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        metadata={
            gbd_metadata_type.LIFE_TABLE_WITH_SHOCKS: life_table_run_id,
            gbd_metadata_type.COMO: como_version,
            gbd_metadata_type.POPULATION: population_run_id
        },
        env=env
    ).gbd_process_version_id
    logging.info(f'Created HALE version {hale_version}')
    return hale_version


def _create_base_directories(hale_version: int) -> None:
    """
    Makes base directories for HALE run and temporary files.
    Sets the permissions to univerally readable but only writable by members
    of Central Comp.
    """
    logging.info('Creating base directories')

    # Set umask so that directories are created with 775 by default.
    os.umask(0o0002)

    # Create HALE run root, change group to central comp, and set up directory
    # so that subdirectories and files inherit the central comp group.
    run_root = path_utils.get_hale_run_root(hale_version)
    os.makedirs(run_root)
    shutil.chown(run_root, group=groups.CENTRAL_COMP)
    os.chmod(run_root, 0o2775)

    # Make directories for population files.
    pop_dir = path_utils.get_population_root(hale_version)
    os.makedirs(pop_dir)


def _create_draw_directories(
        hale_version: int,
        location_ids: List[int]
) -> None:
    """Makes directories for HALE outputs"""
    logging.info('Creating draw and summary directories')

    # Make directories for draws and summaries.
    # Draws are stored in directories by location; summaries are stored in a
    # single directory.
    for location_id in location_ids:
        draw_dir = path_utils.get_draws_root(hale_version, location_id)
        os.makedirs(draw_dir)
    summary_dir = path_utils.get_summaries_root(hale_version)
    os.makedirs(summary_dir)


def _cache_population(
        hale_version: int,
        population_run_id: int,
        location_ids: List[int],
        year_ids: List[int],
        age_group_ids: List[int],
        under_one_age_group_ids: List[int],
        gbd_round_id: int,
        decomp_step_id: int
) -> None:
    """Pulls population and saves populations by location"""
    logging.info('Pulling population')
    age_group_ids = under_one_age_group_ids + age_group_ids
    population_df = db_queries\
        .get_population(
            location_id=location_ids,
            year_id=year_ids,
            age_group_id=age_group_ids,
            sex_id='all',
            gbd_round_id=gbd_round_id,
            decomp_step=gbd.decomp_step.decomp_step_from_decomp_step_id(
                decomp_step_id
            ),
            run_id=population_run_id)\
        .drop(columns=columns.RUN_ID)

    # This takes a few minutes, but it speeds up the rest of the HALE run.
    logging.info('Caching population files')
    for location_id in location_ids:
        population_path = path_utils.get_population_path(
            hale_version, location_id
        )
        population_df[
            population_df[columns.LOCATION_ID] == location_id
        ].reset_index(drop=True).to_feather(population_path)

    logging.info('Finished caching population')
