import logging

from sqlalchemy import orm

from db_tools import ezfuncs

from hale.common import path_utils
from hale.common.constants import connection_definitions, queries


def get_run_id(gbd_round_id: int, decomp_step_id: int, process_id: int) -> int:
    """
    Gets the best run ID (from the mortality database) associated with a
    GBD round, decomp step, and mortality process.
    """
    logging.info(f'Pulling run ID for process {process_id}')
    run_df = ezfuncs.query(
        queries.GET_RUN_ID,
        conn_def=connection_definitions.MORTALITY,
        parameters={
            'process_id': process_id,
            'gbd_round_id': gbd_round_id,
            'decomp_step_id': decomp_step_id
        }
    )
    if run_df.empty:
        raise RuntimeError(
            f'No best run ID for process {process_id}, GBD round ID '
            f'{gbd_round_id}, decomp_step_id {decomp_step_id}'
        )
    elif len(run_df) > 1:
        raise RuntimeError(
            f'Multiple best run IDs found for process {process_id}, GBD round '
            f'ID {gbd_round_id}, decomp_step_id {decomp_step_id}'
        )
    run_id = int(run_df.at[0, 'run_id'])
    logging.info(f'Pulled run ID {run_id} for process {process_id}')
    return run_id


def get_como_version(gbd_round_id: int, decomp_step_id: int) -> int:
    """
    Gets the best COMO output version ID (from the epi.output_version table)
    associated with a GBD round and decomp step.
    """
    logging.info('Pulling COMO version')
    output_version_df = ezfuncs.query(
        queries.GET_COMO_VERSION_ID,
        conn_def=connection_definitions.EPI,
        parameters={
            'gbd_round_id': gbd_round_id,
            'decomp_step_id': decomp_step_id
        }
    )
    if output_version_df.empty:
        raise RuntimeError(
            'No best COMO output version ID for GBD round ID '
            f'{gbd_round_id}, decomp step ID {decomp_step_id}'
        )
    elif len(output_version_df) > 1:
        raise RuntimeError(
            'Multiple best COMO output version IDs found for GBD round ID '
            f'{gbd_round_id}, decomp step ID {decomp_step_id}'
        )
    como_version = int(output_version_df.at[0, 'output_version_id'])
    logging.info(f'Pulled COMO version {como_version}')
    return como_version


def infile(session: orm.Session, hale_version: int, single: bool) -> None:
    """
    Executes an INFILE into the gbd output table for this HALE run.
    """
    infile_path = path_utils.get_infile_path(hale_version, single=single)
    single_str = 'single' if single else 'multi'
    session.execute(
        queries.INFILE.format(
            path=infile_path,
            single=single_str,
            version=hale_version
        )
    )
