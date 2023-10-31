"""Pipeline management module for the creation of Maternal Mortality Ratio.

Maternal Mortality Ratio, or MMR, is a demographic-specific ratio of:

    MMR = ((maternal deaths / births) * 100,000).

MMR is generated for all maternal most detailed causes, as well as the parent
maternal death cause (cause_id 366). NOTE the parent marternal cause MMR is
also saved and uploaded to cause_id 294, the all-cause cause_id.

Outputs for this script are pointed to:

FILEPATH

and logs are written to:

FILEPATH
"""

import logging
import os
import pathlib
import subprocess
import sys
import time
from typing import List, Optional

import click

from db_queries import get_cause_metadata
from db_tools.ezfuncs import query
from gbd.decomp_step import decomp_step_id_from_decomp_step

sys.path.append(str(pathlib.Path(__file__).parents[1]))  # noqa; TODO: package!

from mmr.db_process_upload import Uploader
import maternal_fns
import mmr_constants


@click.command()
@click.option('--decomp-step', type=str, required=True)
@click.option('--gbd-round-id', type=int, required=True)
@click.option(
    '--db-env', type=click.Choice(['prod', 'dev']), default='prod',
    help='dev or prod run of MMR.', show_default=True)
@click.option(
    '--process-version-id', type=int, default=None,
    help='To resume for given process version, pass process_version_id', show_default=True)
@click.option(
    '--codcorrect-version-id', type=int, default=None,
    help='To hardcode a non-bested codcorrect_version_id', show_default=True)
@click.option('--debug', is_flag=True)
@click.option('--pdb', is_flag=True)
def cli(**kwargs):
    pdb_post_mortem_run = kwargs.pop('pdb')
    if pdb_post_mortem_run:
        try:
            main(**kwargs)
        except Exception as err:
            print(f'Exception found, in namespace as "err":\n{err}')
            import pdb
            pdb.post_mortem()
    else:
        main(**kwargs)


def main(
    decomp_step: str,
    gbd_round_id: int,
    db_env: str,
    process_version_id: Optional[int],
    debug: bool,
    codcorrect_version_id: Optional[int],
):
    _setup_logging(logging.DEBUG if debug else logging.INFO)
    logging.info("Running MMR")
    os.chdir(pathlib.Path(__file__).resolve().parent)
    logging.info("Running from directory {}".format(os.getcwd()))

    logging.info("Getting cause metdata")
    cause_df = get_cause_metadata(
        mmr_constants.MATERNAL_CAPSTONE_CAUSE_SET_ID,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)

    logging.info("Filtering causes")
    # only most-detailed and root cause in maternal hierarchy
    causes = cause_df.loc[
        (cause_df.most_detailed == 1) | (cause_df.level == 0)
    ].cause_id.unique().tolist()
    logging.info("Running for {} causes".format(len(causes)))

    logging.info("Getting best codcorrect version")
    codcorrect_vers = get_best_codcorrect_vers(
        decomp_step, gbd_round_id, codcorrect_version_id
    )
    logging.info("Using codcorrect version {}".format(codcorrect_vers))

    if not process_version_id:
        logging.info("Making process version")
        process_version_id = Uploader(
            codcorrect_vers, decomp_step, gbd_round_id, db_env
        ).prep_upload()
    mmr_out_dir, arc_out_dir = set_out_dirs(process_version_id)
    logging.info("Using process version {}".format(process_version_id))

    logging.info("Launching save_birth_estimates")
    launch_save_birth_estimate_job(
        gbd_round_id, decomp_step, process_version_id, db_env)
    maternal_fns.wait('save_birth_estimates', 60)

    logging.info("Launching calculate_mmr jobs")
    launch_mmr_jobs(
        mmr_constants.OUTPUT_YEARS, causes, process_version_id, mmr_out_dir,
        codcorrect_vers, decomp_step, gbd_round_id)
    maternal_fns.wait('mmr_calculate_mmr', 300)

    logging.info("Launching calculate_arc jobs")
    launch_pct_change_jobs(causes, arc_out_dir)
    maternal_fns.wait('mmr_calculate_pct_change', 300)

    logging.info("Uploading!")
    launch_upload_jobs(
        mmr_constants.UPLOAD_TYPES,
        mmr_out_dir, arc_out_dir,
        process_version_id, codcorrect_vers,
        gbd_round_id, decomp_step, db_env)
    maternal_fns.wait('mmr_upload', 300)

    logging.info("Done with gbd outputs upload, now running SDG upload")
    launch_sdg(process_version_id)

    logging.info("Done with everything")


def change_permission(in_dir, recursively=True):
    permission_cmd = ['chmod', '775', in_dir]
    if recursively:
        permission_cmd.insert(1, '-R')
    logging.info(' '.join(permission_cmd))
    subprocess.check_output(permission_cmd)


def set_out_dirs(process_vers):
    out_dir = str(pathlib.Path(mmr_constants.OUTDIR) / str(process_vers))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    for folder in mmr_constants.MMR_SUBDIRECTORIES:
        directory = '{}/{}'.format(out_dir, folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
    change_permission(out_dir)
    return '{}/single_year'.format(out_dir), '{}/multi_year'.format(out_dir)


def get_best_codcorrect_vers(decomp_step, gbd_round_id, cc_version: int = None):
    if cc_version is not None:
        return cc_version
    decomp_step_id = decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id)
    params = {
        "decomp_step_id": decomp_step_id,
        "gbd_round_id": gbd_round_id
    }
    q = """
        SELECT MAX(CAST(output_version_id AS UNSIGNED)) as id
        FROM cod.output_version
        WHERE status = 1 AND is_best = 1
        AND decomp_step_id = :decomp_step_id
        AND code_version = :gbd_round_id
    """

    result = query(q, parameters=params, conn_def='cod')
    return int(result.at[0, 'id'])


def launch_save_birth_estimate_job(gbd_round_id, decomp_step, process_vers, db_env):
    call = (
        'qsub -cwd -P proj_centralcomp -N "save_birth_estimates-{}" -l '
        'fmem=5G,fthread=2,h_rt=24:00:00 -q all.q -o '
        'FILEPATH/maternal '
        '-e FILEPATH/maternal cluster_shell.sh '
        'save_birth_estimates.py '
        '--gbd-round-id {} '
        '--decomp-step {} '
        '--process-vers {} '
        '--db-env {} '
    ).format(
        _get_pretty_time(), gbd_round_id, decomp_step, process_vers, db_env
    )
    subprocess.call(call, shell=True)
    logging.debug('save_birth_estimate call:\n{}'.format(call))


def launch_mmr_jobs(
    years, causes, process_v, mmr_out_dir,
    cod_process_v, decomp_step, gbd_round_id
):
    for year in years:
        for cause in causes:
            call = (
                'qsub -cwd -P proj_centralcomp -N "mmr_calculate_mmr_{}_{}-{}" '
                '-l fmem=12G,fthread=10,h_rt=24:00:00 -q all.q '
                '-o FILEPATH/maternal '
                '-e FILEPATH/maternal cluster_shell.sh '
                'calculate_mmr.py "{}" "{}" "{}" "{}" "{}" "{}"'.format(
                    _get_pretty_time(),
                    cause, year, cause, year, mmr_out_dir,
                    cod_process_v, decomp_step, gbd_round_id))
            subprocess.call(call, shell=True)
    logging.debug('last mmr call:\n{}'.format(call))


def launch_pct_change_jobs(causes, arc_output_dir):
    for cause in causes:
        call = (
            'qsub -cwd -P proj_centralcomp -N "mmr_calculate_pct_change_{}-{}" -l '
            'fmem=20G,fthread=10,h_rt=24:00:00 -q all.q '
            '-o FILEPATH/maternal '
            '-e FILEPATH/maternal cluster_shell.sh '
            'calculate_arc.py "{}" "{}"'.format(
                cause, _get_pretty_time(), cause, arc_output_dir))
        subprocess.call(call, shell=True)
    logging.debug('last pct_change call:\n{}'.format(call))


def launch_upload_jobs(
    upload_types: List[str],
    mmr_out_dir: str,
    arc_out_dir: str,
    process_vers: int,
    codcorrect_version: int,
    gbd_round_id: int,
    decomp_step: str,
    db_env: str
):
    for u_type in upload_types:
        if u_type == 'single':
            in_dir = mmr_out_dir + r'/summaries'
        else:
            in_dir = arc_out_dir + r'/summaries'
        call = (
            'qsub -cwd -P proj_centralcomp -N "mmr_upload_{upload_type}-{time}" '
            '-l fmem=10G,fthread=5,h_rt=24:00:00 -q all.q '
            '-o FILEPATH/maternal '
            '-e FILEPATH/maternal '
            'cluster_shell.sh db_process_upload.py '
            '--upload-type {upload_type} '
            '--process-vers {process_vers} '
            '--in-dir {in_dir} '
            '--codcorrect-version {codcorrect_version} '
            '--gbd-round-id {gbd_round_id} '
            '--decomp-step {decomp_step} '
            '--db-env {db_env}'
        ).format(
            upload_type=u_type,
            time=_get_pretty_time(),
            process_vers=process_vers,
            in_dir=in_dir,
            codcorrect_version=codcorrect_version,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            db_env=db_env
        )
        subprocess.call(call, shell=True)
        time.sleep(5)
    logging.debug('last upload call:\n{}'.format(call))


def launch_sdg(process_vers):
    call = (
        'qsub -cwd -P proj_centralcomp -N "part4_sdg_MMR-{}" '
        '-l fmem=30G,fthread=15,h_rt=24:00:00 -q all.q '
        '-o FILEPATH/maternal '
        '-e FILEPATH/maternal cluster_shell.sh '
        '../save_custom_results.py "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'
    ).format(
        _get_pretty_time(), process_vers,
        'FILEPATH/draws'.format(process_vers),
        'draws_{year_id}_366.h5',
        1990, 2019,
        'sdg',
        'MMR {}'.format(process_vers),
        4
    )
    subprocess.call(call, shell=True)
    logging.debug('sdg call:\n{}'.format(call))


# TODO: refactor below functions to utilites module when this is packaged
def _get_pretty_time():
    return time.strftime('%y%m%d-%H%M%S')


def _setup_logging(log_level):
    """Define root logger for stream and filesystem logging."""
    fmt_string = '%(asctime)s - %(message)s'
    logfile_name = 'mmr_main_{}.log'.format(_get_pretty_time())
    logging.basicConfig(level=log_level, format=fmt_string)
    # add file handler to root logger
    root_logger = logging.getLogger()
    file_hdlr = logging.FileHandler(pathlib.Path(mmr_constants.LOGDIR) / 'try2' / logfile_name)
    file_hdlr.setFormatter(logging.Formatter(fmt_string))
    file_hdlr.setLevel(log_level)
    root_logger.addHandler(file_hdlr)


if __name__ == '__main__':
    cli()
