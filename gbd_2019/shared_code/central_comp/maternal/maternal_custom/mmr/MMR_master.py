"""
Maternal Mortality Ratio, or MMR, is a demographic-specific ratio of:

    MMR = ((maternal deaths / births) * 100,000).

MMR is generated for all maternal most detailed causes, as well as the parent
maternal death cause

Outputs for this script are pointed to:

FILEPATH{process_version_id}

and logs are written to:

FILEPATH
"""

import os
import pathlib
import subprocess
import sys
import time

sys.path.append("..")

from db_queries import get_cause_metadata
from db_tools.ezfuncs import query
from gbd.decomp_step import decomp_step_id_from_decomp_step

from db_process_upload import Uploader
import maternal_fns
import mmr_constants


def change_permission(in_dir, recursively=True):
    if recursively:
        change_permission_cmd = ['chmod',
                                 '-R', '775',
                                 in_dir]
    else:
        change_permission_cmd = ['chmod',
                                 '775',
                                 in_dir]
    print(' '.join(change_permission_cmd))
    subprocess.check_output(change_permission_cmd)


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


def get_best_codcorrect_vers(decomp_step, gbd_round_id):
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


def launch_save_birth_estimate_job(gbd_round_id, decomp_step, process_vers):
    call = (
        'qsub -cwd -P PROJECT -N "save_birth_estimates" -l '
        'fmem=5G,fthread=2,h_rt=24:00:00 -q all.q -o '
        'FILEPATH '
        '-e FILEPATH cluster_shell.sh '
        'save_birth_estimates.py '
        '--gbd-round-id {} --decomp-step {} --process-vers {}'.format(
            gbd_round_id, decomp_step, process_vers))
    subprocess.call(call, shell=True)


def launch_mmr_jobs(
    years, causes, process_v, mmr_out_dir,
    cod_process_v, decomp_step, gbd_round_id):
    for year in years:
        for cause in causes:
            call = (
                'qsub -cwd -P PROJECT -N "mmr_calculate_mmr_{}_{}" '
                '-l fmem=12G,fthread=10,h_rt=24:00:00 -q all.q '
                '-o FILEPATH '
                '-e FILEPATH cluster_shell.sh '
                'calculate_mmr.py "{}" "{}" "{}" "{}" "{}" "{}"'.format(
                    cause, year, cause, year, mmr_out_dir,
                    cod_process_v, decomp_step, gbd_round_id))
            subprocess.call(call, shell=True)


def launch_pct_change_jobs(causes, arc_output_dir):
    for cause in causes:
        call = (
            'qsub -cwd -P PROJECT -N "mmr_calculate_pct_change_{}" -l '
            'fmem=20G,fthread=10,h_rt=24:00:00 -q all.q '
            '-o FILEPATH '
            '-e FILEPATH cluster_shell.sh '
            'calculate_arc.py "{}" "{}"'.format(
                cause, cause, arc_output_dir))
        subprocess.call(call, shell=True)


def launch_upload_jobs(
    upload_types, mmr_out_dir, arc_out_dir,
    process_vers, gbd_round_id, conn_def):
    for u_type in upload_types:
        if u_type == 'single':
            in_dir = mmr_out_dir + r'/summaries'
        else:
            in_dir = arc_out_dir + r'/summaries'
        call = ('qsub -cwd -P PROJECT -N "mmr_upload_{}" '
                '-l fmem=10G,fthread=5,h_rt=24:00:00 -q all.q '
                '-o FILEPATH '
                '-e FILEPATH cluster_shell.sh '
                'db_process_upload.py "{}" "{}" "{}" "{}"'
                .format(u_type, u_type, process_vers, conn_def, in_dir))
        subprocess.call(call, shell=True)
        time.sleep(5)


def launch_sdg(process_vers):
    call = ('qsub -cwd -P PROJECT -N "part4_sdg_MMR" '
            '-l fmem=30G,fthread=15,h_rt=24:00:00 -q all.q '
            '-o FILEPATH '
            '-e FILEPATH cluster_shell.sh '
            '../save_custom_results.py "{}" "{}" "{}" "{}" "{}" "{}" "{}" "{}"'
            .format(
                process_vers,
                'FILEPATH{}/single_year/draws'.format(process_vers),
                'draws_{year_id}_366.h5',
                1990, 2019, 'sdg', 'MMR {}'.format(process_vers), 4))
    subprocess.call(call, shell=True)


if __name__ == '__main__':

    print("Initiating script.")
    decomp_step, gbd_round_id, conn_def = sys.argv[1:4]
    gbd_round_id = int(gbd_round_id)

    print("Getting cause metdata")
    cause_df = get_cause_metadata(8, gbd_round_id=gbd_round_id)

    print("Getting causes")
    # only most-detailed and root cause
    causes = cause_df.loc[
        (cause_df.most_detailed == 1) | (cause_df.level == 0)
    ].cause_id.unique().tolist()
    codcorrect_vers = get_best_codcorrect_vers(decomp_step, gbd_round_id)

    print("setting process version")
    process_vers = Uploader(
        conn_def, codcorrect_vers, decomp_step, int(gbd_round_id)).prep_upload()
    process_vers =14774
    mmr_out_dir, arc_out_dir = set_out_dirs(process_vers)

    print("Launching save_birth_estimates")
    launch_save_birth_estimate_job(
        gbd_round_id, decomp_step, process_vers)
    maternal_fns.wait('save_birth_estimates', 60)
    print("Launching calculate_mmr jobs")
    launch_mmr_jobs(
        mmr_constants.OUTPUT_YEARS, causes, process_vers, mmr_out_dir,
        codcorrect_vers, decomp_step, gbd_round_id)
    maternal_fns.wait('mmr_calculate_mmr', 300)
    print("Launching calculate_arc jobs")
    launch_pct_change_jobs(causes, arc_out_dir)
    maternal_fns.wait('mmr_calculate_pct_change', 300)
    print("Uploading!")
    launch_upload_jobs(
        mmr_constants.UPLOAD_TYPES, mmr_out_dir, arc_out_dir,
        process_vers, gbd_round_id, conn_def)
    print("Done with gbd outputs upload, now running SDG upload")
    launch_sdg(process_vers)
