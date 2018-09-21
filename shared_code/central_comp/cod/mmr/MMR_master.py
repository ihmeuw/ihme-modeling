import subprocess
import os
import time
import sys

from db_queries.get_cause_metadata import get_cause_metadata
from db_process_upload import Uploader
sys.path.insert(1, str(os.getcwd()).rstrip('/mmr'))
import maternal_fns


def change_permission(in_dir, recursively=True):
    if recursively:
        change_permission_cmd = ['chmod',
                                 '-R', '775',
                                 in_dir]
    else:
        change_permission_cmd = ['chmod',
                                 '775',
                                 in_dir]
    print ' '.join(change_permission_cmd)
    subprocess.check_output(change_permission_cmd)


def set_out_dirs(process_vers):
    out_dir = ('FILEPATH/{}'
               .format(process_vers))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    new_folders = ['multi_year', 'multi_year/summaries',
                   'single_year', 'single_year/draws', 'single_year/summaries']
    for folder in new_folders:
        directory = '{}/{}'.format(out_dir, folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
    change_permission(out_dir)
    return '{}/single_year'.format(out_dir), '{}/multi_year'.format(out_dir)


def launch_mmr_jobs(years, causes, process_v, mmr_out_dir):
    for year in years:
        for cause in causes:
            call = ('qsub -cwd -P proj_custom_models -N "part1_{}_{}" -l '
                    'mem_free=12G -pe multi_slot 6 -o FILEPATH '
                    '-e FILEPATH cluster_shell.sh '
                    'calculate_mmr.py "{}" "{}" "{}"'
                    .format(cause, year, cause, year, mmr_out_dir))
            subprocess.call(call, shell=True)


def launch_arc_jobs(causes, arc_output_dir):
    for cause in causes:
        call = ('qsub -cwd -P proj_custom_models -N "part2_{}" -l '
                'mem_free=20G -pe multi_slot 10 '
                '-o FILEPATH'
                '-e FILEPATH cluster_shell.sh '
                'calculate_arc.py "{}" "{}"'
                .format(cause, cause, arc_out_dir))
        subprocess.call(call, shell=True)


def launch_upload_jobs(upload_types, process_vers, gbd_round_id, conn_def):
    for u_type in upload_types:
        if u_type == 'single':
            in_dir = mmr_out_dir + r'/summaries'
        else:
            in_dir = arc_out_dir + r'/summaries'
        call = ('qsub -cwd -P proj_custom_models -N "part3_{}" '
                '-l mem_free=10G -pe multi_slot 5 '
                '-o FILEPATH '
                '-e FILEPATH cluster_shell.sh '
                'db_process_upload.py "{}" "{}" "{}" "{}" "{}"'
                .format(u_type, u_type, process_vers, gbd_round_id, conn_def,
                        in_dir))
        subprocess.call(call, shell=True)
        time.sleep(5)


if __name__ == '__main__':
    gbd_round_id, conn_def = sys.argv[1:3]
    years = range(1990, 2017)
    cause_df = get_cause_metadata(8)
    causes = cause_df.ix[(cause_df.most_detailed == 1) | (cause_df.level == 0)
                         ].cause_id.unique().tolist()
    upload_types = ['single', 'multi']

    process_vers = Uploader(int(gbd_round_id), conn_def).prep_upload(conn_def)
    mmr_out_dir, arc_out_dir = set_out_dirs(process_vers)

    launch_mmr_jobs(years, causes, process_vers, mmr_out_dir)
    maternal_fns.wait('part1', 300)
    launch_arc_jobs(causes, arc_out_dir)
    maternal_fns.wait('part2', 300)
    launch_upload_jobs(upload_types, process_vers, gbd_round_id, conn_def)
