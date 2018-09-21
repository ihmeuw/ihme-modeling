import os
import sys
import getpass

import pandas as pd

from db_queries import get_location_metadata, get_population

sys.path.append('/PATH')
import cluster_helpers as ch

REPO_DIR = '/PATH/{}/health_learning_adjusted_years/'.format(
    getpass.getuser()
)


def submit_location_draws(location_id, ihme_loc_id, version_number,
                          rerun=False):
    '''
    Run location-specific job.
    '''
    jobname = 'hlays_v{}_{}'.format(version_number, ihme_loc_id)
    worker_script = '{}/hlays_worker.py'.format(REPO_DIR)
    shell_script = '{}/python_shell.sh'.format(REPO_DIR)
    args = {
        '--location_id': location_id,
        '--ihme_loc_id': ihme_loc_id,
        '--version_number': version_number
    }
    draw_path = '/PATH/{}/draws/{}.csv'.format(
        version_number, location_id
    )
    if rerun or not os.path.exists(draw_path):
        job_id = ch.qsub(worker_script, shell_script, 'proj_emergent_science',
                         custom_args=args, name=jobname, slots=4, verbose=True)
        return job_id
    else:
        print(
            'Draws for ' + ihme_loc_id + ' already exist, '\
            'and rerun set to False\n'
        )


def main():
    '''
    Master script for running expected human capital jobs by location.
    '''
    version_number = sys.argv[1]
    version_path = '/PATH/{}'.format(version_number)

    if not os.path.exists('{}/draws/'.format(version_path)):
        os.makedirs('{}/draws/'.format(version_path))
    if not os.path.exists('{}/summaries/'.format(version_path)):
        for dir_type in ['hlay', 'ex', 'health', 'adj_ex', 'att', 'learn']:
            os.makedirs('{}/summaries/{}/'.format(version_path, dir_type))

    if not os.path.exists('{}/loc_df.csv'.format(version_path)):
        loc_df = get_location_metadata(location_set_id=35, gbd_round_id=4)
        loc_df.to_csv(
            '{}/loc_df.csv'.format(version_path), index=False, encoding='utf-8'
        )
    else:
        loc_df = pd.read_csv('{}/loc_df.csv'.format(version_path))
    loc_df = loc_df.loc[loc_df.level == 3]
    loc_args = zip(loc_df['location_id'], loc_df['ihme_loc_id'])

    if not os.path.exists('{}/pop_df.csv'.format(version_path)):
        pop_df = get_population(
            location_id=loc_df.location_id.values.tolist(),
            year_id=range(1990, 2017),
            age_group_id=range(2, 21) + [30, 31, 32, 235, 22],
            sex_id=[1, 2, 3],
            gbd_round_id=4
        )
        pop_df.to_csv('{}/pop_df.csv'.format(version_path), index=False)

    job_ids = []
    for location_id, ihme_loc_id in loc_args:
        job_id = submit_location_draws(location_id, ihme_loc_id, version_number)
        job_ids.append(job_id)


if __name__ == '__main__':
    main()