import argparse
import datetime
import getpass
import os
import pandas as pd
import sqlalchemy as sql
import subprocess
import time
 
import sys

from db_tools.ezfuncs import query

from shock_aggregator.core import (write_json,
                                   read_json)
from shock_aggregator.database import get_best_model_version
from shock_aggregator.database import (get_cause_hierarchy_version,
                                       get_cause_hierarchy,
                                       get_cause_metadata)
from shock_aggregator.database import (get_location_hierarchy_version,
                                       get_location_hierarchy,
                                       get_location_metadata)
from shock_aggregator.database import create_new_shock_version_row
from shock_aggregator.database import upload_models
from shock_aggregator.database import (unmark_best,
                                       mark_best,
                                       update_status)
from shock_aggregator.restrictions import expand_id_set
from shock_aggregator.restrictions import (get_eligible_age_group_ids,
                                           get_eligible_sex_ids)
from shock_aggregator.submit_jobs import Task, TaskList

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../..'))

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('output_version_id', type=str)
    parser.add_argument('-r', '--resume',
                        help='Resume run of Shock Aggregator',
                        action='store_true')
    parser.add_argument('-b', '--best',
                        help='Mark best', action='store_true')
    args = parser.parse_args()
    output_version_id = args.output_version_id
    resume = args.resume
    best = args.best
    print("output_version_id: {}".format(output_version_id))
    print("resume: {}".format(resume))
    print("mark best: {}".format(best))
    return output_version_id, resume, best


def set_up_folders(output_directory, output_version_id='new'):
    new_folders = ['_temp', 'draws', 'logs', 'summaries']

    for folder in new_folders:
        directory = '{d}/{v}/{f}'.format(d=output_directory,
                                         v=output_version_id,
                                         f=folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
    return '{d}/{v}'.format(d=output_directory, v=output_version_id)


def generate_shock_jobs(task_list, code_directory, log_directory, locations,
                        resume=False):

    for location_id in locations:
        job_name = 'shocks-{}-{}'.format(location_id, output_version_id)
        job_command = [
            '-o', 'FILEPATH',
            '-e', 'FILEPATH',
            "FILEPATH",
            'FILEPATH',
            '--output_version_id', str(output_version_id),
            '--location_id', str(location_id)]

        job_log = '{ld}/shocks_{v}_{lid}.txt'.format(v=output_version_id,
                                                     ld=log_directory,
                                                     lid=location_id)
        job_project = ''
        job_mem = 40
        job_dependencies = []
        task_list.add_task(
            Task(job_name, job_command, job_log, job_project,
                 mem=job_mem, resume=resume),
            job_dependencies)
    return task_list


def generate_location_aggregation_jobs(task_list, code_directory,
                                       log_directory, location_data,
                                       resume=False):
    """Generate one job for each location."""
    max_location_hierarchy = location_data.loc[
        location_data['is_estimate'] == 1, 'level'].max()
    for level in range(max_location_hierarchy, 0, -1):
        parent_location_ids = [
            location_id for location_id in location_data.loc[
                location_data['level'] == level, 'parent_id']
            .drop_duplicates()]

        for location_id in parent_location_ids:
            job_name = 'agg-location-{}-{}'.format(location_id,
                                                   output_version_id)
            job_command = [
                '-o', ('FILEPATH'),
                '-e', ('FILEPATH'),
                "FILEPATH",
                'FILEPATH',
                '--output_version_id', str(output_version_id),
                '--location_id', str(location_id)]

            job_log = '{ld}/agg_location_{v}_{lid}.txt'.format(
                v=output_version_id, ld=log_directory, lid=location_id)



            job_project = 'proj_shocks'
            job_mem = 150
            job_dependencies = []
            for child_id in location_data.loc[
                (location_data['parent_id'] == location_id) &
                (location_data['level'] == level), 'location_id'
            ].drop_duplicates():
                if len(location_data.loc[(
                    location_data['location_id'] == child_id) &
                   (location_data['most_detailed'] == 1)]) == 1:
                    job_dependencies.append('shocks-{}-{}'.format(
                        child_id, output_version_id))
                else:
                    job_dependencies.append('agg-location-{}-{}'.format(
                        child_id, output_version_id))
            task_list.add_task(
                Task(job_name, job_command, job_log, job_project,
                     mem=job_mem, resume=resume),
                job_dependencies)
    return task_list


if __name__ == '__main__':

    code_directory = "FILEPATH"
    output_directory = 'FILEPATH'

    output_version_id, resume, best = parse_args()
    parent_dir = set_up_folders(output_directory, output_version_id)

    if not resume:
        (cause_agg_set_version_id,
         cause_agg_metadata_version_id) = get_cause_hierarchy_version(3, 2019)
        cause_aggregation_hierarchy = get_cause_hierarchy(
            cause_agg_set_version_id)
        cause_data = cause_aggregation_hierarchy.copy(deep=True)


        (location_set_version_id,
         location_metadata_version_id) = get_location_hierarchy_version(
             21, 2019)
        location_data = get_location_hierarchy(location_set_version_id)
        location_metadata = get_location_metadata(location_metadata_version_id)
        eligible_location_ids = location_data.loc[
            location_data['most_detailed'] == 1, 'location_id'].tolist()

        location_name_data = query("SELECT * FROM ADDRESS",
                                   conn_def='cod')
        cause_name_data = query("SELECT * FROM ADDRESS",
                                conn_def='cod')
        age_name_data = query("SELECT * FROM ADDRESS",
                              conn_def='cod')

        valid_causes = [729,945,387,699,707,693,695,727,854,842,711,703,
                        302,335,341,408,345,843,357,725,726]


        # Save helper files
        cause_aggregation_hierarchy.to_csv(
            os.path.join(parent_dir, 'FILEPATH'),
            index=False)
        location_data.to_csv(
            os.path.join(parent_dir, 'FILEPATH'),
            index=False)

        config = {}
        config['envelope_column'] = 'envelope'
        config['envelope_pop_column'] = 'pop'
        config['index_columns'] = ['location_id', 'year_id', 'sex_id',
                                   'age_group_id', 'cause_id']
        config['data_columns'] = ['draw_{}'.format(x) for x in range(1000)]
        config['eligible_location_ids'] = eligible_location_ids
        config['eligible_year_ids'] = list(range(1950, 2020))

        write_json(config, os.path.join(parent_dir, '_temp/config.json'))
    else:

        location_data = pd.read_csv(
            os.path.join(parent_dir, '_temp/location_hierarchy.csv'))

        config = read_json(os.path.join(parent_dir, '_temp/config.json'))

        eligible_location_ids = config['eligible_location_ids']
        eligible_year_ids = config['eligible_year_ids']

    sh_agg_job_list = TaskList()
    sh_agg_job_list = generate_shock_jobs(
        sh_agg_job_list,
        code_directory,
        os.path.join(parent_dir, 'logs'),
        eligible_location_ids,
        resume=resume)
    sh_agg_job_list = generate_location_aggregation_jobs(
        sh_agg_job_list,
        code_directory,
        os.path.join(parent_dir, 'logs'),
        location_data,
        resume=resume)

    sh_agg_job_list.update_status(resume=resume)
    while (sh_agg_job_list.completed < sh_agg_job_list.all_jobs) and (
           sh_agg_job_list.retry_exceeded == 0):
        if (sh_agg_job_list.submitted > 0) or (sh_agg_job_list.running > 0):
            time.sleep(30)
        sh_agg_job_list.update_status()
        sh_agg_job_list.run_jobs()
        print("There are:")
        print("    {} all jobs".format(sh_agg_job_list.all_jobs))
        print("    {} submitted jobs".format(sh_agg_job_list.submitted))
        print("    {} running jobs".format(sh_agg_job_list.running))
        print("    {} not started jobs".format(sh_agg_job_list.not_started))
        print("    {} completed jobs".format(sh_agg_job_list.completed))
        print("    {} failed jobs".format(sh_agg_job_list.failed))
        print("    {} jobs whose retry attempts are exceeded".format(
            sh_agg_job_list.retry_exceeded))
    if sh_agg_job_list.retry_exceeded > 0:
        sh_agg_job_list.display_jobs(status="Retry exceeded")
    print("Done with shk.agg!")
