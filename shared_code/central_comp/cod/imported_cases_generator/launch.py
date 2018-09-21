import os
import time
import argparse

from imported_cases.core import get_data_rich_spacetime_restricted_causes
from imported_cases.submit_jobs import Task, TaskList
import imported_cases.log_utilities as l


"""
Example usage:

python launch.py [output_version_id], where output_version_id is the number you
want to create. If output_version_id == new, the current max version + 1 will
be used.

Example of test:
python launch.py _test
"""


def parse_args():
    """
        Parse command line arguments

        Arguments are output_version_id

        Returns:
        string
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--resume",
                        help="Resume run of Imported Cases",
                        action="store_true")

    # Parse arguments
    args = parser.parse_args()
    resume = args.resume
    # Print arguments
    print "resume: {}".format(resume)
    # Return outputs
    return resume


def set_up_folders(output_directory, spacetime_cause_ids):
    """ Create output directory to store draws """
    new_folders = ['logs'] + [str(x) for x in spacetime_cause_ids]

    for folder in new_folders:
        directory = '{d}/{f}'.format(d=output_directory, f=folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
    return output_directory


def generate_imported_cases_jobs(task_list, code_directory, log_directory,
                                 cause_ids, resume=False):
    """ Generate one job for each cause """
    for cause_id in cause_ids:
        job_name = "generate_imported_cases-{}".format(cause_id)
        job_command = ["{c}/python_shell.sh".format(c=code_directory),
                       "{c}/generate_imported_cases.py"
                       .format(c=code_directory),
                       str(cause_id)]
        job_log = "FILEPATH.txt".format(
            c=cause_id, ld=log_directory)
        job_project = "proj_codcorrect"
        job_slots = 10
        job_dependencies = []
        task_list.add_task(Task(job_name, job_command, job_log, job_project,
                                slots=job_slots, resume=resume,
                                retry_attempts=1), job_dependencies)
    return task_list


if __name__ == '__main__':

    # Set some core variables
    code_directory = 'FILEPATH'
    output_directory = 'FILEPATH'

    # Parse arguments
    log_dir = r'FILEPATH'
    l.setup_logging(log_dir, 'launch_imported_cases',
                    time.strftime("%m_%d_%Y_%H"))
    resume = parse_args()

    # Get list of current space-time restricted causes
    spacetime_cause_ids = get_data_rich_spacetime_restricted_causes()
    for cause_id in [298, 299, 300, 741, 347, 360]:
        if cause_id in spacetime_cause_ids:
            spacetime_cause_ids.remove(cause_id)

    # Set up folders
    parent_dir = set_up_folders(output_directory, spacetime_cause_ids)

    # Generate Imported Cases jobs
    job_list = TaskList()
    job_list = generate_imported_cases_jobs(job_list, code_directory,
                                            parent_dir + '/logs',
                                            spacetime_cause_ids,
                                            resume=resume)

    # Run jobs
    job_list.update_status(resume=resume)
    while (job_list.completed < job_list.all_jobs
           ) and (job_list.retry_exceeded == 0):
        if job_list.submitted > 0 or job_list.running > 0:
            time.sleep(60)
        job_list.update_status()
        job_list.run_jobs()
        print "There are:"
        print "    {} all jobs".format(job_list.all_jobs)
        print "    {} submitted jobs".format(job_list.submitted)
        print "    {} running jobs".format(job_list.running)
        print "    {} not started jobs".format(
            job_list.not_started)
        print "    {} completed jobs".format(job_list.completed)
        print "    {} failed jobs".format(job_list.failed)
        print "    {} jobs whose retry attempts are exceeded".format(
            job_list.retry_exceeded)
    if job_list.retry_exceeded > 0:
        job_list.display_jobs(status="Retry exceeded")

    print "Done!"
