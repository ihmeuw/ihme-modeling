import os
import time
import sys

import pandas as pd
import subprocess
import copy
from xml.etree import ElementTree as ET
from collections import dUSERtdict
import logging


def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = dUSERtdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.iteritems():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.iteritems()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.iteritems())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d


class Task(object):
    """
    Handles the submission and status for an individual processes
    """

    def __init__(self, name, command, log_path, project_name, slots=10,
                 retry_attempts=3, resume=False):
        # Basic task args
        self.name = name
        self.command = command
        self.log_path = log_path
        self.project_name = project_name
        self.slots = slots
        self.memory = slots * 2
        self.retry_attempts = retry_attempts
        self.resume = resume

        # Job stats
        self.started_at = None
        self.completed_at = None
        self.attempt = 0
        self.status = "Not started"

        # Cluster management
        self.cluster_job_id = None
        self.cluster_status = None

        # Remove log if needed
        if not self.resume:
            try:
                if os.path.isfile(self.log_path):
                    subprocess.check_output(['rm', self.log_path])
            except:
                pass

    def submit_job(self, submit=True):
        print "Submitting {}".format(self.name)
        submission_params = ['qsub',
                             '-N', self.name,
                             '-P', self.project_name,
                             '-pe', 'multi_slot', '{}'.format(self.slots),
                             '-l', 'mem_free={}g'.format(self.memory)]
        if 'dev' not in os.environ['SGE_CLUSTER_NAME']:
            submission_params.extend(['-l', 'hosttype=intel'])
        for p in self.command:
            submission_params.append(p)
        print '{}'.format(submission_params)
        if submit:
            if self.attempt < self.retry_attempts:
                if not self.resume:
                    try:
                        if os.path.isfile(self.log_path):
                            subprocess.check_output(['rm', self.log_path])
                    except:
                        pass
                subprocess.check_output(submission_params)
                self.status = "Submitted"
                self.attempt += 1
                print "Job {} submitted".format(self.name)
            else:
                self.status = "Retry exceeded"
                print "Job {} reached the max num of retries".format(self.name)
                print "Check logs at {} for job details".format(self.log_path)
        else:
            print ' '.join([str(x) for x in submission_params])

    def delete_job(self, submit=True):
        print "Submitting {}".format(self.name)
        delete_params = ['qdel', self.name]
        if submit:
            try:
                subprocess.check_output(delete_params)
            except:
                pass
        else:
            print ' '.join([str(x) for x in delete_params])


class TaskList(object):
    def __init__(self):
        self.tasks = {}
        self.not_started = 0
        self.submitted = 0
        self.running = 0
        self.completed = 0
        self.failed = 0
        self.retry_exceeded = 0
        self.all_jobs = 0
        self.logger = logging.getLogger('submit_jobs.TaskList')

    def add_task(self, task, dependencies):
        if task.name in self.tasks:
            raise KeyError(
                "Task list already contains a job named {}".format(task.name))
        else:
            for d in dependencies:
                if d not in self.tasks:
                    raise KeyError("Dependency {} not in the current task list"
                                   .format(d))
            self.tasks[task.name] = {
                'task': task, 'dependencies': dependencies}
            self.not_started += 1
            self.all_jobs += 1

    def qstat(self):
        # Get XML results from cluster
        self.logger.info("qstat-ing")
        try:
            qstat_cmd = ["qstat", "-xml"]
            rj = ET.XML(subprocess.check_output(qstat_cmd))
            rj = etree_to_dict(rj)
            # Parse results
            df = {}
            for v in ['job_id', 'state', 'time_submission', 'name', 'machine']:
                df[v] = []
            jobs = copy.deepcopy(rj)
            self.logger.info("{}".format(jobs))
            if len(jobs['job_info']['job_info']) > 0:
                if type(jobs['job_info']['job_info']['job_list']) != list:
                    job_list = [jobs['job_info']['job_info']['job_list']]
                else:
                    job_list = jobs['job_info']['job_info']['job_list']
                for j in job_list:
                    print j
                    df['job_id'].append(j['JB_job_number'])
                    df['state'].append(j['state'])
                    df['time_submission'].append(j['JB_submission_time'])
                    df['name'].append(j['JB_name'])
                    df['machine'].append('')
            if len(jobs['job_info']['queue_info']) > 0:
                for j in jobs['job_info']['queue_info']['job_list']:
                    df['job_id'].append(j['JB_job_number'])
                    df['state'].append(j['state'])
                    df['time_submission'].append(j['JAT_start_time'])
                    df['name'].append(j['JB_name'])
                    df['machine'].append(j['queue_name'])
            df = pd.DataFrame(df)
        except Exception as e:
            self.logger.exception("{}".format(e))
            sys.exit()
        return df

    def update_status_counts(self):
        self.logger.info("updating status counts")
        self.not_started = 0
        self.submitted = 0
        self.running = 0
        self.completed = 0
        self.failed = 0
        self.retry_exceeded = 0
        self.all_jobs = 0
        for job_name in self.tasks:
            self.all_jobs += 1
            if self.tasks[job_name]['task'].status == "Not started":
                self.not_started += 1
            elif self.tasks[job_name]['task'].status == "Submitted":
                self.submitted += 1
            elif self.tasks[job_name]['task'].status == "Running":
                self.running += 1
            elif self.tasks[job_name]['task'].status == "Completed":
                self.completed += 1
            elif self.tasks[job_name]['task'].status == "Failed":
                self.failed += 1
            elif self.tasks[job_name]['task'].status == "Retry exceeded":
                self.retry_exceeded += 1

    def update_status(self, resume=False):
        try:
            # Get a list of cluster jobs
            qstat = self.qstat()
            qstat = qstat.ix[~qstat['name'].duplicated()]
            # Loop through tasks and update status
            for job_name in self.tasks:
                self.logger.info("job_name: {n} has status of: {s}".format(
                    n=job_name, s=self.tasks[job_name]['task'].status))
                if resume:
                    self.logger.info("in resume")
                    skip_statuses = ["Completed", "Retry exceeded"]
                    dUSERt_status = "Not started"
                else:
                    self.logger.info("not in resume")
                    skip_statuses = ["Not started", "Completed",
                                     "Retry exceeded"]
                    dUSERt_status = "Failed"
                if self.tasks[job_name]['task'].status in skip_statuses:
                    self.logger.info("job name in skip_statuses")
                    pass
                else:
                    self.logger.info("job name NOT in skip_statuses")
                    # Set all jobs that have not completed or started to Failed
                    self.tasks[job_name]['task'].status = dUSERt_status
                    self.tasks[job_name]['task'].cluster_status = None
                    # Check if job has been submitted to cluster or has a
                    # completed log file
                    if len(qstat.ix[qstat['name'] == job_name]) > 0:
                        self.logger.info("job name in qstat")
                        self.tasks[job_name]['task'].cluster_job_id = (
                            qstat.set_index('name').ix[job_name, 'job_id'])
                        self.tasks[job_name]['task'].cluster_status = (
                            qstat.set_index('name').ix[job_name, 'state'])
                        if self.tasks[job_name]['task'].cluster_status in [
                           'r', 't', 'Rr', 'Rt']:
                            self.logger.info("job in 'r', 't', 'Rr', or 'Rt'")
                            self.tasks[job_name]['task'].status = "Running"
                        elif self.tasks[job_name]['task'].cluster_status in [
                                'qw', 's', 'S']:
                            self.logger.info("job_name in 'qw', 's', 'S'")
                            self.tasks[job_name]['task'].status = "Submitted"
                    elif os.path.isfile(self.tasks[job_name]['task'].log_path):
                        self.logger.info("job name has a log file")
                        if 'All done' in open(self.tasks[job_name]['task']
                                              .log_path).read():
                            self.logger.info("job_name has 'all done' in log")
                            self.tasks[job_name]['task'].status = "Completed"
                        else:
                            time.sleep(5)  # avoid a race condition
                            if 'All done' in open(self.tasks[job_name]['task']
                                                  .log_path).read():
                                self.tasks[job_name]['task'].status = (
                                    "Completed")
                            else:
                                self.logger.info("""job name doesn't have
                                                 'all done' in log""")
                                self.tasks[job_name]['task'].delete_job()
                                self.tasks[job_name]['task'].status = "Failed"
                self.logger.info("""after finishing, job_name: {n}
                                 has status of: {s}""".format(
                    n=job_name, s=self.tasks[job_name]['task'].status))
        except Exception as e:
            print str(e)
            pass
        self.update_status_counts()

    def run_jobs(self):
        for job_name in self.tasks:
            if self.tasks[job_name]['task'].status in ["Running", "Submitted",
                                                       "Completed",
                                                       "Retry exceeded"]:
                pass
            elif self.tasks[job_name]['task'].status in ["Failed"]:
                self.tasks[job_name]['task'].submit_job()
            elif self.tasks[job_name]['task'].status in ["Not started"]:
                if len(self.tasks[job_name]['dependencies']) == 0:
                    self.tasks[job_name]['task'].submit_job()
                else:
                    launch_status = True
                    for d in self.tasks[job_name]['dependencies']:
                        if self.tasks[d]['task'].status not in ["Completed"]:
                            launch_status = False
                    if launch_status:
                        self.tasks[job_name]['task'].submit_job()
        self.update_status_counts()

    def display_jobs(self, status=None):
        for job_name in self.tasks:
            if status:
                if self.tasks[job_name]['task'].status == status:
                    print job_name + ": " + self.tasks[job_name]['task'].status
            else:
                print job_name + ": " + self.tasks[job_name]['task'].status
