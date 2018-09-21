import argparse
import os
import time
from jobmon import qmaster, central_job_monitor
from jobmon.schedulers import RetryScheduler
from jobmon.executors.sge_exec import SGEExecutor
from old_version import get_new_vers

def prep_dem(q, vers, upload_dir):
    ##########################################
    #Launch envelope + population prep
    ##########################################
    root_path = os.path.dirname(os.path.abspath(__file__))
    runfile = "%s/dem_prep.py" % root_path
    params = ['--upload_dir', upload_dir, '--vers', str(vers)]
    remote_job = q.create_job(runfile=runfile,
                              jobname='oldCorrect_dem_prep',
                              parameters=params)
    q.queue_job(remote_job, slots=40, memory=80, project='proj_codcorrect',
                stderr='/FILEPATH',
                stdout='/FILEPATH')
    q.block_till_done(stop_scheduler_when_done=False)

def scaler(q, cause_set, vers, upload_dir):
    ##########################################
    #Launch scaling jobs (parallelized by
    #year)
    ##########################################
    root_path = os.path.dirname(os.path.abspath(__file__))
    runfile = "%s/scaler.py" % root_path
    for year_id in range(1980, 2017):
        params = ['--cause_set', str(cause_set), '--upload_dir', upload_dir,
                  '--vers', str(vers), '--year_id', str(year_id)]
        remote_job = q.create_job(runfile=runfile,
                                  jobname='oldCorrect_scaling_%s' % year_id,
                                  parameters=params)
        q.queue_job(remote_job, slots=10, memory=20, project='proj_codcorrect',
                    stderr='/FILEPATH',
                    stdout='/FILEPATH')
    q.block_till_done(stop_scheduler_when_done=False)


def saver(q, cause_set, vers, upload_dir, detail, env, log_dir):
    ##########################################
    #Launch saving jobs after scaling jobs
    #have completed
    ##########################################
    root_path = os.path.dirname(os.path.abspath(__file__))
    runfile = "%s/saver.py" % root_path
    params = ['--cause_set', str(cause_set), '--upload_dir', upload_dir,
              '--vers', str(vers), '--detail', detail, '--env', env,
              '--log_dir', log_dir]
    remote_job = q.create_job(runfile=runfile,
                              jobname='oldCorrect_saving', parameters=params)
    q.queue_job(remote_job, slots=5, memory=10, project='proj_codcorrect',
                stderr='FILEPATH',
                stdout='FILEPATH')
    q.block_till_done(stop_scheduler_when_done=False)


def launch_squeeze(detail,
                   env,
                   cause_set=10,
                   upload_dir='FILEPATH',
                   log_dir='FILEPATH'):
    ##########################################
    #Start jobmon instance, get oldCorrect run
    #version, and run saving and scaling jobs
    ##########################################
    if os.path.isfile(log_dir + "/monitor_info.json"):
        os.remove(log_dir +  "/monitor_info.json")
    try:
        jm = central_job_monitor.CentralJobMonitor(log_dir, persistent=False)
        time.sleep(5)
    except Exception as e:
        raise RuntimeError("Couldnt launch central_job_monitor: {}".format(e))
    else:
        q = qmaster.JobQueue(log_dir, executor=SGEExecutor,
                             scheduler=RetryScheduler)
        vers = get_new_vers()

        prep_dem(q, vers, upload_dir)

        scaler(q, cause_set, vers, upload_dir)

        saver(q, cause_set, vers, upload_dir, detail, env, log_dir)
    finally:
        jm.generate_report()
        jm.stop_responder()
        jm.stop_publisher()


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--env",
            help="db environment",
            dUSERt="prod",
            type=str)
    parser.add_argument(
            "--detail",
            help="more detailed info",
            dUSERt="",
            type=str)
    args = parser.parse_args()
    env = args.env
    detail = args.detail

    launch_squeeze(detail=detail, env=env)
