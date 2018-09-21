import os
import time
from glob import glob

from hierarchies import dbtrees
from jobmon import qmaster, job, sge
from jobmon.central_job_monitor import CentralJobMonitor
from jobmon.responder import MonitorAlreadyRunning
from jobmon.executors import sge_exec


def squeezer(q, log_dir):
    locations = dbtrees.loctree(None, 35)
    root_path = os.path.dirname(os.path.abspath(__file__))
    runfile = "%s/squeeze_em_all.py" % root_path

    for location_id in [l.id for l in locations.leaves()]:
        for year_id in [1990, 1995, 2000, 2005, 2010, 2016]:
            for sex_id in [1, 2]:
                params = ['--location_id', str(location_id),
                          '--year_id', str(year_id),
                          '--sex_id', str(sex_id)]
                remote_job = job.Job(mon_dir=log_dir, runfile=runfile,
                                     name='squeeze_%s_%s_%s' % (location_id,
                                                                year_id,
                                                                sex_id),
                                     job_args=params)
                q.queue_job(remote_job,
                            slots=30,
                            memory=60,
                            project='proj_epic',
                            stderr='/FILEPATH',
                            stdout='/FILEPATH')

    q.block_till_done(poll_interval=60)


def saver(q, dirs, save_func, log_dir):
    runfile = sge.true_path(executable=save_func)
    for d in dirs:
        meid = os.path.basename(d)
        params = [meid, "super-squeeze result", d, '--env', 'prod',
                  '--file_pattern', '{location_id}_{year_id}_{sex_id}.h5',
                  '--h5_tablename', 'draws', '--best']
        remote_job = job.Job(mon_dir=log_dir, runfile=runfile,
                             name='ss_save_%s' % meid, job_args=params)
        q.queue_job(remote_job,
                    slots=20,
                    memory=40,
                    project='proj_epic',
                    stderr='/FILEPATH',
                    stdout='/FILEPATH')

    q.block_till_done(poll_interval=60)


def launch_squeeze(log_dir='FILEPATH', squeeze=True,
                   save=True):
    try:
        jm = CentralJobMonitor(log_dir, persistent=False)
        time.sleep(5)
    except MonitorAlreadyRunning:
        pass
    else:
        err = glob('/FILEPATH')
        out = glob('/FILEPATH')
        ps = glob('/FILEPATH')
        for log in err + out + ps:
            os.remove(log)
        conda_dir = '/FILEPATH'
        execute = sge_exec.SGEExecutor(jm.out_dir, 3, 30000, conda_dir, 'epic')
        q = qmaster.MonitoredQ(execute)
        if squeeze:
            squeezer(q, log_dir)
        dirs = glob("/FILEPATH")
        save_func = "save_custom_results"
        if save:
            saver(q, dirs, save_func, log_dir)
    finally:
        jm.generate_report()
        jm.stop_responder()
