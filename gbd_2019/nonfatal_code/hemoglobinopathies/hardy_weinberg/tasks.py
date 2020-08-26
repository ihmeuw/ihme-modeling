import luigi
import os
import shutil
import json
import time
import pandas as pd
from jobmon import qmaster, central_job_monitor, job, sge
from jobmon.executors import sge_exec
from task_master import builder
from task_master.process_artifact import process, artifact

log_dir = 'FILEPATH'
conda_bin_dir = "FILEPATH"
conda_env = "epic"

root = "FILEPATH"

# make log directory
if not os.path.exists(os.path.join(root, "logs")):
    os.makedirs(os.path.join(root, "logs"))
else:
    shutil.rmtree(os.path.join(root, "logs"))
    os.makedirs(os.path.join(root, "logs"))

# descriptions
description = "Hardy Weinberg calculation result"


class _BaseBuild(builder.TaskBuilder):

    task_builder = luigi.Parameter(
        significant=False,
        default=("FILEPATH"))

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def mourn_failure(task, exception):
        df = pd.DataFrame({"process": task.identity,
                           "error": str(exception)},
                          index=[0])
        df.to_csv("FILEPATH"

    def get_qmaster(self):
        execute = sge_exec.SGEExecutor(
            log_dir, 3, 30000, conda_bin_dir, conda_env)
        return qmaster.MonitoredQ(execute)


class HardyWeinberg(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        kwargs = self.build_args[1]
        save_id = kwargs.pop("save_id")
        hardy_type = kwargs.pop("hardy_type")

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        calc_params = ["--hardy_type", hardy_type, "--out_dir", directory,
                       "--other_args", json.dumps(kwargs), "--year_id"]

        q = self.get_qmaster()  # start up monitor

        # parallelize by year
        year_list = [i for i in range(1990, 2011, 5)] + [2017]
        for i in year_list:
            remote_job = job.Job(mon_dir=log_dir,
                                 runfile=("FILEPATH"
                                 name="{proc}_{loc}".format(proc=self.identity,
                                                            loc=i),
                                 job_args=calc_params + [str(i)])
            q.queue_job(
                remote_job,
                slots=4,
                memory=8,
                project="proj_custom_models",
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them

        # now save the results
        save_params = [
            save_id, description, directory, "--best", "--file_pattern",
            "{year_id}.h5", "--h5_tablename", "draws"]
        remote_job = job.Job(mon_dir=log_dir,
                             name="save_" + str(save_id),
                             runfile=("FILEPATH")
                             job_args=save_params)
        q.queue_job(remote_job,
                    slots=20,
                    memory=40,
                    project="proj_custom_models",
                    stderr=log_dir,
                    stdout=log_dir)
        q.block_till_done(poll_interval=60)


class ModelableEntity(_BaseBuild, artifact.ModelableEntity):
    pass


class Hook(_BaseBuild, process.Hook):
    identity = luigi.Parameter(default="process_hook")
    pass


if __name__ == "__main__":
    try:
        cjm = central_job_monitor.CentralJobMonitor(log_dir, persistent=False)
        time.sleep(3)
        luigi.run()
        cjm.generate_report()
    finally:
        cjm.stop_responder()
