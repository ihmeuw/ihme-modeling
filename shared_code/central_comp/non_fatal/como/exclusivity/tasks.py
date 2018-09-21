import luigi
import os
import time
import json

import pandas as pd

from jobmon import qmaster, central_job_monitor, job, sge
from jobmon.executors import sge_exec

from task_master import builder
from task_master.process_artifact.process import PyProcess, Hook, ShellProcess
from task_master.process_artifact.artifact import ModelableEntity, ComoVersion

from db_tools.ezfuncs import query

from super_squeeze import launch_squeeze


# module level globals
code_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = "filepath"
log_dir = "filepath"

conda_bin_dir = "filepath"
conda_env = "epic"

description = "Exclusivity adjustment auto-mark"


class DagMonitorMixins(builder.TaskBuilder):

    task_builder = luigi.Parameter(
        significant=False,
        dUSERt=(
            "task_master.process_artifact.builders.JSONProcessArtifactMap?"
            "{code_dir}/final.json".format(code_dir=code_dir)))

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def mourn_failure(task, exception):
        df = pd.DataFrame({"process": task.identity,
                           "error": str(exception)},
                          index=[0])
        df.to_csv(os.path.join(log_dir, "failures", task.identity + ".csv"),
                  index=False)

    def get_qmaster(self):
        execute = sge_exec.SGEExecutor(
            log_dir, 3, 30000, conda_bin_dir, conda_env)
        return qmaster.MonitoredQ(execute)


class Hook(Hook, DagMonitorMixins):
    identity = luigi.Parameter(dUSERt="como")


class ModelableEntity(ModelableEntity, DagMonitorMixins):
    pass


class ComoVersion(ComoVersion, DagMonitorMixins):
    pass


class SevSplits(PyProcess, DagMonitorMixins):

    def _get_latest_mvid(self, meid):
        q = """
            SELECT model_version_id
            FROM epi.model_version
            WHERE modelable_entity_id = {meid}
            ORDER BY date_inserted DESC LIMIT 1
        """.format(meid=meid)
        mvid = query(q, conn_def='epi').model_version_id.item()
        return mvid

    def execute(self):

        # get args
        kwargs = self.build_args[1]
        parent_meid = kwargs["parent_meid"]
        env = "prod"

        # get qmaster
        q = self.get_qmaster()

        # submit split job
        remote_job = job.Job(
            mon_dir=log_dir,
            name="split_" + (str(parent_meid)),
            runfile=os.path.join(code_dir, "scripts", "run_split.py"),
            job_args=[str(parent_meid), env])
        q.queue_job(
            remote_job,
            slots=49,
            memory=98,
            project="proj_epic")
        q.block_till_done(poll_interval=60)

        # submit aggregation/save jobs
        outputs_tuples = self.builder.get_process_outputs(self.identity)
        children_meids = [task_tuple[0] for task_tuple in outputs_tuples]
        for meid in children_meids:
            mvid = self._get_latest_mvid(meid)
            remote_job = job.Job(
                mon_dir=log_dir,
                name="save_" + str(mvid),
                runfile=sge.true_path(executable="aggregate_mvid"),
                job_args=[str(mvid), '--env', env, '--mark_best'])
            q.queue_job(
                remote_job,
                slots=40,
                memory=80,
                project="proj_epic")
        q.block_till_done(poll_interval=60)


class Exclusivity(PyProcess, DagMonitorMixins):

    def execute(self):

        # compile submission arguments
        kwargs = self.build_args[1]
        me_map = kwargs.pop("me_map")

        # get qmaster
        q = self.get_qmaster()

        # command line args for adjuster.py
        # parallelize by year
        for i in [1990, 1995, 2000, 2005, 2010, 2016]:
            ex_params = ["--me_map", json.dumps(me_map),
                         "--out_dir", data_dir, "--year_id", str(i)]
            remote_job = job.Job(
                mon_dir=log_dir,
                runfile=os.path.join(code_dir, "scripts", "run_ex_adjust.py"),
                name="{proc}_{year}".format(proc=self.identity, year=i),
                job_args=ex_params
            )
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project="proj_epic")
        q.block_till_done(poll_interval=60)

        outputs_tuples = self.builder.get_process_outputs(self.identity)
        result_meids = [task_tuple[0] for task_tuple in outputs_tuples]
        for meid in result_meids:
            save_params = [
                meid,
                description,
                os.path.join(data_dir, str(meid)),
                "--best",
                "--file_pattern", "{year_id}.h5",
                "--h5_tablename", "draws"]

            remote_job = job.Job(
                mon_dir=log_dir,
                name="save_" + str(meid),
                runfile=sge.true_path(executable="save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=40,
                memory=80,
                project="proj_epic")
        q.block_till_done(poll_interval=60)


class SuperSqueeze(ShellProcess, DagMonitorMixins):

    def execute(self):
        launch_squeeze(log_dir=log_dir)


if __name__ == "__main__":
    try:
        cjm = central_job_monitor.CentralJobMonitor(log_dir, persistent=False)
        time.sleep(3)
    except:
        pass
    else:
        luigi.run()
    finally:
        cjm.generate_report()
        cjm.stop_responder()
