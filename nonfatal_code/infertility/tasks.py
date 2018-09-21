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
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import getset

log_dir = {FILEPATH}
conda_bin_dir = {FILEPATH}
conda_env = {CONDA ENVIRONMENT}

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

# make log directory
if not os.path.exists(os.path.join(root, "logs")):
    os.makedirs(os.path.join(root, "logs"))
else:
    shutil.rmtree(os.path.join(root, "logs"))
    os.makedirs(os.path.join(root, "logs"))

# descriptions
env_description = "Env run"
pid_split_description = "Pid split"
female_attr_description = "Female attribution"
male_attr_description = "Male attribution"
excess_description = "Excess redistribution"


class _BaseBuild(builder.TaskBuilder):

    task_builder = luigi.Parameter(
        significant=False,
        dUSERt=("task_master.process_artifact.builders.JSONProcessArtifactMap?"
                 "{root}/infertility/maps/full.json".format(root=root)))

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def mourn_failure(task, exception):
        df = pd.DataFrame({"process": task.identity,
                           "error": str(exception)},
                          index=[0])
        df.to_csv("{root}/logs/{process}.csv".format(root=root,
                                                     process=task.identity))

    def get_qmaster(self):
        execute = sge_exec.SGEExecutor(
            log_dir, 3, 30000, conda_bin_dir, conda_env)
        return qmaster.MonitoredQ(execute)


class ModelableEntity(_BaseBuild, artifact.ModelableEntity):
    pass


class Envelope(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        kwargs = self.build_args[1]
        male_prop_id = kwargs.pop("male_prop_id")
        female_prop_id = kwargs.pop("female_prop_id")
        exp_id = kwargs.pop("exp_id")
        env_id = kwargs.pop("env_id")
        male_env_id = kwargs.pop("male_env_id")
        female_env_id = kwargs.pop("female_env_id")

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        # make output directories
        for _id in [male_env_id, female_env_id]:
            sub_dir = os.path.join(directory, str(_id))
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            else:
                shutil.rmtree(sub_dir)
                os.makedirs(sub_dir)

        env_params = ["--male_prop_id", str(male_prop_id), "--female_prop_id",
                      str(female_prop_id), "--exp_id", str(exp_id), "--env_id", str(env_id),
                      "--male_env_id", str(male_env_id), "--female_env_id",
                      str(female_env_id), "--out_dir", str(directory), "--year_id"]

        q = self.get_qmaster()
        # try:

        # parallelize by year
        for i in {YEAR IDS}:
            remote_job=job.Job(
                mon_dir=log_dir,
                name="{proc}_{loc}".format(proc=self.identity,
                                              loc=i),
                runfile="{root}/infertility/calc_env.py".format(root=root),
                job_args=env_params + [str(i)])
            q.queue_job(
                remote_job,
                slots=7,
                memory=14,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them

        # save the results
        for save_id in [male_env_id, female_env_id]:

            save_params = [
                str(save_id), env_description,
                os.path.join(directory, str(save_id)), "--best",
                "--file_pattern", "{year_id}.h5", "--h5_tablename", "data"]
            remote_job=job.Job(
                mon_dir=log_dir,
                name="save_" + str(save_id),
                runfile=("{FILEPATH}/save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them


class PID(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        kwargs = self.build_args[1]
        pid_env_id = kwargs.pop("pid_env_id")
        chlam_prop_id = kwargs.pop("chlam_prop_id")
        gono_prop_id = kwargs.pop("gono_prop_id")
        other_prop_id = kwargs.pop("other_prop_id")
        chlam_id = kwargs.pop("chlam_id")
        gono_id = kwargs.pop("gono_id")
        other_id = kwargs.pop("other_id")

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        split_params = [
            str(pid_env_id),
            "--target_meids", str(chlam_id), str(gono_id), str(other_id),
            "--prop_meids", str(chlam_prop_id), str(gono_prop_id), str(other_prop_id),
            "--split_meas_ids", "{MEASURE ID}", "{MEASURE ID}",
            "--prop_meas_id", "{MEASURE ID}",
            "--output_dir", directory]

        q = self.get_qmaster()

        # try:
        remote_job = job.Job(
            mon_dir=log_dir,
            name=self.identity,
            runfile=("{FILEPATH}/epi_splits"),
            job_args=split_params)
        q.queue_job(
            remote_job,
            slots=40,
            memory=60,
            project={PROJECT},
            stderr=log_dir,
            stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them

        # save the results
        for save_id in [chlam_id, gono_id, other_id]:

            save_params = [
                str(save_id), pid_split_description,
                os.path.join(directory, str(save_id)), "--best",
                "--sexes", "{SEX ID}", "--file_pattern", "{location_id}.h5",
                "--h5_tablename", "draws"]

            remote_job = job.Job(
                mon_dir=log_dir,
                name="save_" + str(save_id),
                runfile=("{FILEPATH}/save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them


class Westrom(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        kwargs = self.build_args[1]
        source_me_id = kwargs.pop("source_me_id")
        target_me_id = kwargs.pop("target_me_id")

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        west_params = [
            "--source_me_id", source_me_id,
            "--target_me_id", target_me_id
        ]

        q = self.get_qmaster()

        remote_job = job.Job(
            mon_dir=log_dir,
            name=self.identity,
            runfile="{root}/infertility/westrom.py".format(root=root),
            job_args=west_params)
        q.queue_job(
            remote_job,
            slots=10,
            memory=20,
            project={PROJECT},
            stderr=log_dir,
            stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them



class FemaleInfert(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        me_map = self.build_args[0][0]

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        # make output directories
        save_ids = []
        for mapper in me_map.values():
            outputs = mapper.get("trgs", {})
            for me_id in outputs.values():
                os.makedirs(os.path.join(directory, str(me_id)))
                save_ids.append(me_id)

        attr_params = ["--me_map", json.dumps(me_map),
                       "--out_dir", directory,
                       "--location_id"]

        q = self.get_qmaster()
        # attribution jobs by location_id
        for i in getset.get_most_detailed_location_ids():
            remote_job=job.Job(
                mon_dir=log_dir,
                name="{proc}_{loc}".format(proc=self.identity,
                                           loc=i),
                runfile="{root}/infertility/female_attr.py".format(
                    root=root),
                job_args=attr_params + [str(i)])
            q.queue_job(
                remote_job,
                slots=4,
                memory=8,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
            time.sleep(1.5)
        q.block_till_done(poll_interval=60)  # monitor them

        # save the results
        for save_id in save_ids:

            save_params = [
                str(save_id), female_attr_description,
                os.path.join(directory, str(save_id)), "--best",
                "--sexes", "{SEX ID}", "--file_pattern", "{location_id}.h5",
                "--h5_tablename", "data"]

            remote_job=job.Job(
                mon_dir=log_dir,
                name="save_" + str(save_id),
                runfile=("{FILEPATH}/save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them


class MaleInfert(_BaseBuild, process.PyProcess):

    def execute(self):

        # compile submission arguments
        me_map = self.build_args[0][0]

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        # make output directories
        save_ids = []
        for mapper in me_map.values():
            outputs = mapper.get("trgs", {})
            for me_id in outputs.values():
                os.makedirs(os.path.join(directory, str(me_id)))
                save_ids.append(me_id)

        attr_params = ["--me_map", json.dumps(me_map),
                       "--out_dir", directory,
                       "--year_id"]

        q = self.get_qmaster()  # monitor

        # attribution jobs by year_id
        for i in [{YEAR IDS}]:
            remote_job=job.Job(
                mon_dir=log_dir,
                name="{proc}_{year}".format(proc=self.identity,
                                            year=i),
                runfile="{root}/infertility/male_attr.py".format(
                    root=root),
                job_args=attr_params + [str(i)])
            q.queue_job(
                remote_job,
                slots=3,
                memory=6,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them

        # save the results
        for save_id in save_ids:

            save_params = [
                str(save_id), male_attr_description,
                os.path.join(directory, str(save_id)), "--best",
                "--sexes", "{SEX ID}", "--file_pattern", "{year_id}.h5",
                "--h5_tablename", "data"]
            remote_job=job.Job(
                mon_dir=log_dir,
                name="save_" + str(save_id),
                runfile=("{FILEPATH}/save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them


class Excess(_BaseBuild, process.PyProcess):

    def execute(self):

        kwargs = self.build_args[1]
        excess_id = kwargs.pop("excess")
        redist_map = kwargs.pop("redist")

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc=self.identity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            shutil.rmtree(directory)
            os.makedirs(directory)

        # make output directories
        for me_id in redist_map.values():
            sub_dir = os.path.join(directory, str(me_id))
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            else:
                shutil.rmtree(sub_dir)
                os.makedirs(sub_dir)

        exs_params = ["--excess_id", str(excess_id),
                      "--redist_map", json.dumps(redist_map),
                      "--out_dir", directory,
                      "--year_id"]

        q = self.get_qmaster()  # monitor

        # attribution jobs by location_id
        for i in [{YEAR IDS}]:
            remote_job=job.Job(
                mon_dir=log_dir,
                name="{proc}_{loc}".format(proc=self.identity,
                                              loc=i),
                runfile="{root}/infertility/excess.py".format(
                    root=root),
                job_args=exs_params + [str(i)])
            q.queue_job(
                remote_job,
                slots=5,
                memory=10,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
            time.sleep(1.5)
        q.block_till_done(poll_interval=60)  # monitor them

        # save the results
        for save_id in redist_map.values():

            save_params = [
                str(save_id), excess_description,
                os.path.join(directory, str(save_id)), "--best",
                "--sexes", "{SEX ID}", "--file_pattern", "{year_id}.h5",
                "--h5_tablename", "data"]
            remote_job=job.Job(
                mon_dir=log_dir,
                name="save_" + str(save_id),
                runfile=("{FILEPATH}/save_custom_results"),
                job_args=save_params)
            q.queue_job(
                remote_job,
                slots=20,
                memory=40,
                project={PROJECT},
                stderr=log_dir,
                stdout=log_dir)
        q.block_till_done(poll_interval=60)  # monitor them


class Hook(_BaseBuild, process.Hook):
    identity = luigi.Parameter(dUSERt="process_hook")
    pass


if __name__ == "__main__":
    try:
        cjm = central_job_monitor.CentralJobMonitor(log_dir, persistent=False)
        time.sleep(3)
        luigi.run()
        cjm.generate_report()
    finally:
        cjm.stop_responder()