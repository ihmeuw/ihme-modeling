import logging
from datetime import datetime
from typing import Dict, List, Optional

from gbd.enums import Cluster
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from codem.joblaunch.profiling import get_parameters
from codem.joblaunch.run_utils import get_jobmon_tool
from codem.reference import db_connect, paths

logger = logging.getLogger(__name__)


class CODEmTask:
    """Class for running a CODEm model."""

    def __init__(
        self,
        model_version_id: int,
        conn_def: str,
        debug_mode: int = 0,
        parameter_dict: Optional[Dict[str, int]] = None,
        max_attempts: int = 5,
        upstream_tasks: Optional[List[Task]] = None,
        step_resources: Optional[Dict[str, str]] = None,
        end_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
    ) -> None:
        """
        Creates a CODEmTask. At this point, all model metadata has been uploaded
        to the database so CODEmTask is going to pull it all to seamlessly
        submit a Jobmon Task in a standalone workflow or in a batch, like a central run.
        All this needs to know is model_version_id and conn_def.
        """
        self.model_version_id: int = model_version_id
        self.parameter_dict: Optional[Dict[str, int]] = parameter_dict
        self.max_attempts: int = max_attempts
        self.upstream_tasks: Optional[List[Task]] = upstream_tasks
        self.conn_def: str = conn_def
        self.debug_mode: int = debug_mode
        self.executable = "run"

        self.name = None
        self.metadata_df = None
        self.release_id = None
        self.model_version_type_id = None
        self.model_version_type = None
        self.sex_id = None
        self.cause_id = None
        self.acause = None
        self.age_start = None
        self.age_end = None
        self.model_dir = None
        self.parameters = None
        self.old_covariates_mvid = None

        self.model_version_metadata()
        self.run_parameters(start_date=start_date, end_date=end_date)

        if self.metadata_df["run_covariate_selection"].iloc[0] == 0:
            self.old_covariates_mvid = self.metadata_df["previous_model_version_id"].iloc[0]
        else:
            self.old_covariates_mvid = 0
        self.step_resources = step_resources or "{}"

        # get jobmon task
        self.tool = get_jobmon_tool()
        self.command = self.generate_command()
        self.task_template = self.get_task_template()

    def generate_command(self) -> str:
        """
        Generates the command that will be run by jobmon. Calls an executable
        created from /codem/scripts/run.py, installed as an entry point in setup.py.
        """
        command = (
            "{environment_variables} "
            f"{self.executable} "
            "--model_version_id {model_version_id} "
            "--conn_def {conn_def} "
            "--old_covariates_mvid {old_covariates_mvid} --debug_mode {debug_mode} "
            "--step_resources {step_resources} "
        )
        return command

    def get_task_template(self) -> TaskTemplate:
        """Gets the task template for the CODEm Task from the Jobmon database."""
        template_transform = self.tool.get_task_template(
            template_name="codem_run",
            command_template=self.command,
            node_args=[
                "model_version_id",
                "conn_def",
                "old_covariates_mvid",
                "debug_mode",
                "step_resources",
                "environment_variables",
            ],
        )

        return template_transform

    def get_task(self) -> Task:
        """Returns a Jobmon task using the codem_run task template."""
        memory = str(self.parameters["memory"]) + "G"
        runtime = self.parameters["runtime"]
        cores = int(self.parameters["cores"])
        queue = self.parameters["queue"]
        logger.info(
            f"Task for model version {self.model_version_id} is running with "
            f"{runtime} seconds and {memory}, running on {cores} cores"
        )
        node_args = {
            "model_version_id": self.model_version_id,
            "conn_def": self.conn_def,
            "old_covariates_mvid": self.old_covariates_mvid,
            "debug_mode": self.debug_mode,
            "step_resources": self.step_resources,
            "environment_variables": f"OMP_NUM_THREADS={cores}",
        }
        task = self.task_template.create_task(
            max_attempts=self.max_attempts,
            upstream_tasks=self.upstream_tasks,
            name=self.name,
            compute_resources={
                "cores": cores,
                "memory": memory,
                "runtime": runtime,
                "queue": queue,
            },
            cluster_name=Cluster.SLURM,
            fallback_queues=["long.q"],
            **node_args,
        )
        return task

    def run_parameters(
        self, start_date=None, end_date=None, max_cores=56, max_seconds=1209600
    ):
        if self.parameter_dict is None:
            self.parameters = get_parameters(
                cause_id=self.cause_id,
                age_start=self.age_start,
                age_end=self.age_end,
                model_version_type_id=self.model_version_type_id,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            self.parameters = self.parameter_dict[self.model_version_type_id][self.age_start][
                self.age_end
            ]

        self.parameters["memory"] = round(self.parameters["ram_gb"])
        self.parameters["cores"] = min(self.parameters["cores_requested"], max_cores)
        self.parameters["runtime"] = min(self.parameters["runtime_min"] * 60, max_seconds)
        # If the run-time is longer than 2.5 days, put it in the long queue
        if self.parameters["runtime"] / (60 * 60 * 24) > 3:
            self.parameters["queue"] = "long.q"
        else:
            self.parameters["queue"] = "all.q"
        logger.info(
            f"Max runtime seconds is {self.parameters['runtime']} running on queue "
            f"{self.parameters['queue']}"
        )

        return self

    def model_version_metadata(self):
        """Get metadata for the model version object that has already been created."""
        call = """
                SELECT cmt.model_version_type, sc.acause, cmv.gbd_round_id, cmv.release_id,
                       cmv.run_covariate_selection, cmv.previous_model_version_id,
                       cmv.cause_id, cmv.age_start, cmv.age_end, cmv.model_version_type_id
                FROM cod.model_version cmv
                INNER JOIN shared.cause sc
                ON sc.cause_id = cmv.cause_id
                INNER JOIN cod.model_version_type cmt
                ON cmt.model_version_type_id = cmv.model_version_type_id
                WHERE model_version_id = :model_version_id"""
        self.metadata_df = db_connect.execute_select(
            call,
            conn_def=self.conn_def,
            parameters={"model_version_id": self.model_version_id},
        )

        self.acause = self.metadata_df["acause"].iloc[0]
        self.cause_id = self.metadata_df["cause_id"].iloc[0]
        self.age_start = self.metadata_df["age_start"].iloc[0]
        self.age_end = self.metadata_df["age_end"].iloc[0]
        self.model_version_type_id = self.metadata_df["model_version_type_id"].iloc[0]
        self.release_id = self.metadata_df["release_id"].iloc[0]
        self.model_version_type = (
            self.metadata_df["model_version_type"].iloc[0].lower().replace(" ", "_")
        )

        self.model_dir = paths.get_base_dir(
            model_version_id=self.model_version_id, conn_def=self.conn_def
        )

        self.name = f"cod_{self.model_version_id}_{self.acause}_{self.model_version_type}"

        return self
