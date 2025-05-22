import logging
from typing import Dict, List, Optional

from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

import hybridizer.metadata as metadata
from hybridizer.reference import db_connect
from hybridizer.utilities import get_jobmon_tool

logger = logging.getLogger(__name__)


class HybridTask:
    """Creates a hybrid task for a CODEm Workflow."""

    def __init__(
        self,
        user: str,
        datarich_model_version_id: int,
        global_model_version_id: int,
        conn_def: str,
        upstream_tasks: Optional[List[Task]] = None,
        parameter_dict: Optional[Dict[str, int]] = None,
        max_attempts: Optional[int] = 3,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> None:
        model_version_id, release_id = metadata.hybrid_metadata(
            user, global_model_version_id, datarich_model_version_id, conn_def
        )
        self.model_version_id: int = model_version_id
        self.user: str = user
        self.global_model_version_id: int = global_model_version_id
        self.datarich_model_version_id: int = datarich_model_version_id
        self.release_id: int = release_id
        self.conn_def: str = conn_def

        logger.info(f"New Hybrid Model Version ID: {model_version_id}")

        self.upstream_task: Optional[List[Task]] = upstream_tasks
        self.parameter_dict: Optional[Dict[str, int]] = parameter_dict
        self.max_attempts: Optional[int] = max_attempts
        self.start_date: Optional[str] = start_date
        self.end_date: Optional[str] = end_date

        self.executable = "hybridize"
        self.tool = get_jobmon_tool()
        self.upstream_tasks = upstream_tasks
        self.get_model_version_metadata()
        self.run_parameters(start_date=self.start_date, end_date=self.end_date)
        self.command = self.generate_command()
        self.task_template = self.get_task_template()
        # TODO: predict GB and minutes rather than relying on past model stats.

    def run_parameters(self, start_date: str, end_date: str, max_cores: int = 24) -> None:
        """Set up cluster resource parameters."""
        try:
            from codem.joblaunch import profiling

            self.parameters = profiling.get_parameters(
                cause_id=self.cause_id,
                age_start=self.age_start,
                age_end=self.age_end,
                model_version_type_id=self.model_version_type_id,
                start_date=start_date or profiling.START_DATE,
                end_date=end_date,
                hybridizer=True,
            )
        except ImportError:
            self.parameters = {
                "cores_requested": 4,
                "ram_gb": 100,
                "ram_gb_requested": 100,
                "runtime_min": 60 * 2,
            }

        self.parameters["memory"] = round(self.parameters["ram_gb"] * 1.2 + 10)
        self.parameters["runtime"] = min(
            (self.parameters["runtime_min"] * 1.2 + 60) * 60, 60 * 60 * 24 * 3
        )
        self.parameters["cores"] = min(self.parameters["cores_requested"], max_cores)
        logger.info(
            f"Max runtime seconds is {self.parameters['runtime']}, with "
            f"{self.parameters['memory']}G of memory, running with {self.parameters['cores']}"
        )

    def get_model_version_metadata(self):  # noqa: ANN201
        """Get metadata for the model version object that has already been created."""
        call = """
                 SELECT
                     cmt.model_version_type, sc.acause, cmv.release_id,
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
        self.name = f"cod_{self.model_version_id}_{self.acause}_{self.model_version_type}"

        return self

    def generate_command(self) -> str:
        """
        Generates the command that will be run by jobmon. Calls an executable
        created from /hybridizer/scripts/hybridize.py, installed as an entry point in setup.py.
        """
        command = (
            "{environment_variables} "
            f"{self.executable} "
            "--user {user} --model_version_id {model_version_id} "
            "--global_model_version_id {global_model_version_id} "
            "--datarich_model_version_id {datarich_model_version_id} --conn_def {conn_def} "
            "--release_id {release_id}"
        )

        return command

    def get_task_template(self) -> TaskTemplate:
        """Gets the task template for the CODEm Hybridizer from the Jobmon database."""
        template_transform = self.tool.get_task_template(
            template_name="codem_hybridizer_hybridize",
            command_template=self.command,
            node_args=[
                "user",
                "model_version_id",
                "global_model_version_id",
                "datarich_model_version_id",
                "release_id",
                "conn_def",
                "environment_variables",
            ],
        )

        return template_transform

    def get_task(self) -> Task:
        """
        Returns a Jobmon task using a task template. Sets executor parameters to values set
        in __init__.
        """
        memory = str(self.parameters["memory"]) + "G"
        runtime = self.parameters["runtime"]
        cores = int(self.parameters["cores"])
        logger.info(
            f"Hybrid model Task for model version {self.model_version_id} is running with "
            f"{runtime} seconds, and {memory}, running on {cores} cores"
        )
        node_args = {
            "user": self.user,
            "model_version_id": self.model_version_id,
            "global_model_version_id": self.global_model_version_id,
            "datarich_model_version_id": self.datarich_model_version_id,
            "release_id": self.release_id,
            "conn_def": self.conn_def,
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
                "queue": "all.q",
            },
            **node_args,
        )
        return task
