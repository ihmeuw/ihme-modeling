import logging
from typing import List, Optional

from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from codem.joblaunch.run_utils import get_jobmon_tool


class CODEmDiagnosticsTask:
    """Contains metadata associated with running codem_diagnostics for two model versions."""

    def __init__(
        self,
        model_version_id_1: int,
        model_version_id_2: int,
        cluster_account: str,
        overwrite: int,
        upstream_tasks: Optional[List[Task]] = None,
    ):
        self.model_version_id_1 = model_version_id_1
        self.model_version_id_2 = model_version_id_2
        self.cluster_account = cluster_account
        self.overwrite = overwrite
        self.upstream_tasks = upstream_tasks
        self.name = (
            f"codem_diagnostics_models_{self.model_version_id_1}_{self.model_version_id_2}"
        )

        self.executable = "CODEmDiagnostics"
        self.tool = get_jobmon_tool()
        self.command = self.generate_command()
        self.task_template = self.get_task_template()

    def generate_command(self) -> str:
        """
        Generates the command that will be run by jobmon. Calls an executable
        created from /codem/scripts/codem_diagnostics_launch.py, installed as an
        entry point in setup.py.
        """
        command = (
            f"{self.executable} "
            "--model_version_id_1 {model_version_id_1} "
            "--model_version_id_2 {model_version_id_2} --cluster_account {cluster_account}"
            " --overwrite {overwrite}"
        )
        return command

    def get_task_template(self) -> TaskTemplate:
        """Gets a task template for running codem_diagnostics for two CODEm models"""
        template_transform = self.tool.get_task_template(
            template_name="codem_diagnostics_launch",
            command_template=self.command,
            node_args=[
                "model_version_id_1",
                "model_version_id_2",
                "cluster_account",
                "overwrite",
            ],
        )

        return template_transform

    def get_task(self) -> Task:
        """Returns a Jobmon task using the codem_diagnostics_launch task template."""
        logging.info(
            f"CODEm Diagnostics task set up for model version {self.model_version_id_1} "
            f"and {self.model_version_id_2}"
        )
        node_args = {
            "model_version_id_1": self.model_version_id_1,
            "model_version_id_2": self.model_version_id_2,
            "cluster_account": self.cluster_account,
            "overwrite": self.overwrite,
        }
        task = self.task_template.create_task(
            max_attempts=2,
            upstream_tasks=self.upstream_tasks,
            name=self.name,
            compute_resources={
                "cores": 1,
                "memory": "256M",
                "runtime": 900,  # 15 min
                "queue": "all.q",
            },
            **node_args,
        )
        return task
