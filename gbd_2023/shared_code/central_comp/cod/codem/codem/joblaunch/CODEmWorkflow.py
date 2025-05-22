import os
from typing import Optional

from gbd.enums import Cluster
from jobmon.client.workflow import Workflow

from codem.joblaunch.run_utils import get_jobmon_tool


class CODEmWorkflow:
    """This class contains metadata for a workflow created by the CODEm jobmon tool."""

    def __init__(
        self,
        name: str,
        description: str,
        model_version_id: Optional[int] = None,
        cause_id: Optional[int] = None,
        acause: Optional[str] = None,
        user: Optional[str] = None,
        project: Optional[str] = "proj_codem",
    ) -> None:
        self.name = name
        self.description = description
        self.model_version_id = model_version_id
        self.cause_id = cause_id
        self.acause = acause
        self.user = user
        self.project = project
        self.default_cluster_name = self.get_default_cluster_name()
        self.stdout = self.get_stdout()
        if not os.path.exists(self.stdout):
            os.mkdir(self.stdout)
        self.stderr = self.get_stderr()
        if not os.path.exists(self.stderr):
            os.mkdir(self.stderr)

    def get_workflow(self) -> Workflow:
        tool = get_jobmon_tool()
        workflow = tool.create_workflow(
            workflow_args=self.name,
            name=self.name,
            description=self.description,
            default_cluster_name=self.default_cluster_name,
            default_compute_resources_set={
                self.default_cluster_name: {
                    "project": self.project,
                    "stdout": self.stdout,
                    "stderr": self.stderr,
                }
            },
            workflow_attributes={
                "model_version_id": self.model_version_id,
                "cause_id": self.cause_id,
                "acause": self.acause,
                "user": self.user,
            },
        )

        return workflow

    def get_stdout(self) -> str:
        """Gets the path to stdout"""
        return f"FILEPATH"

    def get_stderr(self) -> str:
        """Gets the path to stderr"""
        return f"FILEPATH"

    @staticmethod
    def get_default_cluster_name() -> str:
        """Gets the cluster to run on."""
        return Cluster.SLURM
