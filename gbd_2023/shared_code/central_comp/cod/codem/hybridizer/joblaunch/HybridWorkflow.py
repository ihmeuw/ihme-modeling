import os
from typing import Optional

from gbd.enums import Cluster
from jobmon.client.workflow import Workflow

from hybridizer.utilities import get_jobmon_tool


class HybridWorkflow:
    """
    This class can create a jobmon workflow for the CODEm Hybridizer and takes a name,
    description and project.
    """

    def __init__(
        self,
        name: str,
        description: str,
        model_version_id: Optional[int] = None,
        cause_id: Optional[int] = None,
        acause: Optional[str] = None,
        user: Optional[str] = None,
        project: Optional[str] = "proj_codem",
    ):
        self.name = name
        self.description = description
        self.project = project
        self.model_version_id = model_version_id
        self.cause_id = cause_id
        self.acause = acause
        self.user = user
        self.default_cluster_name = self.get_default_cluster_name()
        self.stdout = f"FILEPATH/{self.name}"
        self.stderr = f"FILEPATH/{self.name}"
        if not os.path.exists(self.stdout):
            os.mkdir(self.stdout)
        if not os.path.exists(self.stderr):
            os.mkdir(self.stderr)

    def get_workflow(self) -> Workflow:
        """Create a jobmon workflow."""
        tool = get_jobmon_tool()
        workflow = tool.create_workflow(
            name=self.name,
            workflow_args=self.name,
            default_cluster_name=self.default_cluster_name,
            description=self.description,
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

    @staticmethod
    def get_default_cluster_name() -> str:
        """Gets the cluster to run on."""
        return Cluster.SLURM
