import json
import logging
from typing import Dict, List, Optional

from gbd.enums import Cluster
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from codem.data.parameters import get_model_parameters
from codem.joblaunch.resource_predictions import (
    get_step_prediction,
    import_resource_parameters,
)
from codem.joblaunch.run_utils import get_jobmon_tool
from codem.metadata.step_metadata import STEP_DICTIONARY, STEP_IDS, STEP_NAMES

logger = logging.getLogger(__name__)


class StepTaskGenerator:
    """Class for generating a CODEmStep object with specific resource parameters."""

    def __init__(
        self,
        model_version_id: int,
        conn_def: str,
        old_covariates_mvid: int,
        debug_mode: int,
        additional_resources: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Generates a CODEm Step that create a Jobmon Task
        for a given model version ID so we don't have to constantly
        pass those arguments.

        :param model_version_id:
        :param conn_def:
        :param old_covariates_mvid:
        :param debug_mode:
        :param additional_resources: dictionary of more resources to use, by step
        """
        self.model_version_id: int = model_version_id
        self.conn_def: str = conn_def
        self.old_covariates_mvid: int = old_covariates_mvid
        self.debug_mode: int = debug_mode
        self.resource_prediction_data = import_resource_parameters()
        if not additional_resources:
            additional_resources = {}
        self.additional_resources: Optional[Dict[str, str]] = additional_resources

    def generate(
        self,
        step_id: int,
        inputs_info: Dict[str, int],
        additional_args: Optional[Dict[str, int]] = None,
        upstream_tasks: Optional[List[Task]] = None,
        resource_scales: Optional[Dict[str, float]] = None,
    ) -> Task:
        parameters = get_step_prediction(
            resource_parameters=self.resource_prediction_data,
            step_id=step_id,
            input_info=inputs_info,
        )
        if str(step_id) in self.additional_resources:
            if "runtime" in self.additional_resources[str(step_id)]:
                logger.info("Overriding max runtime seconds.")
                parameters["runtime"] = int(
                    self.additional_resources[str(step_id)]["runtime"]
                )
            if "memory" in self.additional_resources[str(step_id)]:
                logger.info("Overriding memory.")
                parameters["memory"] = str(self.additional_resources[str(step_id)]["memory"])
            if "cores" in self.additional_resources[str(step_id)]:
                logger.info("Overriding cores.")
                parameters["cores"] = int(self.additional_resources[str(step_id)]["cores"])

        # Forcing step 15 models to use specified RAM and runtime, a subset of
        # models often fail at this step because they require much more memory
        # and runtime than is predicted
        if step_id == STEP_IDS["Diagnostics"]:
            logger.info("Overriding memory and max runtime seconds for Diagnostics step")
            parameters["runtime"] = 25000
            parameters["memory"] = "25G"

        # Update queue if runtime is more than 3 days
        if parameters["runtime"] > 259200:
            parameters["queue"] = "long.q"

        return CODEmStep(
            model_version_id=self.model_version_id,
            conn_def=self.conn_def,
            old_covariates_mvid=self.old_covariates_mvid,
            debug_mode=self.debug_mode,
            step_id=step_id,
            inputs_info=inputs_info,
            additional_args=additional_args,
            cores=parameters["cores"],
            runtime=parameters["runtime"],
            memory=parameters["memory"],
            queue=parameters["queue"],
            upstream_tasks=upstream_tasks,
            resource_scales=resource_scales,
        ).setup_task()


class CODEmStep:
    def __init__(
        self,
        model_version_id: int,
        conn_def: str,
        old_covariates_mvid: int,
        debug_mode: int,
        step_id: int,
        inputs_info: Dict[str, int],
        retries: int = 10,
        additional_args: Optional[Dict[str, int]] = None,
        runtime: Optional[int] = None,
        memory: Optional[str] = None,
        cores: Optional[int] = None,
        queue: str = "all.q",
        upstream_tasks: Optional[List[Task]] = None,
        resource_scales: Optional[Dict[str, int]] = None,
    ) -> None:
        """CODEm Step class to define variables passed to create a Jobmon Task."""
        logger.info(f"Creating a step {step_id} Jobmon Task for {model_version_id}.")
        self.model_version_id: int = model_version_id
        self.conn_def: str = conn_def
        self.old_covariates_mvid: int = old_covariates_mvid
        self.debug_mode: int = debug_mode
        self.step_id: int = step_id
        self.additional_args: Optional[Dict[str, str]] = additional_args
        self.retries: int = retries
        self.upstream_tasks: Optional[List[Task]] = upstream_tasks
        self.inputs_info: Dict[str, int] = inputs_info
        self.memory: Optional[str] = memory
        self.runtime: Optional[int] = runtime
        self.cores: Optional[int] = cores
        self.queue: str = queue
        self.resource_scales: Optional[Dict[str, str]] = resource_scales

        self.step_name = STEP_NAMES[self.step_id]
        self.step_metadata = STEP_DICTIONARY[self.step_name]
        self.model_parameters = get_model_parameters(
            model_version_id=self.model_version_id, conn_def=self.conn_def, update=False
        )

        # get jobmon task
        self.tool = get_jobmon_tool()
        self.command = self.generate_command()
        self.job_name = self.get_job_name()
        self.task_template = self.get_task_template()

    def get_job_name(self) -> str:
        """Gets the job name that will be used for the jobmon Task."""
        return (
            f"cod_{self.model_version_id}"
            f'_{self.model_parameters["acause"]}'
            f'_{self.model_parameters["model_version_type"].lower().replace(" ", "_")}'
            f"_{self.step_id:02d}"
        )

    def generate_command(self) -> str:
        """
        Generates the command that will be run by jobmon. Calls an executable
        that lives in codem.work_exec. Files in codem/work_exec are installed as
        entry points in setup.py.
        """
        command = (
            "{environment_variables} "
            f"{self.step_name} "
            "--model_version_id {model_version_id} "
            "--conn_def {conn_def} "
            "--old_covariates_mvid {old_covariates_mvid} "
            "--debug_mode {debug_mode} "
            "--cores {cores} "
        )
        if self.additional_args:
            # unpack additional args and format for command template
            for arg_name in self.additional_args.keys():
                command += f"--{arg_name} " + "{" + f"{arg_name}" + "}"
        return command

    def get_task_template(self) -> TaskTemplate:
        """Gets the task template for the CODEm step from the Jobmon database."""
        node_args = [
            "model_version_id",
            "conn_def",
            "old_covariates_mvid",
            "debug_mode",
            "cores",
            "environment_variables",
        ]
        if self.additional_args:
            node_args = node_args + [key for key in self.additional_args.keys()]

        template_transform = self.tool.get_task_template(
            template_name=f"codem_{self.step_name}",
            command_template=self.command,
            node_args=node_args,
        )

        return template_transform

    def setup_task(self) -> Task:
        """
        Command to run -- took this out of the init because sometimes we need model metadata
        to determine which parameters to pass to the command.
        """
        node_args = {
            "model_version_id": self.model_version_id,
            "conn_def": self.conn_def,
            "old_covariates_mvid": self.old_covariates_mvid,
            "debug_mode": self.debug_mode,
            "cores": self.cores,
            "environment_variables": f"OMP_NUM_THREADS={self.cores}",
        }
        if self.additional_args is not None:
            for arg_name, arg in self.additional_args.items():
                node_args[arg_name] = arg

        task = self.task_template.create_task(
            task_attributes={"INPUTS_INFO": json.dumps(self.inputs_info)},
            max_attempts=self.retries,
            upstream_tasks=self.upstream_tasks,
            name=self.job_name,
            compute_resources={
                "cores": self.cores,
                "memory": self.memory,
                "runtime": self.runtime,
                "queue": self.queue,
            },
            cluster_name=Cluster.SLURM,
            resource_scales=self.resource_scales,
            fallback_queues=["long.q"],
            **node_args,
        )
        return task
