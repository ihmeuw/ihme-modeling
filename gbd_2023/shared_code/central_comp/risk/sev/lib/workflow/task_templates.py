"""Logic surrounding jobmon task template creation."""

import pathlib
import sys
from typing import Any, Dict, List, Optional, Union

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from ihme_cc_sev_calculator.lib import constants

SCRIPT_DIR: str = 
R_EXECUTABLE: str = 


def _hours_to_seconds(hours: Union[int, float]) -> int:
    """Converts hours into seconds."""
    return int(hours * 60 * 60)


def get_jobmon_tool() -> Tool:
    """Get jobmon Tool for SEV Calculator."""
    return Tool(name=constants.TOOL)


def get_compute_resources_rr_max(rei_id: Optional[int] = None) -> Dict[str, Union[str, int]]:
    """Get compute resources for RR max calculation by REI ID.

    Note:
        These numbers come from the R version of the SEV Calculator and may not
        make sense for the Python version.
    """
    # Defaults
    hours = 6
    memory = "50G"

    if rei_id == constants.BMI_ADULT_REI_ID:
        # Has ~40 risk - cause pairs in RR
        hours = 48
        memory = "200G"
    elif rei_id == constants.IRON_DEFICIENCY_REI_ID:
        memory = "100G"

    return {
        constants.MAX_RUNTIME: _hours_to_seconds(hours),
        constants.MEMORY: memory,
        constants.NUM_CORES: 10,
        constants.QUEUE: constants.ALL_QUEUE,
    }


class SevTaskTemplate:
    """Base class for Task Templates.

    Class fields to be filled in:
        script: launch script for task
        template name: name of the task template, or stage
        task_name_template: template for the job name for a task.
            Formattable string for tasks with varying args. Note: kwargs given
            to TaskTemplate.get_task must include AT LEAST all keys in task_template_name,
            but may also contain more
        command_line_args: formattable string of args to be passed in for the task.
            Ie '--version_id {version_id} --release_id {release_id}'. Args follow
            the following category order: hardcoded args, node_args, task_args. Within
            each category, args are sorted alphabetically.
        compute_resources: dictionary of compute resources relevant to Slurm. Expects at
            most five args: 'cores', 'memory', 'runtime', 'queue', 'project'.
        max_attempts: number of tries for a task template. Defaults to 3.
        node_args: arguments that differentiate individual tasks within a task template
            (stage of SEV Calculator). These are the args the stage is parallelized by.
            Ie for a task template run by location, location_id would be a node_arg.
        task_args: arguments that are constant across all tasks across a task template (stage)
            Ie. 'version_id' as the same version id is passed into all jobs

    Class constants:
        COMMAND_PREFIX: all commands begin with this. Set to "{python} {script}"
        OP_ARGS: args that are constant across the workflow. Set to python, script
        USE_R_EXECUTABLE: True if R executable should be used for task template. Defaults
            to False.

    Fields created on instantiation:
        python: the python executable
        template: fully formed task template

    Methods:
        get_task: Returns a task within the template given the kwargs passed in
    """

    script: str
    template_name: str
    task_name_template: str
    command_line_args: str
    compute_resources: Dict[str, Union[str, int]]
    max_attempts: int = 3
    node_args: List[str]
    task_args: List[str]

    COMMAND_PREFIX: str = 
    OP_ARGS: List[str] = ["executable", "script_dir", "script"]
    USE_R_EXECUTABLE: bool = False  # Default to the bash executable for Python tasks

    def __init__(self, tool: Tool):
        self.executable: str = sys.executable if not self.USE_R_EXECUTABLE else R_EXECUTABLE
        self.template: TaskTemplate = tool.get_task_template(
            template_name=self.template_name,
            command_template=self.COMMAND_PREFIX + self.command_line_args,
            node_args=self.node_args,
            task_args=self.task_args,
            op_args=self.OP_ARGS,
        )

    def get_task(self, *_: Any, **kwargs: Any) -> Task:
        """Returns a task within the template.

        Passes in given kwargs to create an individual task.

        Note:
            kwargs given must include AT LEAST all keys in task_template_name,
            but may also contain more.
        """
        return self.template.create_task(
            compute_resources=self.compute_resources,
            cluster_name=constants.SLURM,
            fallback_queues=[constants.LONG_QUEUE],
            name=self.task_name_template.format(**kwargs),
            max_attempts=self.max_attempts,
            script_dir=SCRIPT_DIR,
            script=self.script,
            executable=self.executable,
            **kwargs,
        )


class RRMax(SevTaskTemplate):
    """Template for calculating RRmax."""

    script = 
    template_name = "Calculate RRmax"
    task_name_template = "rr_max_{rei_id}"
    command_line_args = "--rei_id {rei_id} --version_id {version_id}"
    # Varies by risk, here we assign the default
    compute_resources = get_compute_resources_rr_max()
    node_args = ["rei_id"]
    task_args = ["version_id"]

    def get_task(self, rei_id: int, *_: Any, **kwargs: Any) -> Task:
        """Get an RRmax task, modifying the compute resources and then calling super."""
        self.compute_resources = get_compute_resources_rr_max(rei_id)
        return super().get_task(rei_id=rei_id, **kwargs)


class AggregateRRMax(SevTaskTemplate):
    """Template for calculating aggregate RRmaxes."""

    script = 
    template_name = "Aggregate RRmax"
    task_name_template = "agg_rr_max_{aggregate_rei_id}"
    command_line_args = "--aggregate_rei_id {aggregate_rei_id} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(0.5),
        constants.MEMORY: "2G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["aggregate_rei_id"]
    task_args = ["version_id"]


class SummarizeRRMax(SevTaskTemplate):
    """Template for calculating RRmax summaries."""

    script = 
    template_name = "Summarize RRmax"
    task_name_template = "summarize_rrmax_{rei_id}"
    command_line_args = "--rei_id {rei_id} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(0.5),
        constants.MEMORY: "2G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["rei_id"]
    task_args = ["version_id"]


class TemperatureEtl(SevTaskTemplate):
    """Template for ETLing temperature SEVs."""

    script = 
    template_name = "ETL temperature SEVs"
    task_name_template = "temperature_etl_{location_id}"
    command_line_args = "--location_id {location_id} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(0.5),
        constants.MEMORY: "10G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["location_id"]
    task_args = ["version_id"]


class CacheExposure(SevTaskTemplate):
    """Template for caching exposure draws for edensity risks."""

    script = 
    template_name = "Cache exposure (edensity)"
    task_name_template = "cache_exposure_{rei_id}_{location_id}"
    command_line_args = (
        "--rei_id {rei_id} --location_id {location_id} --version_id {version_id}"
    )
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(0.5),
        constants.MEMORY: "10G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["rei_id", "location_id"]
    task_args = ["version_id"]


class RiskPrevalence(SevTaskTemplate):
    """Template for calculating risk prevalence for edensity risks."""

    USE_R_EXECUTABLE = True

    script = 
    template_name = "Calculate risk prevalence (edensity)"
    task_name_template = "risk_prevalence_{rei_id}_{location_id}"
    command_line_args = "{rei_id} {location_id} {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(5),
        constants.MEMORY: "4G",
        constants.NUM_CORES: 2,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["rei_id", "location_id"]
    task_args = ["version_id"]


class Sevs(SevTaskTemplate):
    """Template for calculating SEVs."""

    script = 
    template_name = "Calculate SEVs"
    task_name_template = "sev_calc_{rei_id}_{location_id}"
    command_line_args = (
        "--rei_id {rei_id} --location_id {location_id} --version_id {version_id}"
    )
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(24),
        constants.MEMORY: "10G",
        constants.NUM_CORES: 2,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["rei_id", "location_id"]
    task_args = ["version_id"]


class LocationAggregation(SevTaskTemplate):
    """Template for SEV location aggregation."""

    script = 
    template_name = "Aggregate locations for SEVs"
    task_name_template = "loc_agg_{rei_id}"
    command_line_args = "--rei_id {rei_id} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(24),
        constants.MEMORY: "45G",
        constants.NUM_CORES: 10,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["rei_id"]
    task_args = ["version_id"]


class SummarizeSevs(SevTaskTemplate):
    """Template for calculating SEV summaries."""

    script = 
    template_name = "Summarize SEVs"
    task_name_template = "summarize_sev_{location_id}"
    command_line_args = "--location_id {location_id} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(24),
        constants.MEMORY: "12G",
        constants.NUM_CORES: 10,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["location_id"]
    task_args = ["version_id"]


class Upload(SevTaskTemplate):
    """Template for uploading summaries."""

    script = 
    template_name = "Upload"
    task_name_template = "upload_{measure}"
    command_line_args = "--measure {measure} --version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(24),
        constants.MEMORY: "46G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = ["measure"]
    task_args = ["version_id"]


class CompareVersion(SevTaskTemplate):
    """Template for mangaging compare versions at the end of a run."""

    script = 
    template_name = "Manage compare version"
    task_name_template = "compare_version"
    command_line_args = "--version_id {version_id}"
    compute_resources = {
        constants.MAX_RUNTIME: _hours_to_seconds(0.25),
        constants.MEMORY: "2G",
        constants.NUM_CORES: 1,
        constants.QUEUE: constants.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]
