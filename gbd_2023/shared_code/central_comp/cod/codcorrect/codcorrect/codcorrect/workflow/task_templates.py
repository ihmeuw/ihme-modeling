"""Task template classes for jobmon workflow."""

import pathlib
import sys
from typing import Any, Dict, List, Union

import numpy as np

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from codcorrect.legacy.utils.constants import Jobmon
from codcorrect.legacy.utils.constants import Scatters as ScatterConst

# Script types for defining where to look for launch scripts
_LEGACY_TYPE: str = "legacy"
_CLI_TYPE: str = "cli"


def _get_script_dir(script_type: str) -> str:
    """Return script directory based on input.

    For majority, set prefix to 'launch_scripts' or 'cli' dir, 2 levels up (parent + 1).
    """
    if script_type == _LEGACY_TYPE:
        return str(pathlib.Path(__file__).parents[2] / "legacy/launch_scripts")
    elif script_type == _CLI_TYPE:
        return str(pathlib.Path(__file__).parents[2] / "cli")
    elif script_type == "":
        return ""
    else:
        raise ValueError(f"Unknown script type: '{script_type}'")


def get_jobmon_tool() -> Tool:
    """Get jobmon Tool for CodCorrect using a specific tool version id."""
    return Tool(name=Jobmon.TOOL, active_tool_version_id=Jobmon.TOOL_VERSION_ID)


class CodTaskTemplate:
    """Base class for jobmon Task Templates.

    Class fields to be filled in:
        script: launch script for task, ie validate_draws.py
        script_type: type of script, i.e. "legacy", "cli", that defines where
            the launch script will be found
        template name: name of the task template, or stage, ie. validate
        task_name_template: template for the SGE job name for a task.
            Formattable string for tasks with varying args. Note: kwargs given
            to TaskTemplate.get_task must include AT LEAST all keys in task_template_name,
            but may also contain more
        command_line_args: formattable string of args to be passed in for the task.
            Ie '--version_id {version_id} --release_id {release_id}'. Args follow
            the following category order: hardcoded args, node_args, task_args. Within
            each category, args are sorted alphabetically.
        compute_resources: dictionary of compute resources relevant to SGE/Slurm. Expects at
            most five args: 'cores', 'memory', 'runtime', 'queue', 'project'.
        max_attempts: number of tries for a task template. Defaults to 3.
        node_args: arguments that differentiate individual tasks within a task template
            (stage of CodCorrect). These are the args the stage is parallelized by.
            Ie for a task template run by location, location_id would be a node_arg.
        task_args: arguments that are constant across all tasks across a task template (stage)
            Ie. 'version_id' as the same version id is passed into all jobs

    Class constants:
        COMMAND_PREFIX: all commands begin with this. Set to "{python} {script}"
        OP_ARGS: args that are constant across the workflow. Set to python, script

    Fields created on instantiation:
        python: the python executable
        jobmon_template: fully formed task template

    Methods:
        get_task: Returns a task within the template given the kwargs passed in
    """

    script: str
    script_type: str = _LEGACY_TYPE
    template_name: str
    task_name_template: str
    command_line_args: str
    compute_resources: Dict[str, Union[str, int]]
    max_attempts: int = 3
    node_args: List[str]
    task_args: List[str]

    COMMAND_PREFIX: str = "{python} {script_dir}/{script} "
    OP_ARGS: List[str] = ["python", "script_dir", "script"]

    def __init__(self, tool: Tool):
        self.script_dir: str = _get_script_dir(self.script_type)
        self.python: str = sys.executable
        self.jobmon_template: TaskTemplate = tool.get_task_template(
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
        # Convert numpy ints into regular ints to work within jobmon
        for key, value in kwargs.items():
            if isinstance(value, np.integer):
                kwargs[key] = int(value)

        task = self.jobmon_template.create_task(
            compute_resources=self.compute_resources,
            cluster_name=Jobmon.SLURM,
            fallback_queues=[Jobmon.LONG_QUEUE],
            name=self.task_name_template.format(**kwargs),
            max_attempts=self.max_attempts,
            script_dir=self.script_dir,
            script=self.script,
            python=self.python,
            **kwargs,
        )
        return task


class AppendShocks(CodTaskTemplate):
    """Template for appending shocks tasks.
    """

    script = "append_shocks.py"
    template_name = "append_shocks"
    task_name_template = "append_shocks_{location_id}_{sex_id}"
    command_line_args = (
        "--location_id {location_id} "
        + "--most_detailed_location {most_detailed_location} "
        + "--sex_id {sex_id} "
        + "--measure_ids {measure_ids} "
        + "--parent_dir {parent_dir} "
        + "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "20G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "most_detailed_location", "sex_id"]
    task_args = ["measure_ids", "parent_dir", "version_id"]


class ApplyCorrection(CodTaskTemplate):
    """Template for correction tasks.
    """

    script = "apply_correction.py"
    template_name = "correction"
    task_name_template = "apply_correction_{location_id}_{sex_id}"
    command_line_args = (
        "--action correct "
        "--location_id {location_id} "
        "--sex_id {sex_id} "
        "--env_version_id {env_version_id} "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "20G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["env_version_id", "release_id", "parent_dir", "version_id"]


class CacheEnvelopeDraws(CodTaskTemplate):
    """Template for caching mortality envelope draws.
    """

    script = "cache_mortality_inputs.py"
    template_name = "cache_envelope_draws"
    task_name_template = "cache_envelope_draws"
    command_line_args = "--mort_process envelope_draws --version_id {version_id}"
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "150G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CacheEnvelopeSummary(CodTaskTemplate):
    """Template for caching mortality envelope summary (mean).
    """

    script = "cache_mortality_inputs.py"
    template_name = "cache_envelope_summary"
    task_name_template = "cache_envelope_summary"
    command_line_args = "--mort_process envelope_summary --version_id {version_id}"
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CachePopulation(CodTaskTemplate):
    """Template for caching population.
    """

    script = "cache_mortality_inputs.py"
    template_name = "cache_population"
    task_name_template = "cache_population"
    command_line_args = "--mort_process population --version_id {version_id}"
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CachePredEx(CodTaskTemplate):
    """Template for caching predicted life expectancy.
    """

    script = "calculate_ylls.py"
    template_name = "cache_pred_ex"
    task_name_template = "cache_pred_ex"
    command_line_args = (
        "--action cache "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["release_id", "parent_dir", "version_id"]


class CacheRegionalScalars(CodTaskTemplate):
    """Template for caching regional scalars.
    """

    script = "aggregate_locations.py"
    template_name = "cache_regional_scalars"
    task_name_template = "cache_regional_scalars"
    command_line_args = (
        "--action cache "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEMORY: "128M",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["release_id", "parent_dir", "version_id"]


class CacheSpacetimeRestriction(CodTaskTemplate):
    """Template for caching spacetime restriction.
    """

    script = "apply_correction.py"
    template_name = "cache_spacetime_restrictions"
    task_name_template = "cache_spacetime_restrictions"
    command_line_args = (
        "--action cache "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEMORY: "1G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["release_id", "parent_dir", "version_id"]


class CalculateYlls(CodTaskTemplate):
    """Template for calculating YLLs.
    """

    script = "calculate_ylls.py"
    template_name = "ylls"
    task_name_template = "calc_ylls_{location_id}_{sex_id}"
    command_line_args = (
        "--action calc "
        "--location_id {location_id} "
        "--sex_id {sex_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "15G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["parent_dir", "version_id"]


class CauseAggregation(CodTaskTemplate):
    """Template for aggregating causes.
    """

    script = "apply_cause_aggregation.py"
    template_name = "cause_aggregation"
    task_name_template = "agg_cause_{location_id}_{sex_id}"
    command_line_args = (
        "--location_id {location_id} "
        "--sex_id {sex_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEMORY: "45G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["parent_dir", "version_id"]


class Diagnostics(CodTaskTemplate):
    """Template for CodCorrect diagnostics creation.

    Currently for uploading pre-correction numbers to be compared
    with post-correction.
    """

    script = "create_diagnostics.py"
    template_name = "diagnostics"
    task_name_template = "diagnostics_{location_id}_{sex_id}"
    command_line_args = (
        "--location_id {location_id} --sex_id {sex_id} --version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "15G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["version_id"]


class LocationAggregation(CodTaskTemplate):
    """Template for aggregating locations.
    """

    script = "aggregate_locations.py"
    template_name = "location_aggregation"
    task_name_template = (
        "agg_location_{aggregation_type}_{location_set_id}_{measure_id}_{year_id}"
    )
    command_line_args = (
        "--action location_aggregation "
        "--aggregation_type {aggregation_type} "
        "--location_set_id {location_set_id} "
        "--measure_id {measure_id} "
        "--year_id {year_id} "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 43200,
        Jobmon.MEMORY: "45G",
        Jobmon.NUM_CORES: 12,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["aggregation_type", "location_set_id", "measure_id", "year_id"]
    task_args = ["release_id", "parent_dir", "version_id"]


class Monitor(CodTaskTemplate):
    """Template for workflow monitor job."""

    script = "monitor_jobs.py"
    template_name = "monitor"
    task_name_template = "monitor_jobs"
    command_line_args = (
        "--version_id {version_id} "
        "--workflow_args {workflow_args} "
        "--resume {resume} "
        "--test {test}"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 8 * 24 * 60 * 60,  # 8 days
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.LONG_QUEUE,
    }
    max_attempts = 1
    node_args = []
    task_args = ["version_id", "workflow_args", "test"]

    OP_ARGS = ["python", "script_dir", "script", "resume"]


class PostScriptum(CodTaskTemplate):
    """Template for post scriptum job that sets GBD process version active and creates
    compare version.
    """

    script = "post_scriptum_upload.py"
    template_name = "post_scriptum"
    task_name_template = "post_scriptum_upload"
    command_line_args = "--version_id {version_id}"
    compute_resources = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class Scatters(CodTaskTemplate):
    """Template for scatters generation task.
    """

    script = ScatterConst.SCRIPT_PATH
    script_type = ""  # overwrite constant as we use an outside script
    template_name = "scatters"
    task_name_template = "codcorrect_scatters"
    command_line_args = (
        "--x-name {x_name} "
        "--y-name {y_name} "
        "--x-pvid {x_pvid} "
        "--y-pvid {y_pvid} "
        "--x-release_id {x_release_id} "
        "--y-release_id {y_release_id} "
        "--year-ids {year_ids} "
        "--plotdir {plotdir} "
        "--fail-safely"
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 900,
        Jobmon.MEMORY: "10G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = [
        "x_name",
        "y_name",
        "x_pvid",
        "y_pvid",
        "x_release_id",
        "y_release_id",
        "year_ids",
        "plotdir",
    ]


class SummarizeCod(CodTaskTemplate):
    """Template for Cod database summaries.

    Only run for deaths, not YLLS. But measure_id is left in
    task name, hence the 1.
    """

    script = "summarize_cod.py"
    template_name = "summarize_cod"
    task_name_template = "summarize_cod_1_{location_id}_{year_id}"
    command_line_args = (
        "--location_id {location_id} "
        "--year_id {year_id} "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEMORY: "5G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "year_id"]
    task_args = ["release_id", "parent_dir", "version_id"]


class SummarizeGbd(CodTaskTemplate):
    """Template for GBD database summaries.
    """

    script = "summarize_gbd.py"
    template_name = "summarize_gbd"
    task_name_template = "summarize_gbd_{measure_id}_{location_id}_{year_id}"
    command_line_args = (
        "--location_id {location_id} "
        "--measure_id {measure_id} "
        "--year_id {year_id} "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEMORY: "8G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "measure_id", "year_id"]
    task_args = ["release_id", "parent_dir", "version_id"]


class SummarizePercentChange(CodTaskTemplate):
    """Template for GBD database percent change summaries.
    """

    script = "summarize_pct_change.py"
    template_name = "summarize_pct_change"
    task_name_template = (
        "summarize_pct_change_{measure_id}_{location_id}_{year_start_id}_{year_end_id}"
    )
    command_line_args = (
        "--location_id {location_id} "
        "--measure_id {measure_id} "
        "--year_start_id {year_start_id} "
        "--year_end_id {year_end_id} "
        "--release_id {release_id} "
        "--parent_dir {parent_dir} "
        "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEMORY: "10G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "measure_id", "year_start_id", "year_end_id"]
    task_args = ["release_id", "parent_dir", "version_id"]


class Upload(CodTaskTemplate):
    """Template for upload jobs.
    """

    script = "upload.py"
    template_name = "upload"
    task_name_template = "upload_{database}_{upload_type}_{measure_id}"
    command_line_args = (
        "--database {database} "
        "--measure_id {measure_id} "
        "--upload_type {upload_type} "
        "--version_id {version_id} "
    )
    compute_resources = {
        Jobmon.MAX_RUNTIME: 8 * 24 * 60 * 60,  # 8 days
        Jobmon.MEMORY: "100G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.LONG_QUEUE,
    }
    node_args = ["database", "measure_id", "upload_type"]
    task_args = ["version_id"]


class Validate(CodTaskTemplate):
    """Template for CoD model version draw validation tasks.
    """

    script = "validate_draws.py"
    template_name = "validate"
    task_name_template = "validate_draws_{model_version_id}"
    command_line_args = "--model_version_id {model_version_id} --version_id {version_id} "
    compute_resources = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEMORY: "100G",
        Jobmon.NUM_CORES: 10,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["model_version_id"]
    task_args = ["version_id"]
