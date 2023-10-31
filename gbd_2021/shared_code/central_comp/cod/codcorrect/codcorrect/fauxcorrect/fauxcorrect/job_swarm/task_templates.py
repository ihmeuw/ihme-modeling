"""Task template classes for jobmon workflow."""
import abc
import pathlib
import sys
from typing import Dict, List, Union

import numpy as np

from jobmon.client.api import ExecutorParameters, Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from fauxcorrect.utils.constants import Jobmon, Scatters


# Set prefix to three levels up (parent + 2)
# resolve() gives us the absolute path
_SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2]


def get_jobmon_tool() -> Tool:
    """Get jobmon Tool for CodCorrect using a specific tool version id."""
    return Tool(name=Jobmon.TOOL, active_tool_version_id=Jobmon.TOOL_VERSION_ID)


class CodTaskTemplate(abc.ABC):
    """Base class for jobmon Task Templates.

    Class fields to be filled in:
        script: launch script for task, ie validate_draws.py
        template name: name of the task template, or stage, ie. validate
        task_name_template: template for the SGE job name for a task.
            Formatable string for tasks with varying args. Note: kwargs given
            to TaskTemplate.get_task must include AT LEAST all keys in task_template_name,
            but may also contain more
        command_line_args: formatable string of args to be passed in for the task.
            Ie '--version_id {version_id} --gbd_round_id {gbd_round_id}'. Args follow
            the following category order: hardcoded args, node_args, task_args. Within
            each category, args are sorted alphabetically.
        executor_parameters: dictionary of executor parameters relevant to SGE. Expects at
            most four args: 'num_cores', 'm_mem_free', 'max_runtime_seconds', 'queue'.
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
    template_name: str
    task_name_template: str
    command_line_args: str
    executor_parameters: Dict[str, Union[str, int]]
    node_args: List[str]
    task_args: List[str]

    SCRIPT_DIR: str = _SCRIPT_DIR
    COMMAND_PREFIX: str = "{python} {script_dir}/{script} "
    OP_ARGS: List[str] = ["python", "script_dir", "script"]

    def __init__(self, tool: Tool):
        self.python: str = sys.executable
        self.jobmon_template: TaskTemplate = tool.get_task_template(
            template_name=self.template_name,
            command_template=self.COMMAND_PREFIX + self.command_line_args,
            node_args=self.node_args,
            task_args=self.task_args,
            op_args=self.OP_ARGS,
        )

    def get_task(self, *_, **kwargs) -> Task:
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
            executor_parameters=ExecutorParameters(**self.executor_parameters),
            name=self.task_name_template.format(**kwargs),
            script_dir=self.SCRIPT_DIR,
            script=self.script,
            python=self.python,
            **kwargs
        )
        return task


class AppendShocks(CodTaskTemplate):
    """Template for appending shocks tasks."""

    script = "append_shocks.py"
    template_name = "append_shocks"
    task_name_template = "append_shocks_{location_id}_{sex_id}"
    command_line_args = (
        "--machine_process codcorrect " +
        "--location_id {location_id} " +
        "--most_detailed_location {most_detailed_location} " +
        "--sex_id {sex_id} " +
        "--measure_ids {measure_ids} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "20G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "most_detailed_location", "sex_id"]
    task_args = ["measure_ids", "parent_dir", "version_id"]


class ApplyCorrection(CodTaskTemplate):
    """Template for correction tasks."""

    script = "apply_correction.py"
    template_name = "correction"
    task_name_template = "apply_correction_{location_id}_{sex_id}"
    command_line_args = (
        "--action correct " +
        "--location_id {location_id} " +
        "--sex_id {sex_id} " +
        "--env_version_id {env_version_id} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "20G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["env_version_id", "gbd_round_id", "parent_dir", "version_id"]


class CacheEnvelopeDraws(CodTaskTemplate):
    """Template for caching mortality envelope draws."""

    script = "cache_mortality_inputs.py"
    template_name = "cache_envelope_draws"
    task_name_template = "cache_envelope_draws"
    command_line_args = (
        "--machine_process codcorrect " +
        "--mort_process envelope_draws " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "150G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CacheEnvelopeSummary(CodTaskTemplate):
    """Template for caching mortality envelope summary (mean)."""
    script = "cache_mortality_inputs.py"
    template_name = "cache_envelope_summary"
    task_name_template = "cache_envelope_summary"
    command_line_args = (
        "--machine_process codcorrect " +
        "--mort_process envelope_summary " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CachePopulation(CodTaskTemplate):
    """Template for caching population. """
    script = "cache_mortality_inputs.py"
    template_name = "cache_population"
    task_name_template = "cache_population"
    command_line_args = (
        "--machine_process codcorrect " +
        "--mort_process population " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class CachePredEx(CodTaskTemplate):
    """Template for caching predicted life expectancy."""
    script = "calculate_ylls.py"
    template_name = "cache_pred_ex"
    task_name_template = "cache_pred_ex"
    command_line_args = (
        "--action cache " +
        "--machine_process codcorrect " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["gbd_round_id", "parent_dir", "version_id"]


class CacheRegionalScalars(CodTaskTemplate):
    """Template for caching regional scalars."""

    script = "aggregate_locations.py"
    template_name = "cache_regional_scalars"
    task_name_template = "cache_regional_scalars"
    command_line_args = (
        "--action cache " +
        "--machine_process codcorrect " +
        "--decomp_step {decomp_step} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEM_FREE: "128M",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["decomp_step", "gbd_round_id", "parent_dir", "version_id"]


class CacheSpacetimeRestriction(CodTaskTemplate):
    """Template for caching spacetime restriction."""

    script = "apply_correction.py"
    template_name = "cache_spacetime_restrictions"
    task_name_template = "cache_spacetime_restrictions"
    command_line_args = (
        "--action cache " +
        "--gbd_round_id {gbd_round_id} "
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEM_FREE: "256M",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["gbd_round_id", "parent_dir", "version_id"]


class CalculateYlls(CodTaskTemplate):
    """Template for calculating YLLs."""

    script = "calculate_ylls.py"
    template_name = "ylls"
    task_name_template = "calc_ylls_{location_id}_{sex_id}"
    command_line_args = (
        "--action calc " +
        "--machine_process codcorrect " +
        "--location_id {location_id} " +
        "--sex_id {sex_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "15G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["parent_dir", "version_id"]


class CauseAggregation(CodTaskTemplate):
    """Template for aggregating causes."""

    script = "apply_cause_aggregation.py"
    template_name = "cause_aggregation"
    task_name_template = "agg_cause_{location_id}_{sex_id}"
    command_line_args = (
        "--location_id {location_id} " +
        "--sex_id {sex_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEM_FREE: "45G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["parent_dir", "version_id"]


class CovidScalarsCalculation(CodTaskTemplate):
    """Template for calculating COVID scalars."""

    script = "calculate_covid_scalars.py"
    template_name = "covid_scalars_calculation"
    task_name_template = "covid_scalars_{location_id}_{sex_id}"
    command_line_args = (
        "--location_id {location_id} " +
        "--sex_id {sex_id} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "15G",
        Jobmon.NUM_CORES: 5,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["version_id"]


class CovidScalarsCompilation(CodTaskTemplate):
    """Template for combiling COVID scalars summaries"""

    script = "calculate_covid_scalars.py"
    template_name = "covid_scalars_compilation"
    task_name_template = "covid_scalars_compilation"
    command_line_args = (
        "--compile " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 14400,
        Jobmon.MEM_FREE: "50G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["version_id"]


class Diagnostics(CodTaskTemplate):
    """Template for CodCorrect diagnostics creation.

    Currently for uploading pre-correction numbers to be compared
    with post-correction.
    """
    script = "create_diagnostics.py"
    template_name = "diagnostics"
    task_name_template = (
        "diagnostics_{location_id}_{sex_id}"
    )
    command_line_args = (
        "--location_id {location_id} " +
        "--sex_id {sex_id} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "15G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "sex_id"]
    task_args = ["version_id"]


class LocationAggregation(CodTaskTemplate):
    """Template for aggregating locations."""

    script = "aggregate_locations.py"
    template_name = "location_aggregation"
    task_name_template = (
        "agg_location_{aggregation_type}_{location_set_id}_{measure_id}_{year_id}"
    )
    command_line_args = (
        "--action location_aggregation " +
        "--machine_process codcorrect " +
        "--aggregation_type {aggregation_type} " +
        "--location_set_id {location_set_id} " +
        "--measure_id {measure_id} " +
        "--year_id {year_id} " +
        "--decomp_step {decomp_step} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 21600,
        Jobmon.MEM_FREE: "45G",
        Jobmon.NUM_CORES: 12,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["aggregation_type", "location_set_id", "measure_id", "year_id"]
    task_args = ["decomp_step", "gbd_round_id", "parent_dir", "version_id"]


class Monitor(CodTaskTemplate):
    """Template for workflow monitor job."""

    script = "monitor_jobs.py"
    template_name = "monitor"
    task_name_template = "monitor_jobs"
    command_line_args = (
        "--parent_dir {parent_dir} " +
        "--workflow_args {workflow_args}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 8 * 24 * 60 * 60,  # 8 days
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.LONG_QUEUE,
    }
    node_args = []
    task_args = ["parent_dir", "workflow_args"]


class PostScriptum(CodTaskTemplate):
    """Template for post scriptum job that sets GBD process version active and creates
    compare version.
    """

    script = "post_scriptum_upload.py"
    template_name = "post_scriptum"
    task_name_template = "post_scriptum_upload"
    command_line_args = (
        "--machine_process codcorrect " +
        "--decomp_step {decomp_step} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--process_version_id {process_version_id}"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 600,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = ["decomp_step", "gbd_round_id", "parent_dir", "process_version_id"]


class Scatters(CodTaskTemplate):
    """Template for scatters generation task."""

    script = Scatters.SCRIPT_PATH
    template_name = "scatters"
    task_name_template = "codcorrect_scatters"
    command_line_args = (
        "--x-name {x_name} " +
        "--y-name {y_name} " +
        "--x-pvid {x_pvid} " +
        "--y-pvid {y_pvid} " +
        "--x-gbd-round-id {x_gbd_round_id} " +
        "--y-gbd-round-id {y_gbd_round_id} " +
        "--x-decomp-step {x_decomp_step} " +
        "--y-decomp-step {y_decomp_step} " +
        "--year-ids {year_ids} " +
        "--plotdir {plotdir} " +
        "--fail-safely"
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 900,
        Jobmon.MEM_FREE: "10G",
        Jobmon.NUM_CORES: 1,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = []
    task_args = [
        "x_name", "y_name", "x_pvid", "y_pvid", "x_gbd_round_id", "y_gbd_round_id",
        "x_decomp_step", "y_decomp_step", "year_ids", "plotdir"
    ]

    # Overwrite class constant as we use an outside script
    SCRIPT_DIR = ""


class SummarizeCod(CodTaskTemplate):
    """Template for Cod database summaries.

    Only run for deaths, not YLLS. But measure_id is left in 
    task name, hence the 1.
    """

    script = "summarize_cod.py"
    template_name = "summarize_cod"
    task_name_template = "summarize_cod_1_{location_id}_{year_id}"
    command_line_args = (
        "--machine_process codcorrect " +
        "--location_id {location_id} " +
        "--year_id {year_id} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "year_id"]
    task_args = ["gbd_round_id", "parent_dir", "version_id"]


class SummarizeGbd(CodTaskTemplate):
    """Template for GBD database summaries."""

    script = "summarize_gbd.py"
    template_name = "summarize_gbd"
    task_name_template = "summarize_gbd_{measure_id}_{location_id}_{year_id}"
    command_line_args = (
        "--machine_process codcorrect " +
        "--location_id {location_id} " +
        "--measure_id {measure_id} " +
        "--year_id {year_id} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEM_FREE: "5G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "measure_id", "year_id"]
    task_args = ["gbd_round_id", "parent_dir", "version_id"]


class SummarizePercentChange(CodTaskTemplate):
    """Template for GBD database percent change summaries."""

    script = "summarize_pct_change.py"
    template_name = "summarize_pct_change"
    task_name_template = (
        "summarize_pct_change_{measure_id}_{location_id}_{year_start_id}_{year_end_id}"
    )
    command_line_args = (
        "--machine_process codcorrect " +
        "--location_id {location_id} " +
        "--measure_id {measure_id} " +
        "--year_start_id {year_start_id} " +
        "--year_end_id {year_end_id} " +
        "--gbd_round_id {gbd_round_id} " +
        "--parent_dir {parent_dir} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 10800,
        Jobmon.MEM_FREE: "10G",
        Jobmon.NUM_CORES: 2,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["location_id", "measure_id", "year_start_id", "year_end_id"]
    task_args = ["gbd_round_id", "parent_dir", "version_id"]


class Upload(CodTaskTemplate):
    """Template for upload jobs."""

    script = "upload.py"
    template_name = "upload"
    task_name_template = "upload_{database}_{upload_type}_{measure_id}"
    command_line_args = (
        "--machine_process codcorrect " +
        "--database {database} " +
        "--measure_id {measure_id} " +
        "--upload_type {upload_type} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 8 * 24 * 60 * 60,  # 8 days
        Jobmon.MEM_FREE: "100G",
        Jobmon.NUM_CORES: 20,
        Jobmon.QUEUE: Jobmon.LONG_QUEUE,
    }
    node_args = ["database", "measure_id", "upload_type"]
    task_args = ["version_id"]


class Validate(CodTaskTemplate):
    """Template for CoD model version draw validation tasks. """

    script = "validate_draws.py"
    template_name = "validate"
    task_name_template = "validate_draws_{model_version_id}"
    command_line_args = (
        "--machine_process codcorrect " +
        "--model_version_id {model_version_id} " +
        "--version_id {version_id} "
    )
    executor_parameters = {
        Jobmon.MAX_RUNTIME: 7200,
        Jobmon.MEM_FREE: "100G",
        Jobmon.NUM_CORES: 10,
        Jobmon.QUEUE: Jobmon.ALL_QUEUE,
    }
    node_args = ["model_version_id"]
    task_args = ["version_id"]
