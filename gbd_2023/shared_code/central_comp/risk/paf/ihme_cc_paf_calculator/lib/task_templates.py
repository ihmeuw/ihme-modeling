import pathlib
from typing import Any, Dict, List, Optional, Union

from gbd import enums
from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from ihme_cc_paf_calculator.lib import constants

CONDA = 
SCRIPT_DIR: str = 


def hours_to_seconds(hours: Union[int, float]) -> int:
    """Converts hours into seconds."""
    return int(hours * 60 * 60)


def get_jobmon_tool() -> Tool:
    """Get jobmon Tool for PAF Calculator."""
    return Tool(name=constants.JOBMON_TOOL)


class PafTaskTemplate:
    """Base class for Task Templates.

    Class fields to be filled in:
        * script: launch script for task
        * conda_env: conda environment to activate and run task in
        * template name: name of the task template, or stage
        * task_name_template: template for the job name for a task.
            Formattable string for tasks with varying args. Note: kwargs given
            to TaskTemplate.get_task must include AT LEAST all keys in task_template_name,
            but may also contain more
        * command_line_args: formattable string of args to be passed in for the task.
            Ie '--version_id {version_id} --release_id {release_id}'. Args follow
            the following category order: hardcoded args, node_args, task_args. Within
            each category, args are sorted alphabetically.
        * compute_resources: dictionary of compute resources relevant to Slurm. Expects at
            most five args: 'cores', 'memory', 'runtime', 'queue', 'project'.
        * max_attempts: number of tries for a task template. Defaults to 3.
        * node_args: arguments that differentiate individual tasks within a task template
            (stage of PAF Calculator). These are the args the stage is parallelized by.
            Ie for a task template run by location, location_id would be a node_arg.
        * task_args: arguments that are constant across all tasks across a task template
            (stage). Ie. 'version_id' as the same version id is passed into all jobs

    Class constants:
        * COMMAND_PREFIX: all commands begin with this. Set to "{python} {script}"
        * OP_ARGS: args that are constant across the workflow. Set to python, script

    Fields created on instantiation:
        * python: the python executable
        * template: fully formed task template
    """

    script: str
    conda_env: str
    compute_resources: Dict[str, Union[str, int]]
    template_name: str
    task_name_template: str
    command_line_args: str
    max_attempts: int = 3
    node_args: List[str]
    task_args: List[str]

    COMMAND_PREFIX: str = "{conda} run -p {conda_env} python "
    OP_ARGS: List[str] = ["conda", "conda_env", "script_dir", "script"]

    def __init__(
        self, tool: Tool, conda_env: str, compute_resources: Dict[str, Union[str, int]]
    ):
        self.template: TaskTemplate = tool.get_task_template(
            template_name=self.template_name,
            command_template=self.COMMAND_PREFIX + self.command_line_args,
            node_args=self.node_args,
            task_args=self.task_args,
            op_args=self.OP_ARGS,
        )
        self.conda_env = conda_env
        self.compute_resources = compute_resources

    def get_task(self, **kwargs: Any) -> Task:
        """Returns a task within the template.

        Passes in given kwargs to create an individual task.

        Note:
            kwargs given must include AT LEAST all keys in task_template_name,
            but may also contain more.
        """
        return self.template.create_task(
            compute_resources=self.compute_resources,
            cluster_name=enums.Cluster.SLURM.value,
            fallback_queues=[enums.Queue.LONG.value],
            name=self.task_name_template.format(**kwargs),
            max_attempts=self.max_attempts,
            conda=CONDA,
            script_dir=SCRIPT_DIR,
            script=self.script,
            conda_env=self.conda_env,
            **kwargs,
        )


class ExposureMinMax(PafTaskTemplate):
    """Template for calculating exposure min/max."""

    script = 
    template_name = "calculate exposure min/max"
    task_name_template = "exposure_min_max"
    command_line_args = "--output_dir {output_dir}"
    node_args = []
    task_args = ["output_dir"]


def cache_exp_tmrel_resources(n_years: int, n_mes: int) -> Dict[str, Union[str, int]]:
    """Resource prediction for exposure and TMREL caching tasks.

    Resource numbers are based off of a combination of Jobmon resource reporting (which is
    essentially Slurm resource usage reporting) in addition to Memray profiling for memory
    usage. See CCYELLOW-371 for a spreadsheet with some comparisons.
    """
    memory = float(250 * pow(1.2, n_years - 1) * pow(1.05, n_mes - 1))
    compute_resources = {
        constants.MAX_RUNTIME: hours_to_seconds(1),
        constants.MEMORY: f"{int(round(memory))}M",
    }
    return compute_resources


class CacheExposureAndTmrelDraws(PafTaskTemplate):
    """Template for caching exposure, exposure SD and TMREL draws."""

    script = 
    template_name = "cache exposure and TMREL draws"
    task_name_template = "cache_{location_id}"
    command_line_args = "--location_id {location_id} --output_dir {output_dir}"
    node_args = ["location_id"]
    task_args = ["output_dir"]


def paf_resources(
    is_continuous: bool,
    n_draws: int,
    n_years: int,
    n_mes: int,
    intervention_rei_id: Optional[int] = None,
) -> Dict[str, Union[str, int]]:
    """Resource prediction function for PAF tasks.

    Resource numbers are based off of a combination of Jobmon resource reporting (which is
    essentially Slurm resource usage reporting) in addition to Memray profiling for memory
    usage. See CCYELLOW-371 for a spreadsheet with some comparisons. Memory caps added in
    CCPURPLE-2373.
    """
    if is_continuous:
        runtime = hours_to_seconds(72)
        memory = float(1600 * pow(1.0005, n_draws - 1) * pow(1.12, n_years - 1))
        # Averted Burden drugs with risks as a health outcome need a little extra memory.
        if intervention_rei_id is not None:
            memory += 2500.0
        if n_draws <= constants.LOW_MEMORY_CAP_DRAW_CUTOFF:
            memory = min(memory, constants.LOW_MEMORY_CAP)
        else:
            memory = min(memory, constants.HIGH_MEMORY_CAP)
    else:
        runtime = hours_to_seconds(1)
        memory = float(
            250 * pow(1.1, n_mes) * pow(1.0001, n_draws - 1) * pow(1.05, n_years - 1)
        )
    compute_resources = {
        constants.MAX_RUNTIME: runtime,
        constants.MEMORY: f"{int(round(memory))}M",
    }
    return compute_resources


def paf_max_tasks(compute_resources: Dict[str, Union[str, int]], max_mem_in_tb: int) -> int:
    """Returns max number of concurrent PAF tasks for a given total memory, in TB."""
    mem_string = compute_resources[constants.MEMORY]
    mem_int_mb = int(mem_string.split("M")[0])  # type: ignore
    mem_int_tb = mem_int_mb / 1e6
    max_tasks = min(10000, int(round(max_mem_in_tb / mem_int_tb)))
    return max_tasks


class PafCalculation(PafTaskTemplate):
    """Template for calculating PAFs."""

    script = 
    template_name = "PAF calculation"
    task_name_template = "paf_calc_{location_id}_{cause_id}"
    command_line_args = (
        "--location_id {location_id} --cause_id {cause_id} --output_dir {output_dir}"
    )
    node_args = ["location_id", "cause_id"]
    task_args = ["output_dir"]


class CauseConversion(PafTaskTemplate):
    """Template for converting non-GBD-cause PAFs to standard cause PAFs. Used for
    converting fractures to injuries for bone mineral density (BMD) and sequelae to
    causes for occupational noise. A post-processing step in PAF calculation.
    """

    script = 
    template_name = "cause conversion"
    task_name_template = "cause_conversion_{location_id}"
    command_line_args = "--location_id {location_id} --output_dir {output_dir}"
    node_args = ["location_id"]
    task_args = ["output_dir"]


class Cleanup(PafTaskTemplate):
    """Template for cleaning up exposure and TMREL cache."""

    script = 
    template_name = "cleanup"
    task_name_template = "cleanup_{location_id}"
    command_line_args = "--location_id {location_id} --output_dir {output_dir}"
    node_args = ["location_id"]
    task_args = ["output_dir"]


class SaveResults(PafTaskTemplate):
    """Template for running save_results."""

    script = 
    template_name = "save_results"
    task_name_template = "save_results_{model_version_id}"
    command_line_args = "--model_version_id {model_version_id} --output_dir {output_dir}"
    node_args = ["model_version_id"]
    task_args = ["output_dir"]
