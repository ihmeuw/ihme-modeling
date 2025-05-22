"""Jobmon task templates for each stage in ST-GPR."""
import abc
import pathlib
from typing import Dict, List, Optional, Union

import stgpr_schema
from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate

from stgpr.lib import constants


def get_jobmon_tool() -> Tool:
    """Get jobmon Tool for ST-GPR using specific tool version."""
    return Tool(
        name=constants.workflow.TOOL_NAME,
        active_tool_version_id=constants.workflow.ACTIVE_TOOL_VERSION_ID,
    )


class StgprTaskTemplate(abc.ABC):
    """Base class for jobmon Task Templates.

    Class fields to be filled in:
        script: launch script for task, ie prep.py
        template name: name of the task template, or stage, ie. 'prep'
        task_name_template: template for the SGE job name for a task.
            Formatable string for tasks with varying args. Note: kwargs given
            to TaskTemplate.get_task must include AT LEAST all keys in task_template_name,
            but may also contain more
        command_line_args: formatable string of args to be passed in for the task.
            Ie '{run_id} {param_set}'. Args must be in the order scripts expect them in.
        compute_resources: dictionary of compute resources relevant to SGE/Slurm. Expects at
            most five args: 'cores', 'memory', 'runtime', 'j_resource', and 'project'.
        max_attempts: number of tries for a task template
        node_args: arguments that differentiate individual tasks within a task template
            (stage of ST-GPR). These are the args the stage is parallelized by. Ie for a task
            template with jobs for each holdout group, holdout would be a node_arg.
        task_args: arguments that are constant across all tasks across a task template (stage)
            Ie. 'run_id' as the same run id is passed into all jobs

    Class constants:
        SCRIPT_DIR: the directories the scripts live
        EXECUTABLE: path to the executable, defaults to python
        COMMAND_PREFIX: all commands begin with this.
            Set to "{executable} {script_dir}/{script}"
        OP_ARGS: args that are constant across the constants.workflow.
            Set to executable, script dir, script

    Fields created on instantiation:
        jobmon_template: fully formed task template

    Methods:
        get_task: Returns a task within the template given the kwargs passed in
    """

    script: str
    template_name: str
    task_name_template: str
    command_line_args: str
    compute_resources: Dict[str, Union[str, int]]
    max_attempts: int
    node_args: List[str]
    task_args: List[str]

    LEGACY_SCRIPT: bool = True  # Default to expecting script in legacy directory
    USE_R_EXECUTABLE = False  # Default to the bash executable for Python tasks.
    OP_ARGS: List[str] = ["script"]

    def __init__(self, tool: Tool):
        settings = stgpr_schema.get_settings()
        script_dir: str = (
            str(settings.path_to_model_code / "src/stgpr/legacy/model")
            if self.LEGACY_SCRIPT
            else str(settings.path_to_model_code / "src/stgpr/lib/model")
        )
        self.executable: str = (
            f"{settings.path_to_r_shell} -s"
            if self.USE_R_EXECUTABLE
            else (
                f"{pathlib.Path(__file__).parents[2] / 'lib/run_script_in_env.sh'} "
                f"{settings.conda_env_path}"
            )
        )
        self.command_prefix: str = f"{self.executable} {script_dir}/{{script}} "
        self.jobmon_template: TaskTemplate = tool.get_task_template(
            template_name=self.template_name,
            command_template=self.command_prefix + self.command_line_args,
            node_args=self.node_args,
            task_args=self.task_args,
            op_args=self.OP_ARGS,
        )

    def get_task(self, upstream_tasks: Optional[List[Task]] = None, *_, **kwargs) -> Task:
        """Returns a task within the template.

        Passes in given kwargs to create an individual task.

        Note:
            kwargs given must include AT LEAST all keys in task_template_name,
            but may also contain more.

        Arguments:
            upstream_tasks: optional, a list of tasks that the created task depends on
        """
        # kwargs may contain keyword for task name that isn't
        # a template arg, so filter out unneeded kwargs
        task_kwargs = {}
        for key, value in kwargs.items():
            if key in self.jobmon_template.active_task_template_version.template_args:
                task_kwargs[key] = value

        task = self.jobmon_template.create_task(
            compute_resources=self.compute_resources,
            script=self.script,
            name=self.task_name_template.format(**kwargs),
            max_attempts=self.max_attempts,
            upstream_tasks=upstream_tasks,
            **task_kwargs,
        )
        return task


class Cleanup(StgprTaskTemplate):
    """Template for post-model cleanup."""

    script = "clean.py"
    template_name = "clean"
    task_name_template = "clean_{run_id}_{holdout}"
    command_line_args = "{run_id} " + "{run_type} " + "{holdout} " + "{draws}"
    # Runtime potentially overwritten if draws > 0
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 600,
        constants.workflow.MEMORY: "1G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 1
    node_args = ["holdout"]
    task_args = ["run_id", "run_type", "draws"]


class Descanso(StgprTaskTemplate):
    """Template for the descanso stage, calculating intermediaries."""

    script = "calculate_intermediaries.py"
    template_name = "amp_nsv"
    task_name_template = "descanso_{run_id}_{holdout}_{param_group}"
    command_line_args = (
        "{run_id} " + "{holdout} " + "{draws} " + "{nparallel} " + "{submit_params}"
    )
    # Runtime potentially overwritten if diet model
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 300,
        constants.workflow.MEMORY: "20G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["holdout", "submit_params"]
    task_args = ["run_id", "draws", "nparallel"]


class DescansoUpload(StgprTaskTemplate):
    """Template for descanso upload."""

    script = "calculate_intermediaries_upload.py"
    template_name = "amp_nsv_upload"
    task_name_template = "descanso_upload_{run_id}_{param_set}"
    command_line_args = "{run_id} " + "{model_iteration_id} " + "{param_set} " + "{last_job}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "20G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["model_iteration_id", "param_set", "last_job"]
    task_args = ["run_id"]


class Eval(StgprTaskTemplate):
    """Template for hyperparameter evaluation."""

    script = "evaluate_rmse.py"
    template_name = "eval"
    task_name_template = "eval_{run_id}"
    command_line_args = "{run_id} " + "{run_type} " + "{holdouts} " + "{n_parameter_sets}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 300,
        constants.workflow.MEMORY: "3G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = []
    task_args = ["run_id", "run_type", "holdouts", "n_parameter_sets"]


class EvalUpload(StgprTaskTemplate):
    """Template for hyperparameter evaluation upload."""

    script = "eval_upload.py"
    template_name = "eval_upload"
    task_name_template = "eval_upload_{run_id}_{param_set}"
    command_line_args = "{run_id} " + "{model_iteration_id} " + "{param_set} " + "{last_job} "
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "5G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["model_iteration_id", "param_set", "last_job"]
    task_args = ["run_id"]


class Gpr(StgprTaskTemplate):
    """Template for stage 3, GPR (gaussian process regression)."""

    script = "run_gpr.py"
    template_name = "gpr"
    task_name_template = "gpr_{run_id}_{holdout}_{param_group}_{loc_group}"
    command_line_args = (
        "{run_id} "
        + "{holdout} "
        + "{draws} "
        + "{submit_params} "
        + "{random_seed} "
        + "{nparallel} "
        + "{loc_group}"
    )
    # Runtime, memory potentially overwritten based on number of draws
    # or if diet model
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1200,
        constants.workflow.MEMORY: 4,
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["holdout", "submit_params", "loc_group"]
    task_args = ["run_id", "draws", "nparallel", "random_seed"]


class GprUpload(StgprTaskTemplate):
    """Template for GPR upload."""

    script = "gpr_upload.py"
    template_name = "gpr_upload"
    task_name_template = "gpr_upload_{run_id}_{param_set}"
    command_line_args = "{run_id} " + "{model_iteration_id} " + "{param_set} " + "{last_job} "
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "25G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["model_iteration_id", "param_set", "last_job"]
    task_args = ["run_id"]


class PostGpr(StgprTaskTemplate):
    """Template for post-GPR (post-run) aggregation and fit statistics calculation."""

    script = "post_gpr.py"
    template_name = "post_gpr"
    task_name_template = "post_gpr_{run_id}_{holdout}_{param_group}"
    command_line_args = (
        "{run_id} " + "{holdout} " + "{run_type} " + "{holdouts} " + "{submit_params}"
    )
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 2400,
        constants.workflow.MEMORY: "100G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["holdout", "submit_params"]
    task_args = ["run_id", "run_type", "holdouts"]


class PostRake(StgprTaskTemplate):
    """Template for post-rake aggregation and summarization."""

    LEGACY_SCRIPT = False

    script = "post_rake.py"
    template_name = "post_rake"
    task_name_template = "post_rake_{run_id}"
    command_line_args = "{run_id}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 4800,
        constants.workflow.MEMORY: "100G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = []
    task_args = ["run_id"]


class Prep(StgprTaskTemplate):
    """Template for the prep stage."""

    script = "prep.py"
    template_name = "prep"
    task_name_template = "prep_{run_id}"
    command_line_args = "{run_id}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 3600,
        constants.workflow.MEMORY: "15G",
        constants.workflow.CORES: 1,
        constants.workflow.CONSTRAINTS: constants.workflow.ARCHIVE,
    }
    max_attempts = 2
    node_args = []
    task_args = ["run_id"]


class Rake(StgprTaskTemplate):
    """Template for raking subnational results to country-level."""

    script = "rake.py"
    template_name = "rake"
    task_name_template = "rake_{run_id}_{location_id}"
    command_line_args = (
        "{run_id} "
        + "0 "
        + "{draws} "  # only rake holdout #0, which has no holdout data
        + "{run_type} "
        + "{rake_logit} "
        + "{location_id}"
    )
    # Runtime, memory are assigned at runtime depending on how many
    # subnational locations are in a country, if 1000 draw-run, and if diet model
    compute_resources = {
        constants.workflow.MAX_RUNTIME: None,
        constants.workflow.MEMORY: None,
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["location_id"]
    task_args = ["run_id", "draws", "run_type", "rake_logit"]


class RakeUpload(StgprTaskTemplate):
    """Template for rake upload."""

    script = "rake_upload.py"
    template_name = "rake_upload"
    task_name_template = "rake_upload_{run_id}_{param_set}"
    command_line_args = "{run_id} " + "{model_iteration_id} " + "{param_set} " + "{last_job} "
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "25G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["model_iteration_id", "param_set", "last_job"]
    task_args = ["run_id"]


class Spacetime(StgprTaskTemplate):
    """Template for stage 2, spacetime (spaciotemporal) smoothing."""

    script = "run_spacetime.py"
    template_name = "spacetime"
    task_name_template = "st_{run_id}_{holdout}_{param_group}_{loc_group}"
    command_line_args = (
        "{run_id} "
        + "{holdout} "
        + "{run_type} "
        + "{submit_params} "
        + "{nparallel} "
        + "{loc_group}"
    )
    # Runtime, memory potentially overwritten if diet model
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1500,
        constants.workflow.MEMORY: "50G",
        constants.workflow.CORES: 6,
    }
    max_attempts = 3
    node_args = ["holdout", "submit_params", "loc_group"]
    task_args = ["run_id", "run_type", "nparallel"]


class SpacetimeUpload(StgprTaskTemplate):
    """Template for spacetime upload."""

    script = "spacetime_upload.py"
    template_name = "spacetime_upload"
    task_name_template = "spacetime_upload_{run_id}_{param_set}"
    command_line_args = "{run_id} " + "{model_iteration_id} " + "{param_set} " + "{last_job} "
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "25G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["model_iteration_id", "param_set", "last_job"]
    task_args = ["run_id"]


class Stage1(StgprTaskTemplate):
    """Template for stage 1, the linear fit.

    Stage 1 is an R job, so pass in output path rather than reading it from env variable in R.
    """

    USE_R_EXECUTABLE = True

    script = "stage1.R"
    template_name = "stage1"
    task_name_template = "stage1_{run_id}_{holdout}"
    command_line_args = "{output_path} {model_root} {holdout}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 300,
        constants.workflow.MEMORY: "3G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = ["holdout"]
    task_args = ["output_path", "model_root"]


class Stage1Upload(StgprTaskTemplate):
    """Template for stage 1 upload."""

    script = "stage1_upload.py"
    template_name = "stage1_upload"
    task_name_template = "stage1_upload_{run_id}"
    command_line_args = "{run_id}"
    compute_resources = {
        constants.workflow.MAX_RUNTIME: 1800,
        constants.workflow.MEMORY: "25G",
        constants.workflow.CORES: 1,
    }
    max_attempts = 2
    node_args = []
    task_args = ["run_id"]
