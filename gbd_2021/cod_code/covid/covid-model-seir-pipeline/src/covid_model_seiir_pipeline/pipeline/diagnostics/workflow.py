import shutil
from typing import List

from covid_shared import workflow

import covid_model_seiir_pipeline
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import (
    DIAGNOSTICS_JOBS,
)


class GridPlotsTaskTemplate(workflow.TaskTemplate):

    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{DIAGNOSTICS_JOBS.grid_plots}_{{plot_name}}"
    command_template = (
            f"{shutil.which('stask')} "
            f"{DIAGNOSTICS_JOBS.grid_plots} "
            "--diagnostics-version {diagnostics_version} "
            "--name {plot_name} "
            "-vv"
    )
    node_args = ['plot_name']
    task_args = ['diagnostics_version']


class CompareCSVTaskTemplate(workflow.TaskTemplate):

    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{DIAGNOSTICS_JOBS.compare_csv}"
    command_template = (
            f"{shutil.which('stask')} "
            f"{DIAGNOSTICS_JOBS.compare_csv} "
            "--diagnostics-version {diagnostics_version} "            
            "-vv"
    )
    node_args = []
    task_args = ['diagnostics_version']


class ScattersTaskTemplate(workflow.TaskTemplate):

    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{DIAGNOSTICS_JOBS.scatters}_{{plot_name}}"
    command_template = (
            f"{shutil.which('stask')} "
            f"{DIAGNOSTICS_JOBS.scatters} "
            "--diagnostics-version {diagnostics_version} "
            "--name {plot_name} "
            "-vv"
    )
    node_args = ['plot_name']
    task_args = ['diagnostics_version']


class DiagnosticsWorkflow(workflow.WorkflowTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    workflow_name_template = 'seiir-diagnostics-{version}'
    task_template_classes = {
        DIAGNOSTICS_JOBS.grid_plots: GridPlotsTaskTemplate,
        DIAGNOSTICS_JOBS.compare_csv: CompareCSVTaskTemplate,
        DIAGNOSTICS_JOBS.scatters: ScattersTaskTemplate,
    }
    # Jobs here are not homogeneous so it's useful to get all failures if
    # things do fail.
    fail_fast = False

    def attach_tasks(self,
                     grid_plots_task_names: List[str],
                     compare_csv: bool,
                     scatters_task_names: List[str]) -> None:
        grid_plots_template = self.task_templates[DIAGNOSTICS_JOBS.grid_plots]
        compare_csv_template = self.task_templates[DIAGNOSTICS_JOBS.compare_csv]
        scatters_template = self.task_templates[DIAGNOSTICS_JOBS.scatters]

        for grid_plots_task_name in grid_plots_task_names:
            grid_plots_task = grid_plots_template.get_task(
                diagnostics_version=self.version,
                plot_name=grid_plots_task_name,
            )
            self.workflow.add_task(grid_plots_task)
        if compare_csv:
            compare_csv_task = compare_csv_template.get_task(
                diagnostics_version=self.version,
            )
            self.workflow.add_task(compare_csv_task)
        for scatters_task_name in scatters_task_names:
            scatters_task = scatters_template.get_task(
                diagnostics_version=self.version,
                plot_name=scatters_task_name,
            )
            self.workflow.add_task(scatters_task)

