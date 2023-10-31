import shutil
from typing import Dict

from covid_shared import workflow

import covid_model_seiir_pipeline
from covid_model_seiir_pipeline.pipeline.parameter_fit.specification import FIT_JOBS, FitScenario


class BetaFitTaskTemplate(workflow.TaskTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{FIT_JOBS.fit}_{{scenario}}_{{draw_id}}"
    command_template = (
        f"{shutil.which('stask')} "
        f"{FIT_JOBS.fit} "
        "--fit-version {fit_version} "
        "--scenario {scenario} "
        "--draw-id {draw_id} "
        "-vv"
    )
    node_args = ['scenario', 'draw_id']
    task_args = ['fit_version']


class FitWorkflow(workflow.WorkflowTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    workflow_name_template = 'seiir-oos-fit-{version}'
    task_template_classes = {
        FIT_JOBS.fit: BetaFitTaskTemplate,
    }

    def attach_tasks(self, n_draws: int, scenarios: Dict[str, FitScenario]):
        fit_template = self.task_templates[FIT_JOBS.fit]

        for scenario_name, scenario_spec in scenarios.items():
            for draw in range(n_draws):
                fit_task = fit_template.get_task(
                    fit_version=self.version,
                    draw_id=draw,
                    scenario=scenario_name,
                )
                self.workflow.add_task(fit_task)
