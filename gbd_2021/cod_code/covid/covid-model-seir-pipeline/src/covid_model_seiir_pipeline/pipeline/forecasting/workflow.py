import shutil
from typing import Dict

from covid_shared import (
    ihme_deps,
    workflow,
)

import covid_model_seiir_pipeline
from covid_model_seiir_pipeline.pipeline.forecasting.specification import (
    ScenarioSpecification,
    FORECAST_JOBS,
)


class BetaResidualScalingTaskTemplate(workflow.TaskTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f'{FORECAST_JOBS.scaling}_{{scenario}}'
    command_template = (
        f"{shutil.which('stask')} "
        f"{FORECAST_JOBS.scaling} "
        "--forecast-version {forecast_version} "
        "--scenario {scenario} "
        "-vv"
    )
    node_args = ['scenario']
    task_args = ['forecast_version']


class BetaForecastTaskTemplate(workflow.TaskTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{FORECAST_JOBS.forecast}_{{scenario}}_{{draw_id}}"
    command_template = (
        f"{shutil.which('stask')} "
        f"{FORECAST_JOBS.forecast} "
        "--forecast-version {forecast_version} "
        "--scenario {scenario} "
        "--draw-id {draw_id} "
        "-vv"
    )
    node_args = ['scenario', 'draw_id']
    task_args = ['forecast_version']


class ForecastWorkflow(workflow.WorkflowTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    workflow_name_template = 'seiir-forecast-{version}'
    task_template_classes = {
        FORECAST_JOBS.scaling: BetaResidualScalingTaskTemplate,
        FORECAST_JOBS.forecast: BetaForecastTaskTemplate,
    }

    def attach_tasks(self, n_draws: int, scenarios: Dict[str, ScenarioSpecification]):
        scaling_template = self.task_templates[FORECAST_JOBS.scaling]

        for scenario_name, scenario_spec in scenarios.items():
            # Computing the beta scaling parameters is the first step for each
            # scenario forecast.
            scaling_task = scaling_template.get_task(
                forecast_version=self.version,
                scenario=scenario_name
            )
            self.workflow.add_task(scaling_task)
            self._attach_forecast_tasks(scenario_name, n_draws, scaling_task)

    def _attach_forecast_tasks(self, scenario_name: str, n_draws: int, *upstream_tasks: ihme_deps.Task) -> None:
        forecast_template = self.task_templates[FORECAST_JOBS.forecast]

        for draw in range(n_draws):
            forecast_task = forecast_template.get_task(
                forecast_version=self.version,
                draw_id=draw,
                scenario=scenario_name,
            )
            for upstream_task in upstream_tasks:
                forecast_task.add_upstream(upstream_task)
            self.workflow.add_task(forecast_task)
