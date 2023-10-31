import shutil

from covid_shared import workflow

import covid_model_seiir_pipeline
from covid_model_seiir_pipeline.pipeline.regression.specification import REGRESSION_JOBS


class BetaRegressionTaskTemplate(workflow.TaskTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{REGRESSION_JOBS.regression}_draw_{{draw_id}}"
    command_template = (
            f"{shutil.which('stask')} "
            f"{REGRESSION_JOBS.regression} "
            "--regression-version {regression_version} "
            "--draw-id {draw_id} "
            "-vv"
    )
    node_args = ['draw_id']
    task_args = ['regression_version']


class HospitalCorrectionFactorTaskTemplate(workflow.TaskTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    task_name_template = f"{REGRESSION_JOBS.hospital_correction_factors}"
    command_template = (
            f"{shutil.which('stask')} "
            f"{REGRESSION_JOBS.hospital_correction_factors} "
            "--regression-version {regression_version} " 
            "-vv"
    )
    node_args = []
    task_args = ['regression_version']


class RegressionWorkflow(workflow.WorkflowTemplate):
    tool = workflow.get_jobmon_tool(covid_model_seiir_pipeline)
    workflow_name_template = 'seiir-regression-{version}'
    task_template_classes = {
        REGRESSION_JOBS.regression: BetaRegressionTaskTemplate,
        REGRESSION_JOBS.hospital_correction_factors: HospitalCorrectionFactorTaskTemplate,
    }

    def attach_tasks(self, n_draws: int):
        regression_template = self.task_templates[REGRESSION_JOBS.regression]
        hospital_correction_factor_template = self.task_templates[REGRESSION_JOBS.hospital_correction_factors]

        for draw_id in range(n_draws):
            task = regression_template.get_task(
                regression_version=self.version,
                draw_id=draw_id
            )
            self.workflow.add_task(task)

        hospital_correction_task = hospital_correction_factor_template.get_task(
            regression_version=self.version,
        )
        self.workflow.add_task(hospital_correction_task)
