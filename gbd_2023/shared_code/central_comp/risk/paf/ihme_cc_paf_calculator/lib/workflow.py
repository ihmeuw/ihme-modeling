import datetime
import itertools
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Final, List, Type, Union

import ihme_cc_averted_burden
import ihme_cc_risk_utils
from gbd import enums
from jobmon.client import workflow
from jobmon.client.task import Task

from ihme_cc_paf_calculator.lib import constants, io_utils, logging_utils
from ihme_cc_paf_calculator.lib import task_templates as tt

logger = logging_utils.module_logger(__name__)


class WorkflowManager:
    """Creates a workflow for a PAF Calculator run."""

    _max_paf_task_memory_in_tb: Final[int] = 25

    def __init__(
        self,
        conda_env: str,
        settings: constants.PafCalculatorSettings,
        location_ids: List[int],
        cause_ids: List[int],
        exposure_me_ids: List[int],
        is_continuous: bool,
    ):
        self.conda_env = conda_env
        self.settings = settings
        self.location_ids = location_ids
        # Use placeholder cause for risks without RRs (intimate partner violence)
        self.cause_ids = cause_ids if cause_ids else [-1]
        self.exposure_me_ids = exposure_me_ids
        self.is_continuous = is_continuous
        self.tool = tt.get_jobmon_tool()

        workflow_name = constants.JOBMON_WORKFLOW_NAME.format(
            rei_id=self.settings.rei_id,
            me_id=self.settings.paf_modelable_entity_id,
            mv_id=self.settings.paf_model_version_id,
        )
        self.model_version_ids = self._get_model_version_ids()

        # Get rei short name from metadata
        rei_metadata = io_utils.get(
            Path(settings.output_dir), constants.CacheContents.REI_METADATA
        )
        rei = rei_metadata["rei"].iat[0]

        if self.settings.resume:
            workflow_args = ihme_cc_risk_utils.get_failed_workflow_args(workflow_name)
        else:
            workflow_args = "\n".join(f"{k} = {v}" for k, v in asdict(self.settings).items())

        self.workflow = self.tool.create_workflow(
            name=workflow_name,
            default_cluster_name=enums.Cluster.SLURM.value,
            default_compute_resources_set={
                enums.Cluster.SLURM.value: {
                    "stderr": ,
                    "stdout": ,
                    "project": self.settings.cluster_proj,
                    constants.QUEUE: enums.Queue.ALL.value,
                    constants.NUM_CORES: 1,
                }
            },
            workflow_args=workflow_args,
            workflow_attributes={
                "timestamp": datetime.datetime.now().isoformat(),
                "rei_id": settings.rei_id,
                "rei": rei,
                "model_version_id": self.settings.paf_model_version_id,
            },
        )
        self.task_map: Dict[str, Dict[str, Task]] = defaultdict(dict)
        logger.info(f"workflow created with workflow_args:\n{workflow_args}")

    def _get_model_version_ids(self) -> List[int]:
        """Get model version IDs for the run.

        For the majority of risks there's either 1 MVID (categorical, custom, continuous w/ no
        mediation) or 2 MVIDs (continuous with mediation). There are two other exceptions,
        * LBW/SG creates MVIDs for its two child risks LBW and SG as well, there's 3 MVIDs.
        * Avertable counterfactuals for Averted Burden create MVIDs for Unavertable as well,
          there's 2 MVIDs.
        """
        model_version_ids = [self.settings.paf_model_version_id]
        if self.settings.paf_unmediated_modelable_entity_id is not None:
            model_version_ids.append(self.settings.paf_unmediated_model_version_id)

        # Add child MVIDs
        if self.settings.rei_id == constants.LBWSGA_REI_ID:
            model_version_ids += [
                self.settings.paf_lbw_model_version_id,
                self.settings.paf_sg_model_version_id,
            ]
        # Add unavertable MVID
        if self.settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
            drug_rei_id = self.settings.intervention_rei_id or self.settings.rei_id
            if (
                drug_rei_id
                == ihme_cc_averted_burden.get_all_rei_ids_for_drug(
                    rei_id=drug_rei_id
                ).avertable_rei_id
            ):
                model_version_ids += [self.settings.paf_unavertable_model_version_id]

        return model_version_ids

    def add_task(self, task: Task, task_template: tt.PafTaskTemplate) -> None:
        """Adds task to self.workflow and self.task_map.

        Wrapper for workflow.add_task(). New entry added for
        self.task_map -> template name -> task name
        """
        self.task_map[task_template.template_name][task.name] = task
        self.workflow.add_task(task)

    def get_all_tasks_from_stage(
        self, task_template: Union[Type[tt.PafTaskTemplate], tt.PafTaskTemplate]
    ) -> List[Task]:
        """Returns list of all tasks for given stage."""
        return list(self.task_map[task_template.template_name].values())

    def create_workflow(self) -> workflow.Workflow:
        """Create PAF Calculator workflow.

        Each function adds its tasks to self.workflow and self.task_map.
        """
        if self.is_continuous:
            self.create_exposure_min_max_task()

        self.create_cache_tasks()
        self.create_paf_tasks()
        self.create_cleanup_tasks()

        if self.settings.rei_id in [
            constants.BMD_REI_ID,
            constants.OCCUPATIONAL_NOISE_REI_ID,
        ]:
            self.create_cause_conversion_tasks()

        if not self.settings.skip_save_results:
            self.create_save_results_tasks()

        return self.workflow

    def create_exposure_min_max_task(self) -> None:
        """Create exposure min/max task.

        Only for continuous risk factors.

        One task. No dependencies.
        """
        template = tt.ExposureMinMax(
            tool=self.tool,
            conda_env=self.conda_env,
            compute_resources={
                constants.MAX_RUNTIME: tt.hours_to_seconds(1),
                constants.MEMORY: "500M",
            },
        )

        task = template.get_task(output_dir=self.settings.output_dir)
        self.add_task(task, template)

    def create_cache_tasks(self) -> None:
        """Create exposure/exposure SD/TMREL draw caching tasks.

        One per most detailed location. No dependencies.
        """
        compute_resources = tt.cache_exp_tmrel_resources(
            n_years=len(self.settings.year_id), n_mes=len(self.exposure_me_ids)
        )

        template = tt.CacheExposureAndTmrelDraws(
            tool=self.tool, conda_env=self.conda_env, compute_resources=compute_resources
        )

        for location_id in self.location_ids:
            task = template.get_task(
                location_id=location_id, output_dir=self.settings.output_dir
            )
            self.add_task(task, template)

    def create_paf_tasks(self) -> None:
        """Create PAF calculation tasks.

        One per (most detailed) location and cause (outcome) for the risk, including distal
        causes and two-stage mediation causes. Depends on cache task for the corresponding
        location and, if risk is continuous, exp min/max task.
        """
        compute_resources = tt.paf_resources(
            is_continuous=self.is_continuous,
            n_draws=self.settings.n_draws,
            n_years=len(self.settings.year_id),
            n_mes=len(self.exposure_me_ids),
            intervention_rei_id=self.settings.intervention_rei_id,
        )
        paf_max_tasks = tt.paf_max_tasks(
            compute_resources=compute_resources, max_mem_in_tb=self._max_paf_task_memory_in_tb
        )

        template = tt.PafCalculation(
            tool=self.tool, conda_env=self.conda_env, compute_resources=compute_resources
        )

        for location_id, cause_id in itertools.product(self.location_ids, self.cause_ids):
            task = template.get_task(
                location_id=location_id,
                cause_id=cause_id,
                output_dir=self.settings.output_dir,
            )

            # Add the exposure/TMREL caching job for the location as upstream
            task.add_upstream(
                self.task_map[tt.CacheExposureAndTmrelDraws.template_name][
                    tt.CacheExposureAndTmrelDraws.task_name_template.format(
                        location_id=location_id
                    )
                ]
            )

            # Continuous risks also depend on the single exposure min/max calculation job
            if self.is_continuous:
                task.add_upstream(
                    self.task_map[tt.ExposureMinMax.template_name][
                        tt.ExposureMinMax.task_name_template
                    ]
                )

            self.add_task(task, template)

        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=tt.PafCalculation.template_name, limit=paf_max_tasks
        )

    def create_cleanup_tasks(self) -> None:
        """Create cleanup tasks.

        One task per most detailed location. Depends on PAF calculation tasks
        for the corresponding location.
        """
        template = tt.Cleanup(
            tool=self.tool,
            conda_env=self.conda_env,
            compute_resources={
                constants.MAX_RUNTIME: tt.hours_to_seconds(0.5),
                constants.MEMORY: "100M",
            },
        )

        for location_id in self.location_ids:
            task = template.get_task(
                location_id=location_id, output_dir=self.settings.output_dir
            )

            # Add all PAF calculate tasks for the location to upstream tasks
            upstream_task_names = [
                tt.PafCalculation.task_name_template.format(
                    location_id=location_id, cause_id=cause_id
                )
                for cause_id in self.cause_ids
            ]
            for task_name in upstream_task_names:
                task.add_upstream(self.task_map[tt.PafCalculation.template_name][task_name])

            self.add_task(task, template)

    def create_cause_conversion_tasks(self) -> None:
        """Create cause conversion tasks.

        Only applicable for BMD and Occupational noise.

        One task per most detailed location. Depends on PAF calculation tasks
        for the corresponding location.
        """
        template = tt.CauseConversion(
            tool=self.tool,
            conda_env=self.conda_env,
            compute_resources={
                constants.MAX_RUNTIME: tt.hours_to_seconds(24),  # TODO: no idea
                constants.MEMORY: "4G",
            },
        )

        for location_id in self.location_ids:
            task = template.get_task(
                location_id=location_id, output_dir=self.settings.output_dir
            )

            # Add all PAF calculate tasks for the location to upstream tasks
            upstream_task_names = [
                tt.PafCalculation.task_name_template.format(
                    location_id=location_id, cause_id=cause_id
                )
                for cause_id in self.cause_ids
            ]
            for task_name in upstream_task_names:
                task.add_upstream(self.task_map[tt.PafCalculation.template_name][task_name])

            self.add_task(task, template)

    def create_save_results_tasks(self) -> None:
        """Create save_results tasks(s).

        One per PAF model version ID. Two if creating an unmediated PAF model,
        one if only creating a PAF model. Depend on all PAF calculation tasks in all
        cases except if risk has cause conversion post processing (BMD, Occupational
        noise), in which case jobs depend on cause conversion tasks.
        """
        template = tt.SaveResults(
            tool=self.tool,
            conda_env=self.conda_env,
            compute_resources={
                constants.MAX_RUNTIME: tt.hours_to_seconds(72),
                constants.MEMORY: "45G",
            },
        )

        for model_version_id in self.model_version_ids:
            upstream_tasks = (
                self.get_all_tasks_from_stage(tt.PafCalculation)
                if self.settings.rei_id
                not in [constants.BMD_REI_ID, constants.OCCUPATIONAL_NOISE_REI_ID]
                else self.get_all_tasks_from_stage(tt.CauseConversion)
            )
            task = template.get_task(
                model_version_id=model_version_id,
                output_dir=self.settings.output_dir,
                upstream_tasks=upstream_tasks,
            )
            self.add_task(task, template)
