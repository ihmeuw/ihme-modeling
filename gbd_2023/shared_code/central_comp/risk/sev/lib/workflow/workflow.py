"""Jobmon workflow."""

import datetime
from collections import defaultdict
from typing import Dict, List, Type, Union

import ihme_cc_risk_utils
from jobmon.client.task import Task

from ihme_cc_sev_calculator.lib import constants, parameters, risks
from ihme_cc_sev_calculator.lib.workflow import task_templates as tt


class Workflow:
    """Creates a workflow for a SEV Calculator run."""

    def __init__(self, params: parameters.Parameters):
        self.tool = tt.get_jobmon_tool()
        self.params = params

        # Calculate SEVs only if the measure is passed in
        self.calculate_sevs = constants.SEV in self.params.measures

        # ETL temperature SEVs if those REIs are included
        self.temperature_reis_included = len(self.params.temperature_rei_ids) > 0

        # Edensity REIs require two additional steps to cache exposure draws and then
        # use R code to calculate risk prevalence
        self.included_edensity_rei_ids = [
            rei_id for rei_id in constants.EDENSITY_REI_IDS if rei_id in params.rei_ids
        ]

        # If resuming, retrieve the workflow args from the previous workflow.
        # Not allowed if previous workflow was successful
        if self.params.resume:
            self.workflow_args = ihme_cc_risk_utils.get_failed_workflow_args(
                constants.WORKFLOW_NAME.format(version_id=self.params.version_id)
            )
        else:
            self.workflow_args: str = constants.WORKFLOW_ARGS.format(
                version_id=self.params.version_id,
                timestamp=datetime.datetime.now().isoformat(),
            )

        self.workflow = self.tool.create_workflow(
            name=constants.WORKFLOW_NAME.format(version_id=self.params.version_id),
            default_cluster_name=constants.SLURM,
            default_compute_resources_set={
                constants.SLURM: {
                    "stderr": ,
                    "stdout": ,
                    "project": constants.PROJECT,
                    "queue": constants.ALL_QUEUE,
                }
            },
            workflow_args=self.workflow_args,
        )
        self.task_map: Dict[str, Dict[str, Task]] = defaultdict(dict)

    def add_task(self, task: Task, task_template: tt.SevTaskTemplate) -> None:
        """Adds task to self.workflow and self.task_map.

        Wrapper for workflow.add_task(). New entry added for
        self.task_map -> template name -> task name
        """
        self.task_map[task_template.template_name][task.name] = task
        self.workflow.add_task(task)

    def get_all_tasks_from_stage(
        self, task_template: Union[Type[tt.SevTaskTemplate], tt.SevTaskTemplate]
    ) -> List[Task]:
        """Returns list of all tasks for given stage."""
        return list(self.task_map[task_template.template_name].values())

    def build_all_tasks(self) -> None:
        """Build all tasks for all stages sequentially.

        Each function adds its tasks to self.workflow and self.task_map.
        """
        self.create_rr_max_tasks()
        self.create_rr_max_aggregation_tasks()
        self.create_rr_max_summary_tasks()

        if self.calculate_sevs:
            if self.temperature_reis_included:
                self.create_temperature_etl_tasks()

            if self.included_edensity_rei_ids:
                self.create_cache_exposure_tasks()
                self.create_risk_prevalence_tasks()

            self.create_sev_tasks()
            self.create_sev_location_aggregation_tasks()
            self.create_sev_summary_tasks()

        self.create_upload_tasks()
        if not self.params.test:
            self.create_compare_version_task()

    def bind(self) -> int:
        """Bind workflow if it hasn't been bound already.

        Returns the workflow ID.
        """
        self.workflow.bind()
        return self.workflow.workflow_id

    def run(self) -> str:
        """Run workflow."""
        timeout = 48 * 60 * 60  # 48 hours
        return self.workflow.run(seconds_until_timeout=timeout, resume=self.params.resume)

    def create_rr_max_tasks(self) -> None:
        """Create RR max calculation tasks.

        One task for each most detailed REI ID.

        No dependencies.

        TODO: some risks will take much longer depending on:
            1) number of causes in RR model
            2) If RRs are location and/or year specific (secondhand smoke in particular)

        Parallizing by cause would help for 1). 2) would need a different solution, but as
        these risks (esp secondhand smoke which has location and year-specific RRs) are
        the bottleneck for the whole SEV pipeline, consider addressing.
        """
        template = tt.RRMax(self.tool)

        for rei_id in self.params.rei_ids:
            task = template.get_task(rei_id=rei_id, version_id=self.params.version_id)
            self.add_task(task, template)

    def create_rr_max_aggregation_tasks(self) -> None:
        """Create RR max aggregation tasks.

        One task for each aggregate REI ID.

        Depends on RR max calculation for each child REI.
        """
        template = tt.AggregateRRMax(self.tool)
        rei_metadata = self.params.read_rei_metadata(constants.AGGREGATION_REI_SET_ID)

        for aggregate_rei_id in self.params.aggregate_rei_ids:
            task = template.get_task(
                aggregate_rei_id=aggregate_rei_id, version_id=self.params.version_id
            )

            # Add all RR max tasks for most detailed child REIs to upstream jobs.
            # Omits REIs that we don't have RR for and assumes the rest are included
            child_rei_ids = risks.get_child_rei_ids(aggregate_rei_id, rei_metadata)
            for rei_id in child_rei_ids:
                rr_max_task_name = tt.RRMax.task_name_template.format(rei_id=rei_id)
                task.add_upstream(self.task_map[tt.RRMax.template_name][rr_max_task_name])

            self.add_task(task, template)

    def create_rr_max_summary_tasks(self) -> None:
        """Create RRmax summary tasks.

        One task for each REI ID, including most detailed and aggregate REIs.

        Depends on RR max calculation tasks, including aggregate calculations.
        """
        template = tt.SummarizeRRMax(self.tool)

        for rei_id in self.params.rei_ids:
            rr_max_task_name = tt.RRMax.task_name_template.format(rei_id=rei_id)
            task = template.get_task(
                rei_id=rei_id,
                version_id=self.params.version_id,
                upstream_tasks=[self.task_map[tt.RRMax.template_name][rr_max_task_name]],
            )
            self.add_task(task, template)

        for aggregate_rei_id in self.params.aggregate_rei_ids:
            rr_max_task_name = tt.AggregateRRMax.task_name_template.format(
                aggregate_rei_id=aggregate_rei_id
            )
            task = template.get_task(
                rei_id=aggregate_rei_id,
                version_id=self.params.version_id,
                upstream_tasks=[
                    self.task_map[tt.AggregateRRMax.template_name][rr_max_task_name]
                ],
            )
            self.add_task(task, template)

    def create_temperature_etl_tasks(self) -> None:
        """Create temperature ETL tasks.

        One task for each most detailed location.

        No dependencies.
        """
        template = tt.TemperatureEtl(self.tool)

        for location_id in self.params.most_detailed_location_ids:
            task = template.get_task(
                location_id=location_id, version_id=self.params.version_id
            )
            self.add_task(task, template)

    def create_cache_exposure_tasks(self) -> None:
        """Create cache exposure tasks for edensity risks.

        One task for each edensity risk (FPG, SBP) included and most detailed location.

        No dependencies.
        """
        template = tt.CacheExposure(self.tool)

        for rei_id in self.included_edensity_rei_ids:
            for location_id in self.params.most_detailed_location_ids:
                task = template.get_task(
                    rei_id=rei_id, location_id=location_id, version_id=self.params.version_id
                )
                self.add_task(task, template)

    def create_risk_prevalence_tasks(self) -> None:
        """Create risk prevalence calculation tasks for edensity risks.

        One task for each edensity risk (FPG, SBP) included and most detailed location.

        Depends on corresponding cache exposure task for the risk and location.
        """
        template = tt.RiskPrevalence(self.tool)

        for rei_id in self.included_edensity_rei_ids:
            for location_id in self.params.most_detailed_location_ids:
                cache_exposure_task = tt.CacheExposure.task_name_template.format(
                    rei_id=rei_id, location_id=location_id
                )
                task = template.get_task(
                    rei_id=rei_id,
                    location_id=location_id,
                    version_id=self.params.version_id,
                    upstream_tasks=[
                        self.task_map[tt.CacheExposure.template_name][cache_exposure_task]
                    ],
                )
                self.add_task(task, template)

    def create_sev_tasks(self) -> None:
        """Create SEV calculation tasks.

        One task for each risk - most detailed location.

        For most detailed risks, depends on corresponding RRmax calculation tasks.
        If the risk is one of the edensity risks (FPG, SBP), also depends on the corresponding
        risk prevalence task. For aggregate risks, depends on RR max aggregation.
        """
        template = tt.Sevs(self.tool)

        for rei_id in self.params.rei_ids:
            # All most detailed risks depend on RRmax task
            rr_max_task_name = tt.RRMax.task_name_template.format(rei_id=rei_id)
            rr_max_task = self.task_map[tt.RRMax.template_name][rr_max_task_name]

            for location_id in self.params.most_detailed_location_ids:
                # edensity risks also depend on risk prevalence task
                if rei_id in constants.EDENSITY_REI_IDS:
                    risk_prevalence_task = tt.RiskPrevalence.task_name_template.format(
                        rei_id=rei_id, location_id=location_id
                    )
                    upstream_tasks = [rr_max_task] + [
                        self.task_map[tt.RiskPrevalence.template_name][risk_prevalence_task]
                    ]

                else:
                    upstream_tasks = [rr_max_task]

                task = template.get_task(
                    rei_id=rei_id,
                    location_id=location_id,
                    version_id=self.params.version_id,
                    upstream_tasks=upstream_tasks,
                )
                self.add_task(task, template)

        for aggregate_rei_id in self.params.aggregate_rei_ids:
            rr_max_task_name = tt.AggregateRRMax.task_name_template.format(
                aggregate_rei_id=aggregate_rei_id
            )
            for location_id in self.params.most_detailed_location_ids:
                task = template.get_task(
                    rei_id=aggregate_rei_id,
                    location_id=location_id,
                    version_id=self.params.version_id,
                    upstream_tasks=[
                        self.task_map[tt.AggregateRRMax.template_name][rr_max_task_name]
                    ],
                )
                self.add_task(task, template)

    def create_sev_location_aggregation_tasks(self) -> None:
        """Create SEV location aggregation tasks.

        One task for each REI ID, including aggregates and temperature REIs.

        Depends on temperature ETL tasks if a temperature risk. Otherwise,
        depends on corresponding SEV calculation tasks for the REI.
        """
        template = tt.LocationAggregation(self.tool)

        for rei_id in self.params.all_rei_ids:
            task = template.get_task(rei_id=rei_id, version_id=self.params.version_id)

            # Add dependencies
            if rei_id in constants.TEMPERATURE_REI_IDS:
                for upstream_task in self.get_all_tasks_from_stage(tt.TemperatureEtl):
                    task.add_upstream(upstream_task)
            else:
                # Otherwise, add SEV tasks for given REI for most detailed locations
                for location_id in self.params.most_detailed_location_ids:
                    sev_task_name = tt.Sevs.task_name_template.format(
                        rei_id=rei_id, location_id=location_id
                    )
                    task.add_upstream(self.task_map[tt.Sevs.template_name][sev_task_name])

            self.add_task(task, template)

    def create_sev_summary_tasks(self) -> None:
        """Creates SEV summary tasks.

        One task per location (including aggregate locations).

        For most detailed locations, depends on all SEV calculation tasks (including
        temperature risks ETLs) for the location. For aggregate locations, depends on all
        location aggregation tasks.
        """
        template = tt.SummarizeSevs(self.tool)

        for location_id in self.params.all_location_ids:
            task = template.get_task(
                location_id=location_id, version_id=self.params.version_id
            )

            # Add dependencies
            if location_id in self.params.most_detailed_location_ids:
                # If location is most detailed, depend on all SEV calc tasks (including temp)
                for rei_id in self.params.rei_ids + self.params.aggregate_rei_ids:
                    sev_task_name = tt.Sevs.task_name_template.format(
                        rei_id=rei_id, location_id=location_id
                    )
                    task.add_upstream(self.task_map[tt.Sevs.template_name][sev_task_name])

                if tt.TemperatureEtl.template_name in self.task_map:
                    temperature_etl_task_name = tt.TemperatureEtl.task_name_template.format(
                        location_id=location_id
                    )
                    task.add_upstream(
                        self.task_map[tt.TemperatureEtl.template_name][
                            temperature_etl_task_name
                        ]
                    )
            else:
                # If location is an aggregate, wait for all location aggregation tasks
                for upstream_task in self.get_all_tasks_from_stage(tt.LocationAggregation):
                    task.add_upstream(upstream_task)

            self.add_task(task, template)

    def create_upload_tasks(self) -> None:
        """Create upload tasks for RRmax and SEV summaries.

        Two upload jobs: one for RRmax summaries and another for SEV summaries.
        The latter is omitted if SEVs are not requested.

        Both depend on all respective summary jobs.
        """
        template = tt.Upload(self.tool)

        rr_max_task = template.get_task(
            measure=constants.RR_MAX,
            version_id=self.params.version_id,
            upstream_tasks=self.get_all_tasks_from_stage(tt.SummarizeRRMax),
        )
        self.add_task(rr_max_task, template)

        if self.calculate_sevs:
            sevs_task = template.get_task(
                measure=constants.SEV,
                version_id=self.params.version_id,
                upstream_tasks=self.get_all_tasks_from_stage(tt.SummarizeSevs),
            )
            self.add_task(sevs_task, template)

    def create_compare_version_task(self) -> None:
        """Create the compare version task.

        One task.

        Depends on all upload jobs.
        """
        template = tt.CompareVersion(self.tool)

        task = template.get_task(
            version_id=self.params.version_id,
            upstream_tasks=self.get_all_tasks_from_stage(tt.Upload),
        )
        self.add_task(task, template)
