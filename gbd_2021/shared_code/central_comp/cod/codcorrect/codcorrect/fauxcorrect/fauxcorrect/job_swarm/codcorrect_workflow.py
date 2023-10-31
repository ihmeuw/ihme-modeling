"""Example CodCorrect jobmon workflow."""
from collections import defaultdict
import datetime
import itertools
import os
from typing import Dict, List

import gbd
from jobmon.client.task import Task

from fauxcorrect.job_swarm import monitor, task_templates
from fauxcorrect.parameters.machinery import CoDCorrectParameters
from fauxcorrect.utils.constants import (
    DataBases, FilePaths, Jobmon, LocationAggregation, LocationSetId, Measures,
    Scatters
)


class CodCorrectWorkflow:

    def __init__(self, parameters: CoDCorrectParameters, resume: bool):
        self.tool = task_templates.get_jobmon_tool()
        self.parameters = parameters
        self.resume = resume

        # Calculate YLLs IFF the measure is passed in
        self.calculate_ylls: bool = gbd.constants.measures.YLL in self.parameters.measure_ids

        # If resuming, retrieve the workflow args from the previous workflow
        # Not allowed if previous workflow was successful
        if resume:
            self.workflow_args = monitor.get_failed_workflow_args(self.parameters.version_id)
        else:
            self.workflow_args: str = Jobmon.WORKFLOW_ARGS.format(
                version_id=self.parameters.version_id,
                timestamp=datetime.datetime.now().isoformat()
            )

        self.task_map: Dict[str, Dict[str, Task]] = defaultdict(dict)

    def create_workflow(self) -> None:
        self.workflow = self.tool.create_workflow(
            name=Jobmon.WORKFLOW_NAME.format(version_id=self.parameters.version_id),
            workflow_args=self.workflow_args,
        )
        self.workflow.set_executor(
            project=Jobmon.PROJECT,
            stdout=os.path.join(
                self.parameters.parent_dir, FilePaths.LOG_DIR, FilePaths.STDOUT
            ),
            stderr=os.path.join(
                self.parameters.parent_dir, FilePaths.LOG_DIR, FilePaths.STDERR
            ),
        )

        self.build_all_tasks()

    def add_task(
        self,
        task_template: task_templates.CodTaskTemplate,
        task: Task
    ) -> None:
        """Adds task to self.workflow and self.task map.

        Wrapper for workflow.add_task()
        New entry added for self.task_map -> template name -> task name
        """
        self.task_map[task_template.template_name][task.name] = task
        self.workflow.add_task(task)

    def get_all_tasks_from_stage(
        self,
        task_template: task_templates.CodTaskTemplate
    ) -> List[Task]:
        """Returns list of all tasks for given stage."""
        return list(self.task_map[task_template.template_name].values())

    def add_upstream_all_tasks_from_stage(
        self,
        task: Task,
        task_template: task_templates.CodTaskTemplate
    ) -> None:
        """Adds all tasks for given stage (task template)
        as upstream for given task. Wraps task.add_upstream

        Args:
            task: a single jobmon task
            task_template: task template for a CodCorrect stage
        """
        for upstream_task in self.get_all_tasks_from_stage(task_template):
            task.add_upstream(upstream_task)

    def build_all_tasks(self) -> None:
        """Build all tasks for all stages sequentially.

        Each function adds its tasks to self.workflow and self.task_map.
        """
        self.create_monitor_task()
        self.create_cache_tasks()
        self.create_draw_validate_tasks()
        self.create_apply_correction_tasks()
        self.create_cause_aggregation_tasks()
        if self.calculate_ylls:
            self.create_calculate_ylls_tasks()
        self.create_location_aggregation_tasks()
        self.create_diagnostics_tasks()
        self.create_append_shocks_tasks()
        self.create_summarize_tasks()
        self.create_upload_tasks()

        if self.parameters.covid_cause_ids:
            self.create_covid_scalars_tasks()

        if self.parameters.scatter_version_id:
            self.create_scatter_tasks()

        if not self.parameters.test:
            self.create_post_scriptum_tasks()

    def create_monitor_task(self) -> None:
        """Creates the job for monitoring the run.

        This job sends Slack messages as steps of CodCorrect finish
        (or fail). The job should never fail itself and exits when either
        all tasks have finished or an unrecoverable error was hit.

        Has no dependancies and nothing depends on it within the jobmon
        dag.
        """
        monitor_template = task_templates.Monitor(self.tool)
        monitor_task = monitor_template.get_task(
            parent_dir=self.parameters.parent_dir,
            workflow_args=self.workflow_args
        )
        self.add_task(monitor_template, monitor_task)

    def create_cache_tasks(self) -> None:
        """Adds up to six cache tasks that must occur at the beginning of each run.

            1. envelope - draws
            2. envelope - summary
            3. population
            4. pred_ex (if calculating YLLs)
            5. regional scalars
            6. spacetime restrictions

        These jobs kick off the CoDCorrect process and do not have any
        upstream dependencies.
        """
        # MORTALITY EXTERNAL PARAMETERS excluding pred_ex (1 - 3)
        for template_class in [
            task_templates.CacheEnvelopeDraws,
            task_templates.CacheEnvelopeSummary,
            task_templates.CachePopulation
        ]:
            template = template_class(self.tool)
            task = template.get_task(version_id=self.parameters.version_id)
            self.add_task(template, task)

        # 4. pred_ex: only cache if we are calculating YLLs
        if self.calculate_ylls:
            pred_ex_template = task_templates.CachePredEx(self.tool)
            pred_ex_task = pred_ex_template.get_task(
                gbd_round_id=self.parameters.gbd_round_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id
            )
            self.add_task(pred_ex_template, pred_ex_task)

        # 5. regional scalars: for location aggregation
        regional_scalars_template = task_templates.CacheRegionalScalars(self.tool)
        regional_scalars_task = regional_scalars_template.get_task(
            decomp_step=self.parameters.decomp_step,
            gbd_round_id=self.parameters.gbd_round_id,
            parent_dir=self.parameters.parent_dir,
            version_id=self.parameters.version_id
        )
        self.add_task(regional_scalars_template, regional_scalars_task)

        # 6. spacetime restrictions (generated by us)
        st_restrictions_template = task_templates.CacheSpacetimeRestriction(self.tool)
        st_restrictions_task = st_restrictions_template.get_task(
            gbd_round_id=self.parameters.gbd_round_id,
            parent_dir=self.parameters.parent_dir,
            version_id=self.parameters.version_id
        )
        self.add_task(st_restrictions_template, st_restrictions_task)

    def create_draw_validate_tasks(self) -> None:
        """Adds tasks to validate the input draws against expected hypercube.

        Creates one job per model_version_id in our parameters of best models.
        Doesn't utilize any cached assets, so these jobs don't have any
        upstream dependencies.
        """
        validation_template = task_templates.Validate(self.tool)
        for model_version_id in self.parameters.best_model_version_ids:
            task = validation_template.get_task(
                model_version_id=model_version_id,
                version_id=self.parameters.version_id,
            )
            self.add_task(validation_template, task)

    def create_apply_correction_tasks(self) -> None:
        """Add jobs for correction step.

        These jobs depend on draw validations and caching of
        envelope draws, envelope summary and spacetime restrictions.
        """
        correction_template = task_templates.ApplyCorrection(self.tool)

        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            correction_task = correction_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                parent_dir=self.parameters.parent_dir,
                gbd_round_id=self.parameters.gbd_round_id,
                version_id=self.parameters.version_id,
                env_version_id=self.parameters.envelope_version_id,
            )

            # Correction tasks rely on all validation tasks and
            # caching of env draws, summary and spacetime restrictions
            self.add_upstream_all_tasks_from_stage(
                correction_task, task_templates.Validate
            )
            self.add_upstream_all_tasks_from_stage(
                correction_task, task_templates.CacheEnvelopeDraws
            )
            self.add_upstream_all_tasks_from_stage(
                correction_task, task_templates.CacheEnvelopeSummary
            )
            self.add_upstream_all_tasks_from_stage(
                correction_task, task_templates.CacheSpacetimeRestriction
            )

            self.add_task(correction_template, correction_task)

    def create_cause_aggregation_tasks(self) -> None:
        """Add jobs for cause aggregation.

        Each job depends on the corresponding location - sex correction job.
        """
        cause_agg_template = task_templates.CauseAggregation(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            cause_agg_task = cause_agg_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
            )

            # Add upstream correction job
            correction_task_name = task_templates.ApplyCorrection.task_name_template.format(
                location_id=location_id, sex_id=sex_id
            )
            cause_agg_task.add_upstream(
                self.task_map[task_templates.ApplyCorrection.template_name][correction_task_name]
            )

            self.add_task(cause_agg_template, cause_agg_task)

    def create_calculate_ylls_tasks(self) -> None:
        """Adds tasks to calculate YLLs from our scaled draws by location, sex,
        and year.

        Each job depends on corresponding locations - sex cause aggregation job
        and pred_ex caching.
        """
        calc_ylls_template = task_templates.CalculateYlls(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            calc_ylls_task = calc_ylls_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=self.get_all_tasks_from_stage(task_templates.CachePredEx)
            )

            # Add upstream cause agg job
            cause_agg_task_name = task_templates.CauseAggregation.task_name_template.format(
                location_id=location_id, sex_id=sex_id
            )
            calc_ylls_task.add_upstream(
                self.task_map[task_templates.CauseAggregation.template_name][cause_agg_task_name]
            )

            self.add_task(calc_ylls_template, calc_ylls_task)

    def create_location_aggregation_tasks(self) -> None:
        """Adds tasks to aggregate up the location hierarchy for each location set
        id, sex, year, and measure in our CodCorrect run.

        Note:
            * Skips aggregation for unscaled draws for YLLs.
            * Relies on base_location_set_id being first in location_set_ids

        Dependent on ALL calculate ylls tasks (if measure is YLLs)
        else on ALL cause aggregation task (measure is deaths).
        Also, depends on regional scalars caching job.

        Also, ALSO: if location set is not the base location set or SDI,
        depends on location aggregation task for a base location set.
        """
        loc_agg_template = task_templates.LocationAggregation(self.tool)
        for location_set_id, loc_agg_type, measure_id, year_id in itertools.product(
            self.parameters.location_set_ids, LocationAggregation.Type.CODCORRECT,
            self.parameters.measure_ids, self.parameters.year_ids
        ):
            if FilePaths.UNSCALED_DIR in loc_agg_type and measure_id == Measures.Ids.YLLS:
                continue

            # Replace / with _ so jobmon is happy; will deciper within job
            loc_agg_type = loc_agg_type.replace("/", "_")

            loc_agg_task = loc_agg_template.get_task(
                aggregation_type=loc_agg_type,
                location_set_id=location_set_id,
                measure_id=measure_id,
                year_id=year_id,
                decomp_step=self.parameters.decomp_step,
                gbd_round_id=self.parameters.gbd_round_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=self.get_all_tasks_from_stage(
                    task_templates.CacheRegionalScalars
                )
            )

            # If measure is YLLs, add all calc ylls tasks as upstreams
            if measure_id == Measures.Ids.YLLS:
                self.add_upstream_all_tasks_from_stage(
                    loc_agg_task, task_templates.CalculateYlls
                )
            else:
                # If measure's not YLLs (deaths), add all cause agg jobs
                self.add_upstream_all_tasks_from_stage(
                    loc_agg_task, task_templates.CauseAggregation
                )

            # If location set is not base or SDI, add base loc set agg as upstream
            if location_set_id not in [
                self.parameters.base_location_set_id, LocationSetId.SDI
            ]:
                loc_agg_task.add_upstream(
                    self.task_map[loc_agg_template.template_name][
                        loc_agg_template.task_name_template.format(
                            aggregation_type=loc_agg_type,
                            location_set_id=self.parameters.base_location_set_id,
                            measure_id=measure_id,
                            year_id=year_id
                        )
                    ]
                )

            self.add_task(loc_agg_template, loc_agg_task)

    def create_diagnostics_tasks(self) -> None:
        """Add jobs for creating before-correction diagnostic files.

        Each job is dependent in the corresponding location - sex cause aggregation job
        if it's a most detailed location, else it's dependent on all location aggregation.
        """
        diagnostics_template = task_templates.Diagnostics(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.location_ids
        ):
            diagnostics_task = diagnostics_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                version_id=self.parameters.version_id,
            )

            # Add upstream cause aggregation job or location aggregation jobs
            if location_id in self.parameters.most_detailed_location_ids:
                cause_agg_task_name = task_templates.CauseAggregation.task_name_template.format(
                    location_id=location_id, sex_id=sex_id
                )
                diagnostics_task.add_upstream(
                    self.task_map[
                        task_templates.CauseAggregation.template_name][cause_agg_task_name]
                )
            else:
                self.add_upstream_all_tasks_from_stage(
                    diagnostics_task, task_templates.LocationAggregation
                )

            self.add_task(diagnostics_template, diagnostics_task)

    def create_append_shocks_tasks(self) -> None:
        """Adds tasks to append shocks to Deaths and YLLs by location and sex.

        If location is most detailed and we calculated YLLs, dependent on
        corresponding location - sex calc yll job. If we didn't calculate ylls
        but location is still most detailed, dependent on corresponding location -
        sex cause agg job. Finally, if not most detailed location,
        dependation on all location aggregation tasks.
        """
        append_shocks_template = task_templates.AppendShocks(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.location_ids
        ):
            most_detailed_location = location_id in self.parameters.most_detailed_location_ids
            append_shocks_task = append_shocks_template.get_task(
                location_id=location_id,
                most_detailed_location=most_detailed_location,
                sex_id=sex_id,
                measure_ids=" ".join([str(x) for x in self.parameters.measure_ids]),
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
            )

            if most_detailed_location:
                if self.calculate_ylls:
                    # Add upstream calc ylls job
                    calc_ylls_task = task_templates.CalculateYlls.task_name_template.format(
                        location_id=location_id, sex_id=sex_id
                    )
                    append_shocks_task.add_upstream(
                        self.task_map[
                            task_templates.CalculateYlls.template_name][calc_ylls_task]
                    )
                else:
                    # Add upstream cause agg job
                    cause_agg_task_name = task_templates.CauseAggregation.task_name_template.format(
                        location_id=location_id, sex_id=sex_id
                    )
                    append_shocks_task.add_upstream(
                        self.task_map[task_templates.CauseAggregation.template_name][
                            cause_agg_task_name
                        ]
                    )
            else:
                # not most detailed, add all location agg jobs as upstream
                self.add_upstream_all_tasks_from_stage(
                    append_shocks_task, task_templates.LocationAggregation
                )

            self.add_task(append_shocks_template, append_shocks_task)

    def create_covid_scalars_tasks(self) -> None:
        """Adds tasks to create COVID mortality scalars.

        First, calculate COVID scalars by cause along with summaries. Then,
        compile summaries into a single file for easy review.

        Scalar calculation is dependent on corresponding location - sex append shocks task.
        Only run for most detailed locations. Scalar compilation is dependent on
        all scalar calculation tasks.
        """
        covid_scalars_template = task_templates.CovidScalarsCalculation(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            covid_scalars_task = covid_scalars_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                version_id=self.parameters.version_id,
            )

            # Add upstream append shocks job
            append_shocks_task = task_templates.AppendShocks.task_name_template.format(
                location_id=location_id, sex_id=sex_id
            )
            covid_scalars_task.add_upstream(
                self.task_map[task_templates.AppendShocks.template_name][append_shocks_task]
            )

            self.add_task(covid_scalars_template, covid_scalars_task)

        # Now create COVID scalar summaries compilation task
        compilation_template = task_templates.CovidScalarsCompilation(self.tool)
        compilation_task = compilation_template.get_task(
            version_id=self.parameters.version_id
        )

        # Add all COVID scalars tasks as upstreams
        self.add_upstream_all_tasks_from_stage(
            compilation_task, task_templates.CovidScalarsCalculation
        )

        self.add_task(compilation_template, compilation_task)


    def create_summarize_tasks(self) -> None:
        """Adds tasks to summarize draw level estimates for each location, year,
        and measure in our CodCorrect run.

        Three types of summaries:
            * gbd - always
            * cod - if cod db is input, deaths only
            * percent change - if we have year_start/end

        Dependent on the mortality input caching tasks (env draws, env summ, pop)
        as well as corresponding location (both sexes) append shocks tasks.
        """
        mortality_cache_tasks = (
            self.get_all_tasks_from_stage(task_templates.CacheEnvelopeDraws) +
            self.get_all_tasks_from_stage(task_templates.CacheEnvelopeSummary) +
            self.get_all_tasks_from_stage(task_templates.CachePopulation)
        )

        summarize_gbd_template = task_templates.SummarizeGbd(self.tool)
        summarize_cod_template = task_templates.SummarizeCod(self.tool)

        for location_id, measure_id, year_id in itertools.product(
            self.parameters.location_ids, self.parameters.measure_ids,
            self.parameters.year_ids
        ):
            summarize_gbd_task = summarize_gbd_template.get_task(
                location_id=location_id,
                measure_id=measure_id,
                year_id=year_id,
                gbd_round_id=self.parameters.gbd_round_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=mortality_cache_tasks,
            )

            # Add corresponding location append shocks task
            append_shocks_tasks = []
            for sex_id in self.parameters.sex_ids:
                append_shocks_tasks.append(
                    self.task_map[task_templates.AppendShocks.template_name][
                        task_templates.AppendShocks.task_name_template.format(
                            location_id=location_id,
                            sex_id=sex_id
                        )
                    ]
                )
            for task in append_shocks_tasks:
                summarize_gbd_task.add_upstream(task)

            self.add_task(summarize_gbd_template, summarize_gbd_task)

            # Only run COD summaries for deaths and if cod db was input
            if (
                measure_id == Measures.Ids.DEATHS and
                DataBases.COD in self.parameters.databases
            ):
                summarize_cod_task = summarize_cod_template.get_task(
                    location_id=location_id,
                    year_id=year_id,
                    gbd_round_id=self.parameters.gbd_round_id,
                    parent_dir=self.parameters.parent_dir,
                    version_id=self.parameters.version_id,
                    upstream_tasks=mortality_cache_tasks,
                )

                # Add corresponding location append shocks task
                for task in append_shocks_tasks:
                    summarize_cod_task.add_upstream(task)

                self.add_task(summarize_cod_template, summarize_cod_task)

        # If no percent change years passed in, skip those jobs
        if not self.parameters.year_start_ids:
            return

        summarize_pct_template = task_templates.SummarizePercentChange(self.tool)
        for measure_id, location_id, year_index, in itertools.product(
            self.parameters.measure_ids, self.parameters.location_ids

        ):
            summarize_pct_task = summarize_pct_template.get_task(
                location_id=location_id,
                measure_id=measure_id,
                year_start_id=self.parameters.year_start_ids[year_index],
                year_end_id=self.parameters.year_end_ids[year_index],
                gbd_round_id=self.parameters.gbd_round_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=mortality_cache_tasks,
            )

            # Add corresponding location append shocks task
            for sex_id in self.parameters.sex_ids:
                summarize_pct_task.add_upstream(
                    self.task_map[task_templates.AppendShocks.template_name][
                        task_templates.AppendShocks.task_name_template.format(
                            location_id=location_id,
                            sex_id=sex_id
                        )
                    ]
                )

            self.add_task(summarize_pct_template, summarize_pct_task)

    def create_upload_tasks(self) -> None:
        """Adds tasks to upload summaries to gbd/cod/codcorrect databases.

        Upload types single/multi refer to uploading normal vs
        percent change summaries. Only relevant to GBD db.

        For GBD upload:
            * If single year, depends on all GBD summary tasks
            * If multi year, depends on all percent change summary tasks

        For COD upload (optional):
            * Deaths only
            * Dependent on COD summary tasks

        For CODCORRECT upload (optional):
            * Deaths only
            * Uploads diagnostics for 'before' correction, depends on Diagnostics tasks
        """
        upload_template = task_templates.Upload(self.tool)

        # Determine if percent change is part of workflow
        upload_types = (
            ['single', 'multi'] if self.parameters.year_start_ids
            else ['single']
        )

        # GBD upload tasks
        for measure_id, upload_type, in itertools.product(
            self.parameters.measure_ids, upload_types
        ):
            upload_gbd_task = upload_template.get_task(
                database=DataBases.GBD,
                measure_id=measure_id,
                upload_type=upload_type,
                version_id=self.parameters.version_id,
            )

            # Depend on GBD summary tasks if single year upload
            if upload_type == "single":
                self.add_upstream_all_tasks_from_stage(
                    upload_gbd_task, task_templates.SummarizeGbd
                )
            else:
                # depend on multi year summary tasks
                self.add_upstream_all_tasks_from_stage(
                    upload_gbd_task, task_templates.SummarizePercentChange
                )

            self.add_task(upload_template, upload_gbd_task)

        # COD upload task - skip if not in inputs
        if DataBases.COD in self.parameters.databases:
            upload_cod_task = upload_template.get_task(
                database=DataBases.COD,
                measure_id=Measures.Ids.DEATHS,
                upload_type="single",
                version_id=self.parameters.version_id,
            )

            # Depend on cod summary tasks
            self.add_upstream_all_tasks_from_stage(
                upload_cod_task, task_templates.SummarizeCod
            )

            self.add_task(upload_template, upload_cod_task)

        # CODCORRECT DIAGNOSTICS upload task - skip if not in inputs
        if DataBases.CODCORRECT in self.parameters.databases:
            upload_codcorrect_task = upload_template.get_task(
                database=DataBases.CODCORRECT,
                measure_id=Measures.Ids.DEATHS,
                upload_type="single",
                version_id=self.parameters.version_id,
            )

            # Depend on diagnostic creation tasks
            self.add_upstream_all_tasks_from_stage(
                upload_codcorrect_task, task_templates.Diagnostics
            )

            self.add_task(upload_template, upload_codcorrect_task)

    def create_scatter_tasks(self) -> None:
        """Creates the scatter task.

        Scatters created have scatter_version_id CodCorrect version on the
        x axis and the current version on the y axis.

        The years produced are based on the current CodCorrect run, so
        if none of the years from the comparison CodCorrect version match
        up, there won't be any meaningful scatters created.

        Dependent on gbd upload tasks.
        """
        scatters_template = task_templates.Scatters(self.tool)
        scatters_task = scatters_template.get_task(
            x_name=Scatters.TITLE.format(version_id=self.parameters.scatter_version_id),
            y_name=Scatters.TITLE.format(version_id=self.parameters.version_id),
            x_pvid=self.parameters.scatter_process_version_id,
            y_pvid=self.parameters.gbd_process_version_id,
            x_gbd_round_id=self.parameters.scatter_gbd_round_id,
            y_gbd_round_id=self.parameters.gbd_round_id,
            x_decomp_step=self.parameters.scatter_decomp_step,
            y_decomp_step=self.parameters.decomp_step,
            year_ids=" ".join([str(year) for year in self.parameters.year_ids]),
            plotdir=Scatters.PLOT_DIR.format(version_id=self.parameters.version_id)
        )

        # Depend on gbd upload tasks
        upload_types = (
            ['single', 'multi'] if self.parameters.year_start_ids
            else ['single']
        )

        for measure_id, upload_type, in itertools.product(
            self.parameters.measure_ids, upload_types
        ):
            scatters_task.add_upstream(
                self.task_map[task_templates.Upload.template_name][
                    task_templates.Upload.task_name_template.format(
                        database=DataBases.GBD,
                        upload_type=upload_type,
                        measure_id=measure_id
                    )
                ]
            )

        self.add_task(scatters_template, scatters_task)

    def create_post_scriptum_tasks(self) -> None:
        """Task to mark the CodCorrect GBD process version active
        and to create a compare version.

        Dependent on all upload tasks as this should be the last thing that happens.
        """
        post_scriptum_template = task_templates.PostScriptum(self.tool)
        post_scriptum_task = post_scriptum_template.get_task(
            decomp_step=self.parameters.decomp_step,
            gbd_round_id=self.parameters.gbd_round_id,
            parent_dir=self.parameters.parent_dir,
            process_version_id=self.parameters.gbd_process_version_id,
        )

        # Depend on upload tasks
        self.add_upstream_all_tasks_from_stage(
            post_scriptum_task, task_templates.Upload
        )

        self.add_task(post_scriptum_template, post_scriptum_task)

    def run(self) -> str:
        """Run workflow with timeout set at two weeks."""
        timeout = 14 * 24 * 60 * 60
        return self.workflow.run(
            seconds_until_timeout=timeout,
            resume=self.resume
        ).status
