"""Example CodCorrect jobmon workflow."""

import datetime
import itertools
from collections import defaultdict
from typing import Dict, List, Type, Union

from gbd import constants as gbd_constants
from jobmon.client.task import Task

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import (
    DataBases,
    Jobmon,
    LocationAggregation,
    LocationSetId,
    Scatters,
    Status,
)
from codcorrect.lib import db
from codcorrect.lib.utils import files
from codcorrect.lib.workflow import monitor
from codcorrect.lib.workflow import task_templates as tt


class CodCorrectWorkflow:
    """Create the CoDCorrect Jobmon workflow."""

    def __init__(self, parameters: MachineParameters, resume: bool):
        self.tool = tt.get_jobmon_tool()
        self.parameters = parameters
        self.resume = resume

        # Calculate YLLs IFF the measure is passed in
        self.calculate_ylls: bool = gbd_constants.measures.YLL in self.parameters.measure_ids

        # If resuming, retrieve the workflow args from the previous workflow
        # Not allowed if previous workflow was successful
        if resume:
            self.workflow_args = monitor.get_failed_workflow_args(self.parameters.version_id)
        else:
            self.workflow_args: str = Jobmon.WORKFLOW_ARGS.format(
                version_id=self.parameters.version_id,
                timestamp=datetime.datetime.now().isoformat(),
            )

        stderr = str(self.parameters.file_system.get_file_path(files.LOGS) / files.STDERR)
        stdout = str(self.parameters.file_system.get_file_path(files.LOGS) / files.STDOUT)
        self.workflow = self.tool.create_workflow(
            name=Jobmon.WORKFLOW_NAME.format(version_id=self.parameters.version_id),
            default_cluster_name=Jobmon.SLURM,
            default_compute_resources_set={
                Jobmon.SLURM: {
                    "stderr": stderr,
                    "stdout": stdout,
                    "project": Jobmon.PROJECT,
                    "queue": Jobmon.ALL_QUEUE,
                }
            },
            workflow_args=self.workflow_args,
        )
        self.task_map: Dict[str, Dict[str, Task]] = defaultdict(dict)

    def add_task(self, task: Task, task_template: tt.CodTaskTemplate) -> None:
        """Adds task to self.workflow and self.task map.

        Wrapper for workflow.add_task()
        New entry added for self.task_map -> template name -> task name
        """
        self.task_map[task_template.template_name][task.name] = task
        self.workflow.add_task(task)

    def get_all_tasks_from_stage(
        self, task_template: Union[Type[tt.CodTaskTemplate], tt.CodTaskTemplate]
    ) -> List[Task]:
        """Returns list of all tasks for given stage."""
        return list(self.task_map[task_template.template_name].values())

    def add_upstream_all_tasks_from_stage(
        self, task: Task, task_template: Union[Type[tt.CodTaskTemplate], tt.CodTaskTemplate]
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

        if self.parameters.scatter_version_id:
            self.create_scatter_tasks()

        self.create_post_scriptum_tasks()

    def create_monitor_task(self) -> None:
        """Creates the job for monitoring the run.

        This job sends Slack messages as steps of CodCorrect finish
        (or fail). The job should never fail itself and exits when either
        all tasks have finished or an unrecoverable error was hit.

        If the CodCorrect run is being resumed, the monitor job will filter
        out the stages that finished in the last attempt so there aren't a
        bunch of extra messages sent.

        Has no dependencies and nothing depends on it within the jobmon
        dag.
        """
        monitor_template = tt.Monitor(self.tool)
        monitor_task = monitor_template.get_task(
            version_id=self.parameters.version_id,
            workflow_args=self.workflow_args,
            resume=int(self.resume),
            test=int(self.parameters.test),
        )
        self.add_task(monitor_task, monitor_template)

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
            tt.CacheEnvelopeDraws,
            tt.CacheEnvelopeSummary,
            tt.CachePopulation,
        ]:
            template = template_class(self.tool)
            task = template.get_task(version_id=self.parameters.version_id)
            self.add_task(task, template)

        # 4. pred_ex: only cache if we are calculating YLLs
        if self.calculate_ylls:
            pred_ex_template = tt.CachePredEx(self.tool)
            pred_ex_task = pred_ex_template.get_task(
                release_id=self.parameters.release_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
            )
            self.add_task(pred_ex_task, pred_ex_template)

        # 5. regional scalars: for location aggregation
        regional_scalars_template = tt.CacheRegionalScalars(self.tool)
        regional_scalars_task = regional_scalars_template.get_task(
            release_id=self.parameters.release_id,
            parent_dir=self.parameters.parent_dir,
            version_id=self.parameters.version_id,
        )
        self.add_task(regional_scalars_task, regional_scalars_template)

        # 6. spacetime restrictions
        st_restrictions_template = tt.CacheSpacetimeRestriction(self.tool)
        st_restrictions_task = st_restrictions_template.get_task(
            release_id=self.parameters.release_id,
            parent_dir=self.parameters.parent_dir,
            version_id=self.parameters.version_id,
        )
        self.add_task(st_restrictions_task, st_restrictions_template)

    def create_draw_validate_tasks(self) -> None:
        """Adds tasks to validate the input draws against expected hypercube.

        Creates one job per model_version_id in our parameters of best models.
        Doesn't utilize any cached assets, so these jobs don't have any
        upstream dependencies.
        """
        validation_template = tt.Validate(self.tool)
        for model_version_id in self.parameters.best_model_version_ids:
            task = validation_template.get_task(
                model_version_id=model_version_id, version_id=self.parameters.version_id
            )
            self.add_task(task, validation_template)

    def create_apply_correction_tasks(self) -> None:
        """Add jobs for correction step.

        These jobs depend on draw validations and caching of
        envelope draws, envelope summary and spacetime restrictions.
        """
        correction_template = tt.ApplyCorrection(self.tool)

        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            correction_task = correction_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                parent_dir=self.parameters.parent_dir,
                release_id=self.parameters.release_id,
                version_id=self.parameters.version_id,
                env_version_id=self.parameters.envelope_version_id,
            )

            # Correction tasks rely on all validation tasks and
            # caching of env draws, summary and spacetime restrictions
            self.add_upstream_all_tasks_from_stage(correction_task, tt.Validate)
            self.add_upstream_all_tasks_from_stage(correction_task, tt.CacheEnvelopeDraws)
            self.add_upstream_all_tasks_from_stage(correction_task, tt.CacheEnvelopeSummary)
            self.add_upstream_all_tasks_from_stage(
                correction_task, tt.CacheSpacetimeRestriction
            )

            self.add_task(correction_task, correction_template)

    def create_cause_aggregation_tasks(self) -> None:
        """Add jobs for cause aggregation.

        Each job depends on the corresponding location - sex correction job.
        """
        cause_agg_template = tt.CauseAggregation(self.tool)
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
            correction_task_name = tt.ApplyCorrection.task_name_template.format(
                location_id=location_id, sex_id=sex_id
            )
            cause_agg_task.add_upstream(
                self.task_map[tt.ApplyCorrection.template_name][correction_task_name]
            )

            self.add_task(cause_agg_task, cause_agg_template)

    def create_calculate_ylls_tasks(self) -> None:
        """Adds tasks to calculate YLLs from our scaled draws by location, sex,
        and year.

        Each job depends on corresponding locations - sex cause aggregation job
        and pred_ex caching.
        """
        calc_ylls_template = tt.CalculateYlls(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.most_detailed_location_ids
        ):
            calc_ylls_task = calc_ylls_template.get_task(
                location_id=location_id,
                sex_id=sex_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=self.get_all_tasks_from_stage(tt.CachePredEx),
            )

            # Add upstream cause agg job
            cause_agg_task_name = tt.CauseAggregation.task_name_template.format(
                location_id=location_id, sex_id=sex_id
            )
            calc_ylls_task.add_upstream(
                self.task_map[tt.CauseAggregation.template_name][cause_agg_task_name]
            )

            self.add_task(calc_ylls_task, calc_ylls_template)

    def create_location_aggregation_tasks(self) -> None:
        """Adds tasks to aggregate up the location hierarchy for each location set
        id, sex, year, and measure in our CodCorrect run.

        Note:
            * Skips aggregation for unscaled draws for YLLs.
            * Relies on base_location_set_id being first in location_set_ids

        Dependent on ALL calculate ylls tasks (if measure is YLLs)
        else on ALL cause aggregation task (measure is deaths).
        Also, depends on regional scalars caching job.
        """
        loc_agg_template = tt.LocationAggregation(self.tool)
        for location_set_id, loc_agg_type, measure_id, year_id in itertools.product(
            self.parameters.location_set_ids,
            LocationAggregation.Type.CODCORRECT,
            self.parameters.measure_ids,
            self.parameters.year_ids,
        ):
            if files.UNSCALED in loc_agg_type and measure_id == gbd_constants.measures.YLL:
                continue

            loc_agg_task = loc_agg_template.get_task(
                aggregation_type=loc_agg_type,
                location_set_id=location_set_id,
                measure_id=measure_id,
                year_id=year_id,
                release_id=self.parameters.release_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=self.get_all_tasks_from_stage(tt.CacheRegionalScalars),
            )

            # If measure is YLLs, add all calc ylls tasks as upstreams
            if measure_id == gbd_constants.measures.YLL:
                self.add_upstream_all_tasks_from_stage(loc_agg_task, tt.CalculateYlls)
            else:
                # If measure's not YLLs (deaths), add all cause agg jobs
                self.add_upstream_all_tasks_from_stage(loc_agg_task, tt.CauseAggregation)

            # If location set is not base or SDI, add base loc set agg as upstream
            if location_set_id not in [
                self.parameters.base_location_set_id,
                LocationSetId.SDI,
            ]:
                loc_agg_task.add_upstream(
                    self.task_map[loc_agg_template.template_name][
                        loc_agg_template.task_name_template.format(
                            aggregation_type=loc_agg_type,
                            location_set_id=self.parameters.base_location_set_id,
                            measure_id=measure_id,
                            year_id=year_id,
                        )
                    ]
                )

            self.add_task(loc_agg_task, loc_agg_template)

    def create_diagnostics_tasks(self) -> None:
        """Add jobs for creating before-correction diagnostic files.

        Each job is dependent in the corresponding location - sex cause aggregation job
        if it's a most detailed location, else it's dependent on all location aggregation.
        """
        diagnostics_template = tt.Diagnostics(self.tool)
        for sex_id, location_id in itertools.product(
            self.parameters.sex_ids, self.parameters.location_ids
        ):
            diagnostics_task = diagnostics_template.get_task(
                location_id=location_id, sex_id=sex_id, version_id=self.parameters.version_id
            )

            # Add upstream cause aggregation job or location aggregation jobs
            if location_id in self.parameters.most_detailed_location_ids:
                cause_agg_task_name = tt.CauseAggregation.task_name_template.format(
                    location_id=location_id, sex_id=sex_id
                )
                diagnostics_task.add_upstream(
                    self.task_map[tt.CauseAggregation.template_name][cause_agg_task_name]
                )
            else:
                self.add_upstream_all_tasks_from_stage(
                    diagnostics_task, tt.LocationAggregation
                )

            self.add_task(diagnostics_task, diagnostics_template)

    def create_append_shocks_tasks(self) -> None:
        """Adds tasks to append shocks to Deaths and YLLs by location and sex.

        If location is most detailed and we calculated YLLs, dependent on
        corresponding location - sex calc yll job. If we didn't calculate ylls
        but location is still most detailed, dependent on corresponding location -
        sex cause agg job. Finally, if not most detailed location,
        dependent on all location aggregation tasks.
        """
        append_shocks_template = tt.AppendShocks(self.tool)
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
                    calc_ylls_task = tt.CalculateYlls.task_name_template.format(
                        location_id=location_id, sex_id=sex_id
                    )
                    append_shocks_task.add_upstream(
                        self.task_map[tt.CalculateYlls.template_name][calc_ylls_task]
                    )
                else:
                    # Add upstream cause agg job
                    cause_agg_task_name = tt.CauseAggregation.task_name_template.format(
                        location_id=location_id, sex_id=sex_id
                    )
                    append_shocks_task.add_upstream(
                        self.task_map[tt.CauseAggregation.template_name][cause_agg_task_name]
                    )
            else:
                # not most detailed, add all location agg jobs as upstream
                self.add_upstream_all_tasks_from_stage(
                    append_shocks_task, tt.LocationAggregation
                )

            self.add_task(append_shocks_task, append_shocks_template)

    def create_summarize_tasks(self) -> None:
        """Adds tasks to summarize draw level estimates for each location, year,
        and measure in our CodCorrect run.

        Three types of summaries:
            * gbd - always
            * cod - if cod db is input, deaths only
            * percent change - if we have year_start/end

        Dependent on the mortality input caching tasks (env draws, env summary, pop)
        as well as corresponding location (both sexes) append shocks tasks.
        """
        mortality_cache_tasks = (
            self.get_all_tasks_from_stage(tt.CacheEnvelopeDraws)
            + self.get_all_tasks_from_stage(tt.CacheEnvelopeSummary)
            + self.get_all_tasks_from_stage(tt.CachePopulation)
        )

        summarize_gbd_template = tt.SummarizeGbd(self.tool)
        summarize_cod_template = tt.SummarizeCod(self.tool)

        for location_id, measure_id, year_id in itertools.product(
            self.parameters.location_ids,
            self.parameters.measure_ids,
            self.parameters.year_ids,
        ):
            summarize_gbd_task = summarize_gbd_template.get_task(
                location_id=location_id,
                measure_id=measure_id,
                year_id=year_id,
                release_id=self.parameters.release_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=mortality_cache_tasks,
            )

            # Add corresponding location append shocks task
            append_shocks_tasks = []
            for sex_id in self.parameters.sex_ids:
                append_shocks_tasks.append(
                    self.task_map[tt.AppendShocks.template_name][
                        tt.AppendShocks.task_name_template.format(
                            location_id=location_id, sex_id=sex_id
                        )
                    ]
                )
            for task in append_shocks_tasks:
                summarize_gbd_task.add_upstream(task)

            self.add_task(summarize_gbd_task, summarize_gbd_template)

            # Only run COD summaries for deaths and if cod db was input
            if (
                measure_id == gbd_constants.measures.DEATH
                and DataBases.COD in self.parameters.databases
            ):
                summarize_cod_task = summarize_cod_template.get_task(
                    location_id=location_id,
                    year_id=year_id,
                    release_id=self.parameters.release_id,
                    parent_dir=self.parameters.parent_dir,
                    version_id=self.parameters.version_id,
                    upstream_tasks=mortality_cache_tasks,
                )

                # Add corresponding location append shocks task
                for task in append_shocks_tasks:
                    summarize_cod_task.add_upstream(task)

                self.add_task(summarize_cod_task, summarize_cod_template)

        # If no percent change years passed in, skip those jobs
        if not self.parameters.year_start_ids:
            return

        summarize_pct_template = tt.SummarizePercentChange(self.tool)
        for measure_id, location_id, year_index in itertools.product(
            self.parameters.measure_ids,
            self.parameters.location_ids,
            [index for index in range(len(self.parameters.year_start_ids))],
        ):
            summarize_pct_task = summarize_pct_template.get_task(
                location_id=location_id,
                measure_id=measure_id,
                year_start_id=self.parameters.year_start_ids[year_index],
                year_end_id=self.parameters.year_end_ids[year_index],
                release_id=self.parameters.release_id,
                parent_dir=self.parameters.parent_dir,
                version_id=self.parameters.version_id,
                upstream_tasks=mortality_cache_tasks,
            )

            # Add corresponding location append shocks task
            for sex_id in self.parameters.sex_ids:
                summarize_pct_task.add_upstream(
                    self.task_map[tt.AppendShocks.template_name][
                        tt.AppendShocks.task_name_template.format(
                            location_id=location_id, sex_id=sex_id
                        )
                    ]
                )

            self.add_task(summarize_pct_task, summarize_pct_template)

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
        upload_template = tt.Upload(self.tool)

        # Determine if percent change is part of workflow
        upload_types = ["single", "multi"] if self.parameters.year_start_ids else ["single"]

        # GBD upload tasks
        for measure_id, upload_type in itertools.product(
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
                self.add_upstream_all_tasks_from_stage(upload_gbd_task, tt.SummarizeGbd)
            else:
                # depend on multi year summary tasks
                self.add_upstream_all_tasks_from_stage(
                    upload_gbd_task, tt.SummarizePercentChange
                )

            self.add_task(upload_gbd_task, upload_template)

        # COD upload task - skip if not in inputs
        if DataBases.COD in self.parameters.databases:
            upload_cod_task = upload_template.get_task(
                database=DataBases.COD,
                measure_id=gbd_constants.measures.DEATH,
                upload_type="single",
                version_id=self.parameters.version_id,
            )

            # Depend on cod summary tasks
            self.add_upstream_all_tasks_from_stage(upload_cod_task, tt.SummarizeCod)

            self.add_task(upload_cod_task, upload_template)

        # CODCORRECT DIAGNOSTICS upload task - skip if not in inputs
        if DataBases.CODCORRECT in self.parameters.databases:
            upload_codcorrect_task = upload_template.get_task(
                database=DataBases.CODCORRECT,
                measure_id=gbd_constants.measures.DEATH,
                upload_type="single",
                version_id=self.parameters.version_id,
            )

            # Depend on diagnostic creation tasks
            self.add_upstream_all_tasks_from_stage(upload_codcorrect_task, tt.Diagnostics)

            self.add_task(upload_codcorrect_task, upload_template)

    def create_scatter_tasks(self) -> None:
        """Creates the scatter task.

        Scatters created have scatter_version_id CodCorrect version on the
        x axis and the current version on the y axis.

        The years produced are based on the current CodCorrect run, so
        if none of the years from the comparison CodCorrect version match
        up, there won't be any meaningful scatters created.

        Dependent on gbd upload tasks.
        """
        scatters_template = tt.Scatters(self.tool)
        scatters_task = scatters_template.get_task(
            x_name=Scatters.TITLE.format(version_id=self.parameters.scatter_version_id),
            y_name=Scatters.TITLE.format(version_id=self.parameters.version_id),
            x_pvid=self.parameters.scatter_process_version_id,
            y_pvid=self.parameters.gbd_process_version_id,
            x_release_id=self.parameters.scatter_release_id,
            y_release_id=self.parameters.release_id,
            year_ids=" ".join([str(year) for year in self.parameters.year_ids]),
            plotdir=Scatters.PLOT_DIR.format(version_id=self.parameters.version_id),
        )

        # Depend on gbd upload tasks
        upload_types = ["single", "multi"] if self.parameters.year_start_ids else ["single"]

        for measure_id, upload_type in itertools.product(
            self.parameters.measure_ids, upload_types
        ):
            scatters_task.add_upstream(
                self.task_map[tt.Upload.template_name][
                    tt.Upload.task_name_template.format(
                        database=DataBases.GBD, upload_type=upload_type, measure_id=measure_id
                    )
                ]
            )

        self.add_task(scatters_task, scatters_template)

    def create_post_scriptum_tasks(self) -> None:
        """Task to mark the CodCorrect GBD process version active
        and to create a compare version.

        Dependent on all upload tasks as this should be the last thing that happens.
        """
        post_scriptum_template = tt.PostScriptum(self.tool)
        post_scriptum_task = post_scriptum_template.get_task(
            version_id=self.parameters.version_id
        )

        # Depend on upload tasks
        self.add_upstream_all_tasks_from_stage(post_scriptum_task, tt.Upload)

        self.add_task(post_scriptum_task, post_scriptum_template)

    def run(self) -> str:
        """Run workflow with timeout set at two weeks and set CodCorrect as running."""
        db.update_status(self.parameters.cod_output_version_id, Status.RUNNING)
        timeout = 14 * 24 * 60 * 60

        return self.workflow.run(seconds_until_timeout=timeout, resume=self.resume)
