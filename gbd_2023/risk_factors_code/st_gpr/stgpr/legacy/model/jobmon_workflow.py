import itertools
from collections import defaultdict
from typing import Dict, List, Type

import numpy as np

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from jobmon.client.task import Task

from stgpr.legacy.model import task_templates as tt
from stgpr.lib import constants, utils


def intersection(a, b):
    return list(set(a) & set(b))


class STGPRJobSwarm:
    def __init__(
        self,
        run_id,
        run_type,
        holdouts,
        draws,
        nparallel,
        n_parameter_sets,
        cluster_project,
        error_log_path,
        output_log_path,
        location_set_id,
        custom_stage1,
        rake_logit,
        code_version,
        modelable_entity_id,
        prediction_location_set_version,
        standard_location_set_version,
        random_seed,
    ):

        self.run_id = run_id
        self.run_type = run_type
        self.holdouts = holdouts
        self.draws = draws
        self.prediction_location_set_version = prediction_location_set_version
        self.standard_location_set_version = standard_location_set_version
        self.nparallel = nparallel
        self.n_parameter_sets = n_parameter_sets
        self.cluster_project = cluster_project
        self.location_set_id = location_set_id
        self.custom_stage1 = custom_stage1
        self.rake_logit = rake_logit
        self.code_version = code_version
        self.random_seed = random_seed
        self.is_diet_model = modelable_entity_id in [
            2430,
            2442,
            2431,
            2434,
            2433,
            2437,
            2435,
            2428,
            2429,
            2436,
            2427,
            2432,
            2440,
            9804,
            2441,
            2544,
            23766,
            2544,
            2438,
            23604,
            23683,
            23766,
            27470,
            27458,
            28503,
            28634,
        ]

        # create jobmon tool, workflow, task map
        self.tool = tt.get_jobmon_tool()
        settings = stgpr_schema.get_settings()
        workflow_args = (
            constants.workflow.WORKFLOW_ARGS_TEMPLATE.format(run_id=self.run_id)
            if not settings.is_test
            else constants.workflow.WORKFLOW_ARGS_TEMPLATE_TEST.format(run_id=self.run_id)
        )
        self.workflow = self.tool.create_workflow(
            name=constants.workflow.WORKFLOW_NAME_TEMPLATE.format(run_id=self.run_id),
            default_cluster_name=constants.workflow.SLURM,
            default_compute_resources_set={
                constants.workflow.SLURM: {
                    "stderr": error_log_path,
                    "stdout": output_log_path,
                    "project": self.cluster_project,
                    "queue": constants.workflow.ALL_QUEUE,
                }
            },
            workflow_args=workflow_args,
        )
        self.task_map: Dict[str, Dict[str, Task]] = defaultdict(dict)

        # Set up model iteration - parameter set mapping (1:1) for uploads
        # List of (model iteration id, parameter set id) tuples
        with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
            model_iteration_ids = db_stgpr.get_model_iterations(self.run_id, scoped_session)
        self.model_iteration_map = [
            (model_iteration_ids[i], i) for i in range(0, self.n_parameter_sets)
        ]
        self.param_groups = utils.get_parameter_groups(
            run_type=self.run_type,
            n_parameter_sets=self.n_parameter_sets,
            holdouts=self.holdouts,
        )

    def add_task(self, task: Task, task_template: tt.StgprTaskTemplate) -> None:
        """Adds task to self.workflow and self.task map.

        Wrapper for constants.workflow.add_task()
        New entry added for self.task_map -> template name -> task name

        Args:
            task: a single jobmon task
            task_template: task template for a ST-GPR stage
        """
        self.task_map[task_template.template_name][task.name] = task
        self.workflow.add_task(task)

    def get_all_tasks_from_stage(
        self, task_template: Type[tt.StgprTaskTemplate]
    ) -> List[Task]:
        """Returns list of all tasks for given stage.

        Returns an empty list if the template has no jobs.
        """
        if task_template.template_name not in self.task_map:
            return []

        return list(self.task_map[task_template.template_name].values())

    def add_upstream_all_tasks_from_stage(
        self, task: Task, task_template: Type[tt.StgprTaskTemplate]
    ) -> None:
        """Adds all tasks for given stage (task template)
        as upstream for given task. Wraps task.add_upstream

        Args:
            task: a single jobmon task
            task_template: task template for a CodCorrect stage
        """
        for upstream_task in self.get_all_tasks_from_stage(task_template):
            task.add_upstream(upstream_task)

    def prep_parallelization_groups(self):
        """Parallize by splitting locations
        to be modelled into *nparallel* groups.
        This function grabs the location hierarchy,
        identifies needed locations, and assigns
        each one to a parallelization group."""
        settings = stgpr_schema.get_settings()
        with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
            locs = stgpr_helpers.get_stgpr_locations(
                self.prediction_location_set_version,
                self.standard_location_set_version,
                scoped_session,
            )
        self.locs = locs.sort_values(
            by=["level_{}".format(constants.location.NATIONAL_LEVEL)]
        )
        self.parallel_groups = np.array_split(
            self.locs.loc[self.locs.level >= constants.location.NATIONAL_LEVEL][
                "location_id"
            ].values,
            self.nparallel,
        )

        # prep raking upstreams and submission locations
        lvl = "level_{}".format(constants.location.NATIONAL_LEVEL)
        self.subnat_locations = (
            locs.loc[locs.level > constants.location.NATIONAL_LEVEL, lvl].unique().astype(int)
        )

    def assign_rake_runtimes(self):
        """
        Rake jobs have immensely different times based
        almost exclusively on the number of subnationals nested
        within the national location. Assign memory based on the
        number of subnationals for each national location with
        subnationals, using (slightly modified)
        intercept and beta values from a  super simple
        linear regression,

        'memory ~ n_subnats'

        for one very data-dense model
        (ie a good upper bound for all st-gpr models)

        Memory Intercept: .75
        Memory Beta_N_subnats: .5 <- bumped to 0.7 for CCMHD-21669

        (More conservative for runtime because not as
        wasteful to go high)
        Runtime Intercept: 5 (min), so 300 sec
        Runtime Beta_N_subnats: .35 (min), so 21 sec
        """

        natcol = f"level_{constants.location.NATIONAL_LEVEL}"
        self.locs["subnat"] = (self.locs["level"] > constants.location.NATIONAL_LEVEL).astype(
            int
        )
        n_subnats = self.locs.groupby(natcol)["subnat"].sum()
        n_subnats = n_subnats.loc[n_subnats > 0].reset_index(name="N")
        n_subnats[natcol] = n_subnats[natcol].astype(int)

        # assign memory
        n_subnats["mem"] = 0.75 + 0.7 * n_subnats["N"]
        n_subnats["runtime"] = 300 + 21 * n_subnats["N"]
        n_subnats = n_subnats.rename(columns={natcol: "location"})
        self.rake_memory_df = n_subnats.copy()

    def create_prep_job(self) -> None:
        """Creates prep job.

        First job to run, no dependencies.
        """
        prep_template = tt.Prep(self.tool)
        prep_task = prep_template.get_task(run_id=self.run_id)
        self.add_task(prep_task, prep_template)

    def create_stage1_jobs(self) -> None:
        """Creates stage 1 jobs (in R).

        Adds one stage 1 job for each holdout (ko). Runs regardless of if there's a
        custom stage 1 or not. Depends on the prep job.
        """
        stage1_template = tt.Stage1(self.tool)
        settings = stgpr_schema.get_settings()
        model_root = settings.path_to_model_code / "src/stgpr/legacy/model"

        settings = stgpr_schema.get_settings()
        for holdout in list(range(0, self.holdouts + 1)):
            stage1_task = stage1_template.get_task(
                holdout=holdout,
                output_path=settings.output_root_format.format(stgpr_version_id=self.run_id),
                model_root=model_root,
                run_id=self.run_id,  # for task name
                upstream_tasks=self.get_all_tasks_from_stage(tt.Prep),
            )

            self.add_task(stage1_task, stage1_template)

    def create_stage1_upload_job(self) -> None:
        """Creates the stage 1 upload job."""
        stage1_upload_template = tt.Stage1Upload(self.tool)
        # update resources if using multiple parameter sets
        if self.run_type in ["in_sample_selection", "oos_selection", "dd"]:
            stage1_upload_template.compute_resources[constants.workflow.MEMORY] = 60
            stage1_upload_template.compute_resources[
                constants.workflow.MAX_RUNTIME
            ] = 3600  # 1 hour
        stage1_upload_task = stage1_upload_template.get_task(
            run_id=self.run_id, upstream_tasks=self.get_all_tasks_from_stage(tt.Stage1)
        )
        self.add_task(stage1_upload_task, stage1_upload_template)

    def create_st_jobs(self) -> None:
        """Creates spacetime jobs by holdout, param group, and loc group.

        Depends on stage 1 upload jobs.
        """
        spacetime_template = tt.Spacetime(self.tool)

        # Overwrite memory, runtime if diet model
        if self.is_diet_model:
            spacetime_template.compute_resources[constants.workflow.MEMORY] = 120
            spacetime_template.compute_resources[
                constants.workflow.MAX_RUNTIME
            ] = 28800  # 8 hours

        for holdout, param_group, loc_group in itertools.product(
            range(0, self.holdouts + 1),
            range(0, len(self.param_groups)),
            range(0, self.nparallel),
        ):
            submit_params = ",".join([str(x) for x in self.param_groups[param_group]])
            spacetime_task = spacetime_template.get_task(
                holdout=holdout,
                submit_params=submit_params,
                loc_group=loc_group,
                run_id=self.run_id,
                run_type=self.run_type,
                nparallel=self.nparallel,
                param_group=param_group,  # for task name
                upstream_tasks=self.get_all_tasks_from_stage(tt.Stage1Upload),
            )

            self.add_task(spacetime_task, spacetime_template)

    def create_spacetime_upload_jobs(self) -> None:
        """Creates spacetime upload jobs for each model iteration.

        Depends on all spacetime jobs and the previous spacetime upload jobs
        to avoid uploads in parallel.
        """
        spacetime_upload_template = tt.SpacetimeUpload(self.tool)

        for model_iteration_id, param_set in self.model_iteration_map:
            last_job = (param_set + 1) == self.n_parameter_sets

            spacetime_upload_task = spacetime_upload_template.get_task(
                model_iteration_id=model_iteration_id,
                param_set=param_set,
                last_job=last_job,
                run_id=self.run_id,
                upstream_tasks=(
                    self.get_all_tasks_from_stage(tt.Spacetime)
                    + self.get_all_tasks_from_stage(tt.SpacetimeUpload)
                ),
            )

            self.add_task(spacetime_upload_task, spacetime_upload_template)

    def create_descanso_jobs(self) -> None:
        """Creates descanso jobs by holdout and param group.

        Calculates intermediaries like AMP and NSV. Depends on spacetime upload jobs.
        """
        descanso_template = tt.Descanso(self.tool)

        if self.is_diet_model:
            descanso_template.compute_resources[constants.workflow.MAX_RUNTIME] = 3600

        for holdout, param_group in itertools.product(
            range(0, self.holdouts + 1), range(0, len(self.param_groups))
        ):
            submit_params = ",".join([str(x) for x in self.param_groups[param_group]])
            descanso_task = descanso_template.get_task(
                holdout=holdout,
                submit_params=submit_params,
                run_id=self.run_id,
                draws=self.draws,
                nparallel=self.nparallel,
                param_group=param_group,  # for task name
                upstream_tasks=self.get_all_tasks_from_stage(tt.SpacetimeUpload),
            )

            self.add_task(descanso_task, descanso_template)

    def create_descanso_upload_jobs(self) -> None:
        """Creates descanso upload jobs for each model iteration.

        Depend on all descanso job and the previous descanso upload jobs.
        """
        descanso_upload_template = tt.DescansoUpload(self.tool)

        for model_iteration_id, param_set in self.model_iteration_map:
            last_job = (param_set + 1) == self.n_parameter_sets
            descanso_upload_task = descanso_upload_template.get_task(
                model_iteration_id=model_iteration_id,
                param_set=param_set,
                last_job=last_job,
                run_id=self.run_id,
                upstream_tasks=(
                    self.get_all_tasks_from_stage(tt.Descanso)
                    + self.get_all_tasks_from_stage(tt.DescansoUpload)
                ),
            )

            self.add_task(descanso_upload_task, descanso_upload_template)

    def create_gpr_jobs(self) -> None:
        """Creates GPR jobs by holdout, param group and loc group.

        Depends on descanso upload jobs. Memory and runtime adjusted depending
        on number of draws and if model is a diet model.
        """
        gpr_template = tt.Gpr(self.tool)

        if self.draws == 100:
            gpr_template.compute_resources[constants.workflow.MEMORY] = 7
            gpr_template.compute_resources[constants.workflow.MAX_RUNTIME] = 1500
        elif self.draws == 1000:
            gpr_template.compute_resources[constants.workflow.MEMORY] = 10
            gpr_template.compute_resources[constants.workflow.MAX_RUNTIME] = 1800

        if self.is_diet_model:
            gpr_template.compute_resources[constants.workflow.MEMORY] *= 3
            gpr_template.compute_resources[constants.workflow.MAX_RUNTIME] *= 3

        for holdout, param_group, loc_group in itertools.product(
            range(0, self.holdouts + 1),
            range(0, len(self.param_groups)),
            range(0, self.nparallel),
        ):
            submit_params = ",".join([str(x) for x in self.param_groups[param_group]])
            gpr_task = gpr_template.get_task(
                holdout=holdout,
                submit_params=submit_params,
                loc_group=loc_group,
                run_id=self.run_id,
                draws=self.draws,
                nparallel=self.nparallel,
                param_group=param_group,  # for task name
                random_seed=self.random_seed,
                upstream_tasks=self.get_all_tasks_from_stage(tt.DescansoUpload),
            )

            self.add_task(gpr_task, gpr_template)

    def create_rake_jobs(self) -> None:
        """Creates rake jobs for each country with subnationals.

        If in-sample or out-of-sample selection, depends on eval job. If running
        with draws, depends on post GPR job for holdout 0, param group 0 to avoid
        rake jobs overwriting draw files post GPR job reads. Else, depends on GPR
        jobs including all subnational and national locations for each rake job.

        Raking is only done on the first holdout (KO 0), which does not hold out any
        data from the dataset.

        Memory and runtime are set at runtime based on country, number of draws
        and if diet model.
        """
        rake_template = tt.Rake(self.tool)

        for location_id in self.subnat_locations:
            memory = int(
                np.ceil(self.rake_memory_df.query(f"location == {location_id}")["mem"].iat[0])
            )
            runtime = int(
                np.ceil(
                    self.rake_memory_df.query(f"location == {location_id}")["runtime"].iat[0]
                )
            )

            if self.draws == 1000:
                memory *= 2
                runtime *= 3
                runtime = max(runtime, 7200)

            if self.is_diet_model:
                memory *= 2
                runtime *= 3
                runtime = max(runtime, 14400)

            # Overwrite rake template's memory and runtime for task
            rake_template.compute_resources[constants.workflow.MEMORY] = memory
            rake_template.compute_resources[constants.workflow.MAX_RUNTIME] = runtime

            rake_task = rake_template.get_task(
                location_id=location_id,
                run_id=self.run_id,
                draws=self.draws,
                run_type=self.run_type,
                rake_logit=self.rake_logit,
            )

            if self.run_type in ["in_sample_selection", "oos_selection"]:
                # depends on eval job to select best hyperparameter set
                self.add_upstream_all_tasks_from_stage(rake_task, tt.Eval)
            elif self.draws > 0:
                # depends on post GPR job for holdout 0 (raking only run for holdout 0)
                # and parameter group 0 (only one param group if not in-sample or
                # out-of-sample)
                post_gpr_task_name = tt.PostGpr.task_name_template.format(
                    run_id=self.run_id, holdout=0, param_group=0
                )
                rake_task.add_upstream(
                    self.task_map[tt.PostGpr.template_name][post_gpr_task_name]
                )
            else:
                # grab all subnationals and country location_ids associated with a country
                all_needed_locs = (
                    self.locs.loc[self.locs["level_3"] == location_id, "location_id"]
                    .unique()
                    .tolist()
                )

                for loc_group in list(range(0, self.nparallel)):
                    # add each gpr job containing a needed national/subnational
                    # for raking to upstreams
                    loc_group_vals = self.parallel_groups[loc_group].tolist()
                    common_elements = len(intersection(all_needed_locs, loc_group_vals))
                    if common_elements > 0:
                        gpr_task_name = tt.Gpr.task_name_template.format(
                            run_id=self.run_id, holdout=0, param_group=0, loc_group=loc_group
                        )
                        rake_task.add_upstream(
                            self.task_map[tt.Gpr.template_name][gpr_task_name]
                        )

            self.add_task(rake_task, rake_template)

    def create_post_rake_job(self) -> None:
        """Creates post-rake job.

        Depends on all rake jobs and post GPR jobs. Runs location aggregation
        and calculates summaries.
        """
        post_template = tt.PostRake(self.tool)

        post_task = post_template.get_task(
            run_id=self.run_id,
            upstream_tasks=(
                self.get_all_tasks_from_stage(tt.Rake)
                + self.get_all_tasks_from_stage(tt.PostGpr)
            ),
        )

        self.add_task(post_task, post_template)

    def create_rake_upload_jobs(self) -> None:
        """Creates rake upload jobs for each model iteration.

        Depends on post rake job, eval upload jobs and previous rake upload jobs.
        """
        rake_upload_template = tt.RakeUpload(self.tool)

        for model_iteration_id, param_set in self.model_iteration_map:
            last_job = (param_set + 1) == self.n_parameter_sets
            rake_upload_task = rake_upload_template.get_task(
                model_iteration_id=model_iteration_id,
                param_set=param_set,
                last_job=last_job,
                run_id=self.run_id,
                upstream_tasks=(
                    self.get_all_tasks_from_stage(tt.PostRake)
                    + self.get_all_tasks_from_stage(tt.RakeUpload)
                    + self.get_all_tasks_from_stage(tt.EvalUpload)
                ),
            )

            self.add_task(rake_upload_task, rake_upload_template)

    def create_post_gpr_jobs(self) -> None:
        """Creates post-GPR jobs by holdout and param group.

        Depends on GPR jobs corresponding to the holdout/param group
        (all loc groups). Runs location aggregation, calculates fit stats
        and summaries, no más.
        """
        post_template = tt.PostGpr(self.tool)

        for holdout, param_group in itertools.product(
            range(0, self.holdouts + 1), range(0, len(self.param_groups))
        ):
            submit_params = ",".join([str(x) for x in self.param_groups[param_group]])
            post_task = post_template.get_task(
                holdout=holdout,
                submit_params=submit_params,
                run_id=self.run_id,
                run_type=self.run_type,
                holdouts=self.holdouts,
                param_group=param_group,  # for task name
            )

            # Add upstream GPR jobs, which are parallelized further by loc group
            for loc_group in range(0, self.nparallel):
                gpr_task_name = tt.Gpr.task_name_template.format(
                    run_id=self.run_id,
                    holdout=holdout,
                    param_group=param_group,
                    loc_group=loc_group,
                )
                post_task.add_upstream(self.task_map[tt.Gpr.template_name][gpr_task_name])

            self.add_task(post_task, post_template)

    def create_gpr_upload_jobs(self) -> None:
        """Creates GPR upload jobs by model iteration.

        Depends on all post GPR jobs and previous GPR upload jobs.
        """
        gpr_upload_template = tt.GprUpload(self.tool)

        for model_iteration_id, param_set in self.model_iteration_map:
            last_job = (param_set + 1) == self.n_parameter_sets
            gpr_upload_task = gpr_upload_template.get_task(
                model_iteration_id=model_iteration_id,
                param_set=param_set,
                last_job=last_job,
                run_id=self.run_id,
                upstream_tasks=(
                    self.get_all_tasks_from_stage(tt.PostGpr)
                    + self.get_all_tasks_from_stage(tt.GprUpload)
                ),
            )

            self.add_task(gpr_upload_task, gpr_upload_template)

    def create_cleanup_jobs(self) -> None:
        """Creates cleanup jobs by holdout.

        Depends on all rake upload jobs. Saves rake summaries and removes
        tempfiles no longer needed.
        """
        cleanup_template = tt.Cleanup(self.tool)

        if self.draws != 0:
            cleanup_template.compute_resources[constants.workflow.MAX_RUNTIME] = 7200

        for holdout in range(0, self.holdouts + 1):
            cleanup_task = cleanup_template.get_task(
                holdout=holdout,
                run_id=self.run_id,
                run_type=self.run_type,
                draws=self.draws,
                upstream_tasks=self.get_all_tasks_from_stage(tt.RakeUpload),
            )

            self.add_task(cleanup_task, cleanup_template)

    def create_eval_jobs(self) -> None:
        """Creates the hyperparameter eval job.

        For hyperparameter selection runs only, determine best hyperparameter
        set based on in-sample or out-of-sample RMSE.
        - run_type = "in_sample_selection"
        - run_type = "oos_selection"

        For runs with only one set of parameters, set the best_param_set
        to the *only* param_set (param_set 0) for consistency in rake inputs.

        Lastly, just collect the disparate fit_stats files and combine into
        a single file, saved as fit_stats.csv for all run types.

        Depends on GPR upload jobs.
        """
        eval_template = tt.Eval(self.tool)
        eval_task = eval_template.get_task(
            run_id=self.run_id,
            run_type=self.run_type,
            holdouts=self.holdouts,
            n_parameter_sets=self.n_parameter_sets,
            upstream_tasks=self.get_all_tasks_from_stage(tt.GprUpload),
        )

        self.add_task(eval_task, eval_template)

    def create_eval_upload_jobs(self) -> None:
        """Create eval upload jobs for each model iteration.

        Depends on all eval jobs and previous eval upload jobs.
        """
        eval_upload_template = tt.EvalUpload(self.tool)

        for model_iteration_id, param_set in self.model_iteration_map:
            last_job = (param_set + 1) == self.n_parameter_sets
            eval_upload_task = eval_upload_template.get_task(
                model_iteration_id=model_iteration_id,
                param_set=param_set,
                last_job=last_job,
                run_id=self.run_id,
                upstream_tasks=(
                    self.get_all_tasks_from_stage(tt.Eval)
                    + self.get_all_tasks_from_stage(tt.EvalUpload)
                ),
            )

            self.add_task(eval_upload_task, eval_upload_template)

    def build_workflow(self) -> None:
        """Build the ST-GPR model's constants.workflow."""

        self.prep_parallelization_groups()
        self.create_prep_job()
        self.create_stage1_jobs()
        self.create_stage1_upload_job()
        self.create_st_jobs()
        self.create_spacetime_upload_jobs()
        self.create_descanso_jobs()
        self.create_descanso_upload_jobs()
        self.create_gpr_jobs()
        self.create_post_gpr_jobs()
        self.create_gpr_upload_jobs()

        # choose best parameter set to run rake for
        self.create_eval_jobs()
        self.create_eval_upload_jobs()

        # run rake/aggregation step and clean outputs
        self.assign_rake_runtimes()
        self.create_rake_jobs()
        self.create_post_rake_job()
        self.create_rake_upload_jobs()
        self.create_cleanup_jobs()

    def run(self) -> int:
        """Run main model estimation pipeline.

        Returns:
            0 on success, 1 on failure. Updates the model status in the db accordingly.
        """

        # Wrap jobmon calls so we can message out failure in case of jobmon issue
        try:
            self.build_workflow()
            status = self.workflow.run(
                seconds_until_timeout=constants.workflow.SECONDS_UNTIL_TIMEOUT, resume=True
            )
        except Exception as e:
            print(f"Uncaught exception attempting to set up and run jobmon workflow: {e}")
            status = "E"  # Error.

        print(f"Workflow finished with status {status}")

        # Set model status, return value based in if model was successful or not
        if status == "D":  # Done.
            model_status = stgpr_schema.ModelStatus.success
            return_val = 0
        else:
            model_status = stgpr_schema.ModelStatus.failure
            return_val = 1

        # Update model status to success/failure
        settings = stgpr_schema.get_settings()
        with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
            db_stgpr.update_model_status(self.run_id, model_status, scoped_session)

        # Print out some of the errors if failure
        if return_val:
            try:
                errors_dict = self.workflow.get_errors(limit=10)
                errors_string = "\n".join(
                    [
                        f"Task id: {k}: {v['error_log'][0]['description']}"
                        for k, v in errors_dict.items()
                    ]
                )
                num_errors = f"{len(errors_dict)}" if len(errors_dict) < 10 else "10+"
                print(f"ST-GPR model had {num_errors} error(s):\n{errors_string}")
            except AttributeError as e:
                print("ST-GPR model failed to build jobmon dag")

        return return_val
