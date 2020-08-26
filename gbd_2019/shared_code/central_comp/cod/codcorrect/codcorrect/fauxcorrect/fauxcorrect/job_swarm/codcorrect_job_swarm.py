
from collections import defaultdict
import os
from typing import Dict
import datetime

import gbd.constants as gbd
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus

from fauxcorrect import parameters as params
from fauxcorrect.parameters.master import CoDCorrectParameters
from fauxcorrect.utils.constants import (
    CauseSetId,
    DAG,
    DataBases,
    FilePaths,
    GBD,
    LocationAggregation,
    LocationSetId,
    Measures,
    MortalityInputs
)

class CoDCorrectSwarm:
    """
    Responsible for creating the Jobmon workflow and running CoDCorrect on the
    cluster.

    Methods:
        construct_workflow(): adds all the tasks of a CoDCorrect run to the
            workflow, assigning upstream dependencies when needed.

        run(): Initiates the FauxCorrect run by calling Jobmon.Workflow.run()
    """
    def __init__(
            self,
            parameters: params.master.CoDCorrectParameters,
            resume: bool = False
    ):
        """
        Creates an instance of a FauxCorrect JobSwarm.

        Arguments:
            parameters (parameters.master.FauxCorrectParameters): instance of
                the FauxCorrrect parameters that this job swarm will execute.
        """
        self.parameters: params.master.CoDCorrectParameters = parameters

        # Intuit the root code directory
        self.code_dir: str = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        # Create workflow object
        self.workflow: Workflow = Workflow(
            workflow_args=DAG.Workflow.CODCORRECT_WORKFLOW_ARGS.format(
                version_id=self.parameters.version_id,
                timestamp=datetime.datetime.now().isoformat()
            ),
            name=DAG.Workflow.CODCORRECT_NAME.format(
                version_id=self.parameters.version_id
            ),
            project=DAG.Workflow.PROJECT,
            stdout=os.path.join(
                self.parameters.parent_dir, FilePaths.LOG_DIR, FilePaths.STDOUT
            ),
            stderr=os.path.join(
                self.parameters.parent_dir, FilePaths.LOG_DIR, FilePaths.STDERR
            ),
            resume=resume,
            # 2 weeks (14*24*60*60)
            seconds_until_timeout=(1209600)
        )

        self.task_map: Dict[str, Dict[str, PythonTask]] = defaultdict(dict)

    def construct_workflow(self) -> None:
        """Adds all of our tasks to our workflow."""
        self.create_cache_tasks()
        self.create_draw_validate_tasks()
        self.create_apply_correction_tasks()
        self.create_cause_aggregation_tasks()
        if gbd.measures.YLL in self.parameters.measure_ids:
            self.create_calculate_ylls_tasks()
        self.create_location_aggregation_tasks()
        self.create_append_shocks_tasks()
        self.create_summarize_tasks()
        self.create_upload_tasks()

    def run(self) -> None:
        """Runs the FauxCorrect workflow and checks for success or failure."""
        exit_code = self.workflow.run()
        if exit_code != DagExecutionStatus.SUCCEEDED:
            raise RuntimeError(
                f"Workflow dag_id: {self.workflow.dag_id} was unsuccessful, "
                "check the job_swarm database for more information."
            )

    def create_cache_tasks(self) -> None:
        """
        Adds up to five cache tasks that must occur at the beginning of each
        FauxCorrect run:

            1. envelope
            2. population
            3. spacetime restrictions
            4. regional scalars
            5. pred_ex

        These jobs kick off the CoDCorrect process and do not have any
        upstream dependencies.
        """
        # Create mortality inputs cache jobs.
        for mort_process, memory in zip(MortalityInputs.ALL_INPUTS,
            DAG.Tasks.Memory.CACHE_ALL_MORTALITY_INPUTS
        ):
            cache_mortality_task = PythonTask(
                script=os.path.join(
                    self.code_dir, DAG.Executables.CACHE_MORT_INPUT
                ),
                args=[
                    '--mort_process', mort_process,
                    '--machine_process', self.parameters.process,
                    '--version_id', self.parameters.version_id
                ],
                name=DAG.Tasks.Name.CACHE_MORTALITY.format(
                    mort_process=mort_process
                ),
                num_cores=DAG.Tasks.Cores.CACHE_MORTALITY,
                m_mem_free=memory,
                max_runtime_seconds=DAG.Tasks.Runtime.CACHE_MORTALITY,
                queue=DAG.QUEUE,
                tag=DAG.Tasks.Type.CACHE
            )
            self.task_map[DAG.Tasks.Type.CACHE][cache_mortality_task.name] = (
                cache_mortality_task
            )
            self.workflow.add_task(cache_mortality_task)

        # Create cache_spacetime_restrictions job
        cache_spacetime_restrictions = PythonTask(
            script=os.path.join(
                self.code_dir, DAG.Executables.CORRECT
            ),
            args=[
                '--action', DAG.Tasks.Type.CACHE,
                '--parent_dir', self.parameters.parent_dir,
                '--gbd_round_id', self.parameters.gbd_round_id
            ],
            name=DAG.Tasks.Name.CACHE_SPACETIME,
            num_cores=DAG.Tasks.Cores.CACHE_SPACETIME_RESTRICTIONS,
            m_mem_free=DAG.Tasks.Memory.CACHE_SPACETIME_RESTRICTIONS,
            max_runtime_seconds=DAG.Tasks.Runtime.CACHE_SPACETIME_RESTRICTIONS,
            queue=DAG.QUEUE,
            tag=DAG.Tasks.Type.CACHE
        )
        self.task_map[DAG.Tasks.Type.CACHE][
            cache_spacetime_restrictions.name] = cache_spacetime_restrictions
        self.workflow.add_task(cache_spacetime_restrictions)

        # Create cache_regional_scalars job
        cache_regional_scalars = PythonTask(
            script=os.path.join(self.code_dir, DAG.Executables.LOC_AGG),
            args=[
                '--action', DAG.Tasks.Type.CACHE,
                '--parent_dir', self.parameters.parent_dir,
                '--gbd_round_id', self.parameters.gbd_round_id
            ],
            name=DAG.Tasks.Name.CACHE_REGIONAL_SCALARS,
            num_cores=DAG.Tasks.Cores.CACHE_REGIONAL_SCALARS,
            m_mem_free=DAG.Tasks.Memory.CACHE_REGIONAL_SCALARS,
            max_runtime_seconds=DAG.Tasks.Runtime.CACHE_REGIONAL_SCALARS,
            queue=DAG.QUEUE,
            tag=DAG.Tasks.Type.CACHE
        )
        self.task_map[DAG.Tasks.Type.CACHE][cache_regional_scalars.name] = (
            cache_regional_scalars
        )
        self.workflow.add_task(cache_regional_scalars)

        if gbd.measures.YLL in self.parameters.measure_ids:
            # Create cache_pred_ex job
            cache_pred_ex = PythonTask(
                script=os.path.join(self.code_dir, DAG.Executables.YLLS),
                args=[
                    '--action', DAG.Tasks.Type.CACHE,
                    '--parent_dir', self.parameters.parent_dir,
                    '--gbd_round_id', self.parameters.gbd_round_id
                ],
                name=DAG.Tasks.Name.CACHE_PRED_EX,
                num_cores=DAG.Tasks.Cores.CACHE_PRED_EX,
                m_mem_free=DAG.Tasks.Memory.CACHE_PRED_EX,
                max_runtime_seconds=DAG.Tasks.Runtime.CACHE_PRED_EX,
                queue=DAG.QUEUE,
                tag=DAG.Tasks.Type.CACHE
            )
            self.task_map[DAG.Tasks.Type.CACHE][cache_pred_ex.name] = (
                cache_pred_ex
            )
            self.workflow.add_task(cache_pred_ex)

    def create_draw_validate_tasks(self) -> None:
        """
        Adds tasks to validate the input draws against expected hypercube

        Creates one job per model_version_id in our parameters of best models.
        Doesn't utilize any cached assets, so these jobs don't have any
        upstream dependencies.
        """
        for model_version_id in self.parameters.best_model_version_ids:
            draw_validate_task = PythonTask(
                script=os.path.join(
                    self.code_dir, DAG.Executables.VALIDATE_DRAWS
                ),
                args=[
                    '--version_id', self.parameters.version_id,
                    '--machine_process', self.parameters.process,
                    '--model_version_id', model_version_id
                ],
                name=DAG.Tasks.Name.VALIDATE_DRAWS.format(
                    model_version_id=model_version_id
                ),
                num_cores=DAG.Tasks.Cores.VALIDATE_DRAWS,
                m_mem_free=DAG.Tasks.Memory.VALIDATE_DRAWS,
                max_runtime_seconds=DAG.Tasks.Runtime.VALIDATE_DRAWS,
                queue=DAG.QUEUE,
                tag=DAG.Tasks.Type.VALIDATE
            )
            self.task_map[DAG.Tasks.Type.VALIDATE][
                draw_validate_task.name
            ] = draw_validate_task
            self.workflow.add_task(draw_validate_task)

    def create_apply_correction_tasks(self) -> None:
        for sex in self.parameters.sex_ids:
            for location in self.parameters.most_detailed_location_ids:
                correct_task = PythonTask(
                    script=os.path.join(
                        self.code_dir, DAG.Executables.CORRECT
                    ),
                    args=[
                        '--action', DAG.Tasks.Type.CORRECT,
                        '--parent_dir', self.parameters.parent_dir,
                        '--gbd_round_id', self.parameters.gbd_round_id,
                        '--version_id', self.parameters.version_id,
                        '--location_id', location,
                        '--sex_id', sex,
                        '--env_version_id', self.parameters.envelope_version_id
                    ],
                    name=DAG.Tasks.Name.APPLY_CORRECTION.format(
                        location=location,
                        sex=sex
                    ),
                    num_cores=DAG.Tasks.Cores.APPLY_CORRECTION,
                    m_mem_free=DAG.Tasks.Memory.APPLY_CORRECTION,
                    upstream_tasks=[
                        self.task_map[DAG.Tasks.Type.CACHE][
                            DAG.Tasks.Name.CACHE_MORTALITY.format(
                                mort_process=MortalityInputs.ENVELOPE_DRAWS
                            )
                        ],
                        self.task_map[DAG.Tasks.Type.CACHE][
                            DAG.Tasks.Name.CACHE_MORTALITY.format(
                                mort_process=(
                                    MortalityInputs.ENVELOPE_SUMMARY
                                )
                            )
                        ]
                    ],
                    max_runtime_seconds=DAG.Tasks.Runtime.APPLY_CORRECTION,
                    tag=DAG.Tasks.Type.CORRECT
                )
                for mvid in self.parameters.best_model_version_ids:
                    upstream_name = DAG.Tasks.Name.VALIDATE_DRAWS.format(
                        model_version_id=mvid
                    )
                    correct_task.add_upstream(
                        self.task_map[DAG.Tasks.Type.VALIDATE][upstream_name]
                    )
                self.task_map[DAG.Tasks.Type.CORRECT][
                    correct_task.name
                ] = correct_task

                self.workflow.add_task(correct_task)

    def create_cause_aggregation_tasks(self) -> None:
        for sex in self.parameters.sex_ids:
            for location in self.parameters.most_detailed_location_ids:
                agg_cause_task = PythonTask(
                    script=os.path.join(
                        self.code_dir, DAG.Executables.CAUSE_AGG
                    ),
                    args=[
                        '--version_id', self.parameters.version_id,
                        '--parent_dir', self.parameters.parent_dir,
                        '--location_id', location,
                        '--sex_id', sex,
                    ],
                    name=DAG.Tasks.Name.CAUSE_AGGREGATION.format(
                        location=location,
                        sex=sex
                    ),
                    num_cores=DAG.Tasks.Cores.CAUSE_AGGREGATION,
                    m_mem_free=DAG.Tasks.Memory.CAUSE_AGGREGATION,
                    upstream_tasks=[
                        self.task_map[DAG.Tasks.Type.CORRECT][
                            DAG.Tasks.Name.APPLY_CORRECTION.format(
                                location=location,
                                sex=sex
                            )
                        ]
                    ],
                    max_runtime_seconds=DAG.Tasks.Runtime.CAUSE_AGGREGATION,
                    tag=DAG.Tasks.Type.CAUSE_AGG
                )
                self.task_map[DAG.Tasks.Type.CAUSE_AGG][
                    agg_cause_task.name
                ] = agg_cause_task
                self.workflow.add_task(agg_cause_task)

    def create_calculate_ylls_tasks(self) -> None:
        """
        Adds tasks to calculate YLLs from our scaled draws by location, sex,
        and year.

        Dependent on completed scalar application for respective locations,
        sexes, and years.
        """
        for sex in self.parameters.sex_ids:
            for location in self.parameters.most_detailed_location_ids:
                calc_ylls = PythonTask(
                    script=os.path.join(
                        self.code_dir, DAG.Executables.YLLS
                    ),
                    args=[
                        '--action', DAG.Tasks.Type.CALCULATE,
                        '--machine_process', self.parameters.process,
                        '--parent_dir', self.parameters.parent_dir,
                        '--location_id', location,
                        '--sex_id', sex
                    ],
                    name=DAG.Tasks.Name.CALC_YLLS.format(
                        location=location,
                        sex=sex
                    ),
                    num_cores=DAG.Tasks.Cores.YLLS,
                    m_mem_free=DAG.Tasks.Memory.YLLS,
                    max_runtime_seconds=DAG.Tasks.Runtime.CALCULATE,
                    upstream_tasks=[
                        self.task_map[DAG.Tasks.Type.CACHE][
                            DAG.Tasks.Name.CACHE_PRED_EX
                        ],
                        self.task_map[DAG.Tasks.Type.CAUSE_AGG][
                            DAG.Tasks.Name.CAUSE_AGGREGATION.format(
                                location=location,
                                sex=sex
                            )
                        ]
                    ],
                    tag=DAG.Tasks.Type.CALCULATE
                )
                self.task_map[DAG.Tasks.Type.CALCULATE][
                    calc_ylls.name
                ] = calc_ylls
                self.workflow.add_task(calc_ylls)

    def create_location_aggregation_tasks(self) -> None:
        """
        Adds tasks to aggregate up the location hierarchy for each location set
        id, sex, year, and measure in our FauxCorrect run.

        Dependent on each location, sex, and year specific group of scalar or
        calculate ylls tasks to be completed for their respective measure ids.
        """
        for location_set_id in self.parameters.location_set_ids:
            for year in self.parameters.year_ids:
                for measure in self.parameters.measure_ids:
                    for loc_agg_type in LocationAggregation.Type.CODCORRECT:
                        if (
                            FilePaths.UNSCALED_DIR in loc_agg_type
                            and
                            measure == Measures.Ids.YLLS
                        ):
                            continue
                        agg_task = PythonTask(
                            script=os.path.join(
                                self.code_dir,
                                DAG.Executables.LOC_AGG
                            ),
                            args=[
                                '--action', DAG.Tasks.Type.LOC_AGG,
                                '--parent_dir', self.parameters.parent_dir,
                                '--gbd_round_id', (
                                    self.parameters.gbd_round_id
                                ),
                                '--aggregation_type', loc_agg_type,
                                '--location_set_id', location_set_id,
                                '--year_id', year,
                                '--measure_id', measure
                            ],
                            name=DAG.Tasks.Name.LOCATION_AGGREGATION.format(
                                aggregation_type=(
                                    loc_agg_type.replace("/","_")
                                ),
                                location_set=location_set_id,
                                measure=measure,
                                year=year
                            ),
                            num_cores=DAG.Tasks.Cores.LOCATION_AGGREGATION,
                            m_mem_free=(
                                DAG.Tasks.Memory.LOCATION_AGGREGATION
                            ),
                            max_runtime_seconds=(
                                DAG.Tasks.Runtime.LOCATION_AGGREGATION
                            ),
                            queue=DAG.QUEUE,
                            upstream_tasks=[
                                self.task_map[DAG.Tasks.Type.CACHE][
                                    DAG.Tasks.Name.CACHE_REGIONAL_SCALARS
                                ]
                            ],
                            tag=DAG.Tasks.Type.LOC_AGG
                        )
                        # Attach upstream dependencies, all locations for
                        # sex and year.
                        for sex in self.parameters.sex_ids:
                            for loc in (
                                self.parameters.most_detailed_location_ids
                            ):
                                if measure == Measures.Ids.YLLS:
                                    # If aggregating measure 4 (YLLs), attach
                                    # calc ylls jobs as upstream
                                    agg_task.add_upstream(
                                        self.task_map[DAG.Tasks.Type.CALCULATE][
                                            DAG.Tasks.Name.CALC_YLLS.format(
                                                location=loc,
                                                sex=sex
                                            )
                                        ]
                                    )
                                else:
                                    # If measure is not 4 (YLLs), then add
                                    # cause agg jobs as upstream.
                                    agg_task.add_upstream(
                                       self.task_map[DAG.Tasks.Type.CAUSE_AGG][
                                            (DAG.Tasks.Name.CAUSE_AGGREGATION
                                            .format(
                                                location=loc,
                                                sex=sex
                                            ))
                                        ]
                                    )
                        if location_set_id not in [
                            LocationSetId.OUTPUTS, LocationSetId.SDI,
                            LocationSetId.STANDARD
                        ]:
                            # If location set is one of the special
                            # sets that central computation only aggregates
                            # at the end of a round, add the outputs location
                            # set as upstream dependency so it finishes first.
                            agg_task.add_upstream(
                                self.task_map[DAG.Tasks.Type.LOC_AGG][
                                    DAG.Tasks.Name.LOCATION_AGGREGATION.format(
                                        aggregation_type=(
                                            loc_agg_type.replace("/","_")
                                        ),
                                        location_set=LocationSetId.OUTPUTS,
                                        measure=measure,
                                        year=year
                                    )
                                ]
                            )
                        self.task_map[DAG.Tasks.Type.LOC_AGG][
                            agg_task.name
                        ] = agg_task

                        self.workflow.add_task(agg_task)

    def create_append_shocks_tasks(self) -> None:
        """
        Adds tasks to append shocks to Deaths and YLLs by location, sex,
        and year.

        Dependent on completed correction application for respective locations,
        sexes, and years. Also dependent on location aggregation.
        """
        for sex in self.parameters.sex_ids:
            for location in self.parameters.location_ids:
                most_detailed_location = (
                    location in self.parameters.most_detailed_location_ids
                )
                append_shocks = PythonTask(
                    script=os.path.join(
                        self.code_dir, DAG.Executables.APPEND_SHOCKS
                    ),
                    args=[
                        '--parent_dir', self.parameters.parent_dir,
                        '--machine_process', self.parameters.process,
                        '--measure_ids',
                        " ".join([str(x) for x in self.parameters.measure_ids]),
                        '--location_id', location,
                        '--most_detailed_location', most_detailed_location,
                        '--sex_id', sex
                    ],
                    name=DAG.Tasks.Name.APPEND_SHOCKS.format(
                        location=location, sex=sex
                    ),
                    num_cores=DAG.Tasks.Cores.APPEND_SHOCKS,
                    m_mem_free=DAG.Tasks.Memory.APPEND_SHOCKS,
                    max_runtime_seconds=DAG.Tasks.Runtime.APPEND,
                    tag=DAG.Tasks.Type.APPEND
                )
                if (
                    most_detailed_location
                    and
                    Measures.Ids.YLLS in self.parameters.measure_ids
                ):
                    # attach calc ylls jobs as upstream
                    append_shocks.add_upstream(
                        self.task_map[DAG.Tasks.Type.CALCULATE][
                            DAG.Tasks.Name.CALC_YLLS.format(
                                location=location,
                                sex=sex
                            )
                        ]
                    )
                elif (
                    most_detailed_location
                    and
                    Measures.Ids.YLLS not in self.parameters.measure_ids
                ):
                    # add cause agg jobs as upstream.
                    append_shocks.add_upstream(
                       self.task_map[DAG.Tasks.Type.CAUSE_AGG][
                            (DAG.Tasks.Name.CAUSE_AGGREGATION
                            .format(
                                location=location,
                                sex=sex
                            ))
                        ]
                    )
                else:
                    # Add location aggregation tasks as upstream dependency.
                    for location_set_id in self.parameters.location_set_ids:
                        for loc_agg_year in self.parameters.year_ids:
                            for loc_agg_measure in self.parameters.measure_ids:
                                for loc_agg_type in (
                                    LocationAggregation.Type.CODCORRECT
                                ):
                                    if (
                                        FilePaths.UNSCALED_DIR in loc_agg_type
                                        and
                                        loc_agg_measure == Measures.Ids.YLLS
                                    ):
                                        continue
                                    append_shocks.add_upstream(
                                        self.task_map[
                                            DAG.Tasks.Type.LOC_AGG][(
                                            DAG.Tasks.Name.LOCATION_AGGREGATION
                                            .format(
                                                aggregation_type=(
                                                    loc_agg_type
                                                    .replace("/","_")
                                                ),
                                                location_set=location_set_id,
                                                measure=loc_agg_measure,
                                                year=loc_agg_year,
                                            )
                                        )]
                                    )
                self.task_map[DAG.Tasks.Type.APPEND][
                    append_shocks.name
                ] = append_shocks
                self.workflow.add_task(append_shocks)

    def create_summarize_tasks(self) -> None:
        """
        Adds tasks to summarize draw level estimates for each location, year,
        and measure in our FauxCorrect run.

        Dependent on the mortality input caching tasks as well as the append
        shocks tasks.

        """
        for measure in self.parameters.measure_ids:
            for year in self.parameters.year_ids:
                for location in self.parameters.location_ids:
                    # Create summarize tasks for gbd schema.
                    summarize_gbd_task = PythonTask(
                        script=os.path.join(
                            self.code_dir,
                            DAG.Executables.SUMMARIZE_GBD
                        ),
                        args=[
                            '--parent_dir', self.parameters.parent_dir,
                            '--gbd_round_id', self.parameters.gbd_round_id,
                            '--location_id', location,
                            '--year_id', year,
                            '--measure_id', measure,
                            '--machine_process', self.parameters.process
                        ],
                        name=DAG.Tasks.Name.SUMMARIZE_GBD.format(
                            measure=measure, location=location, year=year
                        ),
                        num_cores=DAG.Tasks.Cores.SUMMARIZE,
                        m_mem_free=DAG.Tasks.Memory.SUMMARIZE,
                        max_runtime_seconds=DAG.Tasks.Runtime.SUMMARIZE,
                        queue=DAG.QUEUE,
                        upstream_tasks=[
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=MortalityInputs.ENVELOPE_DRAWS
                                )
                            ],
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=(
                                        MortalityInputs.ENVELOPE_SUMMARY
                                    )
                                )
                            ],
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=MortalityInputs.POPULATION
                                )
                            ]
                        ],
                        tag=DAG.Tasks.Type.SUMMARIZE
                    )
                    for append_sex in self.parameters.sex_ids:
                        summarize_gbd_task.add_upstream(
                            self.task_map[DAG.Tasks.Type.APPEND][
                                DAG.Tasks.Name.APPEND_SHOCKS.format(
                                    location=location,
                                    sex=append_sex
                                )
                            ]
                        )
                    self.task_map[DAG.Tasks.Type.SUMMARIZE][
                        summarize_gbd_task.name
                    ] = summarize_gbd_task

                    self.workflow.add_task(summarize_gbd_task)

                    if (
                        measure == Measures.Ids.YLLS
                        or
                        DataBases.COD not in self.parameters.databases
                    ):
                        continue
                    # Create summarize tasks for deaths and cod schema.
                    summarize_cod_task = PythonTask(
                        script=os.path.join(
                            self.code_dir,
                            DAG.Executables.SUMMARIZE_COD
                        ),
                        args=[
                            '--version_id', self.parameters.version_id,
                            '--parent_dir', self.parameters.parent_dir,
                            '--gbd_round_id', self.parameters.gbd_round_id,
                            '--location_id', location,
                            '--year_id', year
                        ],
                        name=DAG.Tasks.Name.SUMMARIZE_COD.format(
                            measure=Measures.Ids.DEATHS,
                            location=location,
                            year=year
                        ),
                        num_cores=DAG.Tasks.Cores.SUMMARIZE,
                        m_mem_free=DAG.Tasks.Memory.SUMMARIZE,
                        max_runtime_seconds=DAG.Tasks.Runtime.SUMMARIZE,
                        queue=DAG.QUEUE,
                        upstream_tasks=[
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=MortalityInputs.ENVELOPE_DRAWS
                                )
                            ],
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=(
                                        MortalityInputs.ENVELOPE_SUMMARY
                                    )
                                )
                            ],
                            self.task_map[DAG.Tasks.Type.CACHE][
                                DAG.Tasks.Name.CACHE_MORTALITY.format(
                                    mort_process=MortalityInputs.POPULATION
                                )
                            ]
                        ],
                        tag=DAG.Tasks.Type.SUMMARIZE
                    )
                    for append_sex in self.parameters.sex_ids:
                        summarize_cod_task.add_upstream(
                            self.task_map[DAG.Tasks.Type.APPEND][
                                DAG.Tasks.Name.APPEND_SHOCKS.format(
                                    location=location,
                                    sex=append_sex
                                )
                            ]
                        )
                    self.task_map[DAG.Tasks.Type.SUMMARIZE][
                        summarize_cod_task.name
                    ] = summarize_cod_task

                    self.workflow.add_task(summarize_cod_task)

            # pct_change summarization tasks
            if self.parameters.year_start_ids:
                for year_index in range(len(self.parameters.year_start_ids)):
                    for pctc_location in self.parameters.location_ids:
                        summarize_pct_change_task = PythonTask(
                            script=os.path.join(
                                self.code_dir,
                                DAG.Executables.SUMMARIZE_PCT_CHANGE
                            ),
                            args=[
                                '--parent_dir', self.parameters.parent_dir,
                                '--gbd_round_id', self.parameters.gbd_round_id,
                                '--location_id', pctc_location,
                                '--year_start_id', self.parameters.year_start_ids[
                                    year_index],
                                '--year_end_id', self.parameters.year_end_ids[
                                    year_index],
                                '--measure_id', measure,
                                '--machine_process', self.parameters.process
                            ],
                            name=DAG.Tasks.Name.SUMMARIZE_PCT_CHANGE.format(
                                measure=measure, location=pctc_location,
                                year_start=self.parameters.year_start_ids[year_index],
                                year_end=self.parameters.year_end_ids[year_index]
                            ),
                            num_cores=DAG.Tasks.Cores.PCT_CHANGE,
                            m_mem_free=DAG.Tasks.Memory.PCT_CHANGE,
                            max_runtime_seconds=DAG.Tasks.Runtime.SUMMARIZE,
                            queue=DAG.QUEUE,
                            upstream_tasks=[
                                self.task_map[DAG.Tasks.Type.CACHE][
                                    DAG.Tasks.Name.CACHE_MORTALITY.format(
                                        mort_process=MortalityInputs.ENVELOPE_DRAWS
                                    )
                                ],
                                self.task_map[DAG.Tasks.Type.CACHE][
                                    DAG.Tasks.Name.CACHE_MORTALITY.format(
                                        mort_process=(
                                            MortalityInputs.ENVELOPE_SUMMARY
                                        )
                                    )
                                ],
                                self.task_map[DAG.Tasks.Type.CACHE][
                                    DAG.Tasks.Name.CACHE_MORTALITY.format(
                                        mort_process=MortalityInputs.POPULATION
                                    )
                                ]
                            ],
                            tag=DAG.Tasks.Type.SUMMARIZE
                        )
                        for append_sex in self.parameters.sex_ids:
                            summarize_pct_change_task.add_upstream(
                                self.task_map[DAG.Tasks.Type.APPEND][
                                    DAG.Tasks.Name.APPEND_SHOCKS.format(
                                        location=pctc_location,
                                        sex=append_sex
                                    )
                                ]
                            )
                        self.task_map[DAG.Tasks.Type.SUMMARIZE][
                            summarize_pct_change_task.name
                        ] = summarize_pct_change_task

                        self.workflow.add_task(summarize_pct_change_task)

    def create_upload_tasks(self) -> None:
        """
        Adds tasks to upload summaries to gbd and / or cod databases.
        """
        # Determine if percent change is part of workflow
        if self.parameters.year_start_ids is not None:
            upload_types = ['single', 'multi']
        else:
            upload_types = ['single']

        # gbd upload tasks
        for measure in self.parameters.measure_ids:
            for upload_type in upload_types:
                upload_gbd_task = PythonTask(
                    script=os.path.join(
                        self.code_dir,
                        DAG.Executables.UPLOAD
                    ),
                    args=[
                        '--machine_process', self.parameters.process,
                        '--version_id', self.parameters.version_id,
                        '--database', DataBases.GBD,
                        '--measure_id', measure,
                        '--upload_type', upload_type
                    ],
                    name=DAG.Tasks.Name.UPLOAD.format(
                        database=DataBases.GBD,
                        uploadtype=upload_type,
                        measure=measure
                    ),
                    num_cores=DAG.Tasks.Cores.UPLOAD,
                    m_mem_free=DAG.Tasks.Memory.UPLOAD,
                    max_runtime_seconds=DAG.Tasks.Runtime.UPLOAD,
                    queue=DAG.UPLOAD_QUEUE,
                    tag=DAG.Tasks.Type.UPLOAD
                )
                # add gbd summarization tasks as upstream dependencies
                if upload_type == 'single':
                    for year in self.parameters.year_ids:
                        for location in self.parameters.location_ids:
                            upload_gbd_task.add_upstream(
                                self.task_map[DAG.Tasks.Type.SUMMARIZE][
                                    DAG.Tasks.Name.SUMMARIZE_GBD.format(
                                        measure=measure,
                                        location=location,
                                        year=year
                                    )
                                ]
                            )
                else:
                    for year_index in range(len(self.parameters.year_start_ids)):
                        for location in self.parameters.location_ids:
                            upload_gbd_task.add_upstream(
                                self.task_map[DAG.Tasks.Type.SUMMARIZE][
                                    DAG.Tasks.Name.SUMMARIZE_PCT_CHANGE.format(
                                        measure=measure,
                                        location=location,
                                        year_start=self.parameters.year_start_ids[
                                            year_index],
                                        year_end=self.parameters.year_end_ids[
                                            year_index]
                                    )
                                ]
                            )
                self.task_map[DAG.Tasks.Type.UPLOAD][
                    upload_gbd_task.name
                ] = upload_gbd_task

                self.workflow.add_task(upload_gbd_task)

        # cod upload tasks - DEATHS only
        if DataBases.COD in self.parameters.databases:
            upload_cod_task = PythonTask(
                script=os.path.join(
                    self.code_dir,
                    DAG.Executables.UPLOAD),

                args=[
                    '--machine_process', self.parameters.process,
                    '--version_id', self.parameters.version_id,
                    '--database', DataBases.COD,
                    '--measure_id', Measures.Ids.DEATHS,
                ],
                name=DAG.Tasks.Name.UPLOAD.format(
                    database=DataBases.COD,
                    uploadtype='single',
                    measure=Measures.Ids.DEATHS
                ),
                num_cores=DAG.Tasks.Cores.UPLOAD,
                m_mem_free=DAG.Tasks.Memory.UPLOAD,
                max_runtime_seconds=DAG.Tasks.Runtime.UPLOAD,
                queue=DAG.QUEUE,
                tag=DAG.Tasks.Type.UPLOAD
            )
            # add cod summarization tasks as upstream dependencies
            for year in self.parameters.year_ids:
                for location in self.parameters.location_ids:
                    upload_cod_task.add_upstream(
                        self.task_map[DAG.Tasks.Type.SUMMARIZE][
                            DAG.Tasks.Name.SUMMARIZE_COD.format(
                                measure=Measures.Ids.DEATHS,
                                location=location,
                                year=year
                            )
                        ]
                    )
            self.task_map[DAG.Tasks.Type.UPLOAD][
                upload_cod_task.name
            ] = upload_cod_task

            self.workflow.add_task(upload_cod_task)