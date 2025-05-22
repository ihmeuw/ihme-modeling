import itertools
import getpass
import sys
from loguru import logger
from typing import List, Optional, Tuple
from pathlib import Path

import numpy as np

from db_queries.api.internal import get_active_location_set_version
from gbd.constants import measures
from gbd.enums import Cluster
from gbd.estimation_years import estimation_years_from_release_id
from hierarchies import dbtrees
from jobmon.client.api import Tool
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core import exceptions
from gbd_outputs_versions import GBDProcessVersion

from como.lib import constants as como_constants
from como.legacy.common import configure_logging
from como.lib.workflows.utils import post_slack, get_sorted_locations
from como.lib.version import ComoVersion
from como.lib.tasks.minimum_incidence_cod_task import MinimumIncidenceCodTaskFactory
from como.legacy.tasks.disability_weight_task import DisabilityWeightTaskFactory
from como.lib.tasks.incidence_task import IncidenceTaskFactory
from como.legacy.tasks.simulation_input_task import SimulationInputTaskFactory
from como.legacy.tasks.simulation_task import SimulationTaskFactory
from como.legacy.tasks.location_agg_task import LocationAggTaskFactory
from como.legacy.tasks.pct_change_task import PctChangeTaskFactory

from como.lib.tasks.internal_upload_task import InternalUploadTaskFactory
from como.lib.tasks.public_sort_task import PublicSortTaskFactory
from como.lib.tasks.public_upload_task import PublicUploadTaskFactory
from como.lib.tasks.summarize_task import SummarizeTaskFactory
from como.lib.upload import public_upload as public_upload_lib
from como.lib import fileshare_inputs as fio, utils
from ihme_cc_cache import FileBackedCacheWriter

CLUSTER_NAME = Cluster.SLURM.value
JOBMON_URL = "URL"


class ComoWorkFlow:
    def __init__(self, como_version):
        self.como_version = como_version
        self.tool = Tool("como")
        # instantiate our factories
        self._task_registry = {}
        self._min_inc_cod_task_fac = MinimumIncidenceCodTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._disability_weight_task_fac = DisabilityWeightTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._public_sort_task_fac = PublicSortTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._public_upload_task_fac = PublicUploadTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._incidence_task_fac = IncidenceTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._simulation_input_task_fac = SimulationInputTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._simulation_task_fac = SimulationTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._location_agg_task_fac = LocationAggTaskFactory(
            self.como_version, self._task_registry, self.tool
        )
        self._summarize_task_fac = SummarizeTaskFactory(
            self.como_version,
            self._task_registry,
            self._location_agg_task_fac.agg_loc_set_map,
            self.tool,
        )
        self._pct_change_task_fac = PctChangeTaskFactory(
            self.como_version,
            self._task_registry,
            self._location_agg_task_fac.agg_loc_set_map,
            self.tool,
        )
        user = getpass.getuser()
        self.workflow = self.tool.create_workflow(
            workflow_args=f"como_v{self.como_version.como_version_id}",
            name=f"COMO v{self.como_version.como_version_id}",
            default_cluster_name=CLUSTER_NAME,
            default_max_attempts=como_constants.JOBMON_MAX_ATTEMPTS,
            default_compute_resources_set={
                Cluster.SLURM.value: {
                    "project": "proj_como",
                    "standard_error": "FILEPATH",
                    "standard_output": "FILEPATH",
                    "stderr": "FILEPATH",
                    "stdout": "FILEPATH",
                    "queue": "all.q",
                }
            },
        )
    
    def _add_min_inc_cod_tasks(self):
        logger.info("adding minimum incidence cod tasks to DAG...")
        cfr = fio.get_case_fatality_rates()
        cause_ids = cfr["cause_id"].unique()
        self._incidence_task_fac.tmi_cause_ids = cause_ids
        concurrent_min_inc_limit = 100
        for index, cause_id in enumerate(cause_ids):
            upstream_cause_id = None
            if index >= concurrent_min_inc_limit:
                upstream_cause_id = cause_ids[index - concurrent_min_inc_limit]
            min_inc_cod_task = self._min_inc_cod_task_fac.get_task(
                cause_id=cause_id, upstream_cause_id=upstream_cause_id
            )
            self.workflow.add_task(min_inc_cod_task)
        
        # Collect items in a cache
        scalable_seqs = fio.get_scalable_seqs()
        manifest_path = Path(self.como_version.como_dir) / como_constants.TMI_CACHE_PATH / como_constants.TMI_CACHE_MANIFEST_PATH
        manifest_path.parent.mkdir(exist_ok=True, parents=True)
        cache_writer = FileBackedCacheWriter(manifest_path)
        cache_writer.put(
            obj=cfr,
            obj_name=como_constants.TMI_CACHE_CFR,
            relative_path=como_constants.TMI_CFR_FILEPATH,
            storage_format=".h5"
        )
        cache_writer.put(
            obj=scalable_seqs,
            obj_name=como_constants.TMI_CACHE_SEQS,
            relative_path=como_constants.TMI_SEQS_FILEPATH,
            storage_format=".json"
        )
        logger.info("...finished adding minimum incidence cod tasks to DAG")

    def _add_disability_weight_tasks(self):
        logger.info("adding disability weight tasks to DAG...")
        parallelism = ["location_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for slices in d.index_slices(parallelism):
            disability_weight_task = self._disability_weight_task_fac.get_task(
                location_id=slices[0], n_processes=5
            )
            self.workflow.add_task(disability_weight_task)
        logger.info("...finished adding disability weight tasks to DAG")
    
    def _add_public_sort_tasks(
        self, sorted_locs: List[int], component_measure_list: List[Tuple[str, int]]
    ) -> None:
        """Given a sorted list of locations and a list of component/measure tuples, adds
        all required public sort tasks to the Jobmon DAG.

        Public sort tasks are location, measure, component, and year type (single/multi)
        specific. Public sort tasks read in existing summaries and resave the data into
        standard files at a sub-directory within the COMO dir
        ({como_dir}/public_upload/summaries) that the public upload tasks then ingest in
        order to upload to the public CS host. Upstream logic is handled in
        public_sort_task.py, and upstream tasks are either summarization or percent
        change tasks depending on the year type of the sort task.
        """
        for component, measure_id in component_measure_list:
            if (
                component == como_constants.Component.IMPAIRMENT
                and measure_id == measures.INCIDENCE
            ):
                continue
            for location_id in sorted_locs:
                public_sort_task = self._public_sort_task_fac.get_task(
                    measure_id=measure_id,
                    component=component,
                    location_id=location_id,
                    year_type=como_constants.YearType.SINGLE,
                )
                self.workflow.add_task(public_sort_task)
            
                if self.como_version.change_years:
                    public_sort_task = self._public_sort_task_fac.get_task(
                        measure_id=measure_id,
                        component=component,
                        location_id=location_id,
                        year_type=como_constants.YearType.MULTI,
                    )
                    self.workflow.add_task(public_sort_task)
        
        logger.info("...finished adding public sort tasks to DAG")
    
    def _add_public_upload_tasks(
        self, sorted_locs: List[int], component_measure_list: List[Tuple[str, int]]
    ) -> None:
        """Given a sorted list of locations and a list of component/measure tuples, adds
        all required public upload tasks to the Jobmon DAG.

        Public upload tasks are location, measure, component, and year type
        (single/multi) specific. Upstream task specification is somewhat complex, and is
        handled both here and in public_upload_task.py. The public host can currently
        only handle four concurrent uploads, so we need to limit the total number of
        upload jobs at any given point to four. Additionally, we need to upload to
        tables strictly in sorted location order. Since tables are measure, component,
        and year type specific, this means we need to make each location upload task
        dependent on the previous location upload. Because of this natural throttling,
        we only need to explicitly throttle the initial jobs for each
        measure/component/year type set of uploads.
        """
        initial_tasks = []
        initial_to_final = {}
        for year_type in [como_constants.YearType.SINGLE, como_constants.YearType.MULTI]:
            if (not self.como_version.change_years) and (
                year_type == como_constants.YearType.MULTI
            ):
                continue
            for component, measure_id in component_measure_list:
                if (
                    component == como_constants.Component.IMPAIRMENT
                    and measure_id == measures.INCIDENCE
                ):
                    continue
                for index, location_id in enumerate(sorted_locs):
                    upstream_location_id = None if index == 0 else sorted_locs[index - 1]
                    public_upload_task = self._public_upload_task_fac.get_task(
                        measure_id=measure_id,
                        component=component,
                        location_id=location_id,
                        year_type=year_type,
                        upstream_location_id=upstream_location_id,
                    )
                    if index == 0:
                        initial_tasks.append(public_upload_task)
                        initial_task = public_upload_task
                    elif index == len(sorted_locs) - 1:
                        initial_to_final[initial_task] = public_upload_task
                        self.workflow.add_task(public_upload_task)
                    else:
                        self.workflow.add_task(public_upload_task)
        
        # Handle public upload task throttling
        public_infile_threshold = 4
        for initial_task in initial_tasks[: public_infile_threshold]:
            self.workflow.add_task(initial_task)
        for index in range(public_infile_threshold):
            this_group = initial_tasks[index :: public_infile_threshold]
            for upstream_initial, task in zip(this_group, this_group[1:]):
                upstream_task = initial_to_final[upstream_initial]
                task.add_upstream(upstream_task)
                self.workflow.add_task(task)

        logger.info("...finished adding public upload tasks to DAG")
                    
    def _get_upload_upstreams(self, location_id, measure_id) -> List:
        upstream_task_list = []
        if self.como_version.change_years:
            upstream_task_list.append(
                PctChangeTaskFactory.get_task_name(
                    measure_id=measure_id, location_id=location_id
                )
            )
        upstream_task_list.extend(
            [
                SummarizeTaskFactory.get_task_name(
                    measure_id=measure_id, location_id=location_id, year_id=year_id
                )
                for year_id in self.como_version.year_id
            ]
        )
        upstream_tasks = [self._task_registry[task] for task in upstream_task_list if task]
        return upstream_tasks
    
    def _add_internal_upload_tasks(self, sorted_locs) -> list[str]:
        """Returns a list of task template names."""
        year_types = [como_constants.YearType.SINGLE]
        if len(self.como_version.change_years) > 0:
            year_types.append(como_constants.YearType.MULTI)
        
        task_factory_names = dict()
        for component, measure_id, year_type in itertools.product(
            self.como_version.components, self.como_version.measure_id, year_types
        ):
            # every (component, measure, year_type) is a specific table
            if (component == "impairment") and (measure_id == measures.INCIDENCE):
                # There is no impairment incidence
                continue
            task_factory = InternalUploadTaskFactory(
                como_version=self.como_version,
                component=component,
                measure_id=measure_id,
                year_type=year_type,
                tool=self.tool,
            )
            task_factory_names[(component, measure_id, year_type)] = (
                task_factory.task_template.template_name
            )
            for location_id in sorted_locs:
                # for a given table, we make a linear DAG based on location_id
                # only one concurrent task per table
                upstreams = self._get_upload_upstreams(location_id, measure_id)
                self.workflow.add_task(
                    task_factory.get_task(location_id=location_id, upstream_tasks=upstreams)
                ) # upload upstreams should be a list
            logger.info(
                f"Added tasks for component {component}, measure {measure_id}, year_type "
                f"{year_type}."
            )
        logger.info("...finished adding internal upload tasks to DAG")
        return list(task_factory_names.values())

    def _add_incidence_tasks(self):
        logger.info("adding incidence tasks to DAG...")
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for slices in d.index_slices(parallelism):
            incidence_task = self._incidence_task_fac.get_task(
                location_id=slices[0], sex_id=slices[1], n_processes=20
            )
            self.workflow.add_task(incidence_task)
        logger.info("...finished adding incidence tasks to DAG")

    def _add_simulation_input_tasks(self):
        logger.info("adding simulation input tasks to DAG...")
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for slices in d.index_slices(parallelism):
            simulation_input_task = self._simulation_input_task_fac.get_task(
                location_id=slices[0], sex_id=slices[1], n_processes=23
            )
            self.workflow.add_task(simulation_input_task)
        logger.info("...finished adding simulation input tasks to DAG")

    def _add_simulation_tasks(self):
        logger.info("adding simulation tasks to DAG...")
        parallelism = ["location_id", "sex_id", "year_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for slices in d.index_slices(parallelism):
            sim_task = self._simulation_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                year_id=slices[2],
                n_processes=23,
            )
            self.workflow.add_task(sim_task)
        logger.info("...finished adding simulation input tasks to DAG")

    def _add_loc_aggregation_tasks(self, agg_loc_set_versions):
        logger.info("adding loc aggregation tasks to DAG...")
        parallelism = ["year_id", "sex_id", "measure_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for location_set_version_id in agg_loc_set_versions:
            loc_trees = dbtrees.loctree(
                location_set_version_id=location_set_version_id,
                release_id=self.como_version.release_id,
                return_many=True,
            )
            for slices in d.index_slices(parallelism):
                for component in self.como_version.components:
                    if not (component == "impairment" and slices[2] == measures.INCIDENCE):
                        agg_task = self._location_agg_task_fac.get_task(
                            component=component,
                            year_id=slices[0],
                            sex_id=slices[1],
                            measure_id=slices[2],
                            loc_trees=loc_trees,
                            location_set_version_id=location_set_version_id,
                        )
                        self.workflow.add_task(agg_task)
        logger.info("...finished adding loc aggregation tasks to DAG")

    def _add_summarization_tasks(self, all_locs):
        logger.info("adding summarization tasks to the DAG...")
        parallelism = ["measure_id", "year_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        for slices in d.index_slices(parallelism):
            for location_id in all_locs:
                summ_task = self._summarize_task_fac.get_task(
                    measure_id=slices[0], year_id=slices[1], location_id=location_id
                )
                self.workflow.add_task(summ_task)
        logger.info("...finished adding summarization tasks to the DAG")

    def _add_pct_change_tasks(self, all_locs):
        logger.info("adding pct change tasks to the DAG...")
        for measure_id in self.como_version.measure_id:
            for location_id in all_locs:
                pct_change_task = self._pct_change_task_fac.get_task(
                    measure_id=measure_id, location_id=location_id
                )
                self.workflow.add_task(pct_change_task)
        logger.info("...finished adding pct change tasks to the DAG")

    def add_tasks_to_dag(self, location_set_version_list, sorted_locs):
        logger.info("adding tasks to the DAG...")
        if self.como_version.minimum_incidence_cod:
            self._add_min_inc_cod_tasks()
        self._add_disability_weight_tasks()
        if measures.INCIDENCE in self.como_version.measure_id:
            self._add_incidence_tasks()
        self._add_simulation_input_tasks()
        self._add_simulation_tasks()
        self._add_loc_aggregation_tasks(location_set_version_list)
        self._add_summarization_tasks(sorted_locs)
        if self.como_version.change_years:
            self._add_pct_change_tasks(sorted_locs)
        self.internal_upload_names = []
        if self.como_version.internal_upload:
            self.internal_upload_names = self._add_internal_upload_tasks(sorted_locs)
        if self.como_version.public_upload:
            component_measure_list = list(
                itertools.product(
                    self.como_version.components, np.atleast_1d(self.como_version.measure_id)
                )
            )
            # Don't run public upload on sequelae, huge and rarely used
            component_measure_list = [
                (component, measure) for component, measure in component_measure_list
                if component != "sequela"
            ]
            self._add_public_sort_tasks(
                sorted_locs=sorted_locs, component_measure_list=component_measure_list
            )
            self._add_public_upload_tasks(
                sorted_locs=sorted_locs, component_measure_list=component_measure_list
            )
        logger.info("...finished adding tasks to the DAG")


def run_como(
    como_dir: Optional[str],
    release_id: int,
    gbd_process_version_note: str,
    location_set_id: int,
    nonfatal_cause_set_id: int,
    incidence_cause_set_id: int,
    impairment_rei_set_id: int,
    injury_rei_set_id: int,
    year_id: Optional[List[int]],
    measure_id: List[int],
    n_draws: int,
    n_simulants: int,
    components: List[str],
    change_years: List[Tuple[int]],
    agg_loc_sets: List[int],
    minimum_incidence_cod: bool,
    codcorrect_version: int,
    test_process_version: bool,
    internal_upload: bool,
    public_upload: bool,
    public_upload_test: bool,
    include_reporting_cause_set: bool,
    resume: bool,
    skip_sequela_summary: bool,
    task_template_concurrency: dict,
) -> None:
    """Workflow entry point.

    Note:
    - location set / list variations:
        - all_loc_sets = list of all location sets; default = [35]
        - agg_loc_sets = input list of aggregate locations; default = []
        - special_loc_sets = input list of aggregate locations excluding location_set_id; default = []
        - sorted_locs = list of most-detailed & aggregate location_ids, sorted in ascending order
        - location_set_version_list = list of all location set version ids
    """
    input_args = locals()
    configure_logging()

    logger.info("firing up COMO...")
    agg_loc_sets = agg_loc_sets if agg_loc_sets else list()
    special_loc_sets = set(agg_loc_sets) - {location_set_id}
    special_loc_sets = list(special_loc_sets)
    all_loc_sets = set(agg_loc_sets) | {location_set_id}
    all_loc_sets = list(all_loc_sets)
    logger.info("sets added...")

    if not codcorrect_version and not como_dir:
        logger.error(f"a codcorrect_version is required: {codcorrect_version}")
        raise ValueError("A codcorrect_version is required.")

    if year_id is None:
        year_id = estimation_years_from_release_id(release_id)
    change_years = change_years if change_years else list()

    valid_components = ["cause", "sequela", "injuries", "impairment"]
    if not (set(components).issubset(set(valid_components))):
        logger.error(
            f"not all components are valid {components}. Valid: {', '.join(valid_components)}."
        )
        raise ValueError(
            f"Not all components are valid {components}. Valid: {', '.join(valid_components)}."
        )

    if como_dir is not None:
        logger.info("reading comoVersion from cache...")
        cv = ComoVersion(como_dir)
        cv.load_cache()
    else:
        logger.info("creating comoVersion...")
        cv = ComoVersion.new(
            release_id=release_id,
            gbd_process_version_note=gbd_process_version_note,
            location_set_id=location_set_id,
            nonfatal_cause_set_id=nonfatal_cause_set_id,
            incidence_cause_set_id=incidence_cause_set_id,
            impairment_rei_set_id=impairment_rei_set_id,
            injury_rei_set_id=injury_rei_set_id,
            year_id=year_id,
            measure_id=measure_id,
            n_draws=n_draws,
            n_simulants=n_simulants,
            components=components,
            change_years=change_years,
            agg_loc_sets=special_loc_sets,
            codcorrect_version=codcorrect_version,
            internal_upload=internal_upload,
            public_upload=public_upload,
            public_upload_test=public_upload_test,
            include_reporting_cause_set=include_reporting_cause_set,
            skip_sequela_summary=skip_sequela_summary,
            minimum_incidence_cod=minimum_incidence_cod,
        )
    logger.info(f"ComoVersion: v{cv.como_version_id}")

    location_set_version_list = []
    for location_set_id in all_loc_sets:
        location_set_version_list.append(
            get_active_location_set_version(
                location_set_id=location_set_id,
                release_id=cv.release_id,
            )
        )

    sorted_locs = get_sorted_locations(como_dir = cv.como_dir)

    if not resume:
        logger.info("preparing internal and/or public DBs")
        if public_upload:
            public_upload_lib.configure_upload(como_version=cv)
        if internal_upload:
            pv = GBDProcessVersion(cv.gbd_process_version_id)
            pv.partition_tables_by_location(location_ids=sorted_locs)

    logger.info("adding tasks to comoWorkflow...")
    cwf = ComoWorkFlow(cv)
    cwf.add_tasks_to_dag(
        location_set_version_list=location_set_version_list,
        sorted_locs=sorted_locs,
    )

    logger.info("Setting task template concurrency.")
    for name in cwf.internal_upload_names:
        value = como_constants.INTERNAL_UPLOAD_CONCURRENCY
        cwf.workflow.set_task_template_max_concurrency_limit(
            task_template_name=name, limit=value
        )
        logger.info(f"Set task template {name} concurrency to {value}.")
    for name, value in task_template_concurrency.items():
        if "upload" in name:
            logger.warning(
                "Setting upload task template concurrency not allowed from the command line."
                f"Skipping {name}:{value} from task template concurrency input."
            )
            continue
        cwf.workflow.set_task_template_max_concurrency_limit(
            task_template_name=name, limit=value
        )
        logger.info(f"Set task template {name} concurrency to {value}.")

    # this call allows us to access the workflow_id before run()
    cwf.workflow.bind()

    try:
        logger.info("launching jobmon DAG...")
        git_branch = utils.get_como_branch()
        pv_status = "internal test" if test_process_version else "best"
        verb = "resuming" if resume else "launching"
        launch_message = (
            f"[COMO v{cv.como_version_id}]: {getpass.getuser()}\n\t"
            f"{verb} DAG on {CLUSTER_NAME}\n\t"
            f"- git branch: {git_branch}\n\t"
            f"- process version status: {pv_status}\n\t"
            f"- jobmon workflow ID: {cwf.workflow.workflow_id}\n\t"
            f"- jobmon GUI URL: {JOBMON_URL.format(workflow_id=cwf.workflow.workflow_id)}\n"
            f"*input arguments*:\n" 
        )
        launch_message += "\n".join([f"\t{k}: {v}" for k, v in input_args.items()])

        post_slack(launch_message)
        logger.info(launch_message)
        wfr = cwf.workflow.run(seconds_until_timeout=60 * 60 * 24 * 7, resume=True)
        success = wfr == WorkflowRunStatus.DONE

    except exceptions.WorkflowAlreadyComplete:
        logger.info("DAG already complete...")
        success = True

    if success:
        complete_message = (
            f"[COMO v{cv.como_version_id}]: jobmon DAG finished successfully"
        )
        post_slack(complete_message)
        logger.info(complete_message)

        if test_process_version:
            cv.mark_test()
        else:
            cv.mark_epi_best()
            cv.mark_best()
            logger.info("creating compare version...")
            cv.create_compare_version()
        if not public_upload_test:
            try:
                logger.info("generating cause scatter plots...")
                cv.generate_cause_scatters()
                compare_message = (
                    f"[COMO v{cv.como_version_id}]: finished generating cause scatter "
                    "plots"
                )
                post_slack(compare_message)
                logger.info(compare_message)
            except Exception as exc:
                failure_message = (
                    f"[COMO v{cv.como_version_id}]: compare version + scatter plot"
                    f" generation failed - {exc}"
                )
                post_slack(failure_message)
                logger.info(failure_message)
                raise

    else:
        logger.error("COMO failed")
        post_slack(f"[COMO v{cv.como_version_id}]: COMO failed")
        sys.exit(1)
    
    logger.info(f"minimum_incidence_cod: {minimum_incidence_cod}")
    if minimum_incidence_cod:
        print("New argument!")
        logger.info(f"minimum_incidence_cod: {minimum_incidence_cod}")
