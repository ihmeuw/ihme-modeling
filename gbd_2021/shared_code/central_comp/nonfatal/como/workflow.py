import getpass
import sys
from loguru import logger
from typing import List, Optional

from db_queries.api.internal import get_active_location_set_version
from gbd.constants import measures
from gbd import decomp_step as gbd_decomp_step
from gbd.estimation_years import estimation_years_from_gbd_round_id
from hierarchies.dbtrees import loctree
from jobmon.client.api import Tool
from jobmon.constants import WorkflowRunStatus
from jobmon import exceptions

from como.legacy.version import ComoVersion
from como.legacy.upload import run_upload
from como.legacy.tasks.disability_weight_task import DisabilityWeightTaskFactory
from como.legacy.tasks.incidence_task import IncidenceTaskFactory
from como.legacy.tasks.simulation_input_task import SimulationInputTaskFactory
from como.legacy.tasks.simulation_task import SimulationTaskFactory
from como.legacy.tasks.location_agg_task import LocationAggTaskFactory
from como.legacy.tasks.summarize_task import SummarizeTaskFactory
from como.legacy.tasks.pct_change_task import PctChangeTaskFactory


class ComoWorkFlow:
    def __init__(self, como_version):
        self.como_version = como_version
        self.tool = Tool.create_tool("como")

        self._task_registry = {}
        self._disability_weight_task_fac = DisabilityWeightTaskFactory(
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
        )
        self.workflow.set_executor(
            project="proj_como",
            stderr=f"FILEPATH",
            stdout=f"FILEPATH",
        )

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

    def _add_simulation_tasks(self, n_simulants):
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
                n_simulants=n_simulants,
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
        for slices in d.index_slices(parallelism):
            for component in self.como_version.components:
                if not (component == "impairment" and slices[2] == measures.INCIDENCE):
                    for location_set_version_id in agg_loc_set_versions:
                        agg_task = self._location_agg_task_fac.get_task(
                            component=component,
                            year_id=slices[0],
                            sex_id=slices[1],
                            measure_id=slices[2],
                            location_set_version_id=location_set_version_id,
                        )
                        self.workflow.add_task(agg_task)
        logger.info("...finished adding loc aggregation tasks to DAG")

    def _add_summarization_tasks(self, agg_loc_set_versions):
        logger.info("adding summarization tasks to the DAG...")
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_trees = loctree(
                location_set_version_id=location_set_version_id, 
                gbd_round_id=self.como_version.gbd_round_id,
                return_many=True,
            )
            for loc_tree in loc_trees:
                all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))

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

    def _add_pct_change_tasks(self, agg_loc_set_versions):
        logger.info("adding pct change tasks to the DAG...")
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_trees = loctree(
                location_set_version_id=location_set_version_id,
                gbd_round_id=self.como_version.gbd_round_id,
                return_many=True,
            )
            for loc_tree in loc_trees:
                all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))

        for measure_id in self.como_version.measure_id:
            for location_id in all_locs:
                pct_change_task = self._pct_change_task_fac.get_task(
                    measure_id=measure_id, location_id=location_id
                )
                self.workflow.add_task(pct_change_task)
        logger.info("...finished adding pct change tasks to the DAG")

    def add_tasks_to_dag(self, n_simulants, agg_loc_sets):
        logger.info("adding tasks to the DAG...")
        location_set_version_list = []
        for location_set_id in agg_loc_sets:
            location_set_version_list.append(
                get_active_location_set_version(
                    location_set_id=location_set_id,
                    gbd_round_id=self.como_version.gbd_round_id,
                    decomp_step=gbd_decomp_step.decomp_step_from_decomp_step_id(
                        self.como_version.decomp_step_id
                    ),
                )
            )

        self._add_disability_weight_tasks()
        if measures.INCIDENCE in self.como_version.measure_id:
            self._add_incidence_tasks()
        self._add_simulation_input_tasks()
        self._add_simulation_tasks(n_simulants)
        self._add_loc_aggregation_tasks(location_set_version_list)
        self._add_summarization_tasks(location_set_version_list)
        if self.como_version.change_years:
            self._add_pct_change_tasks(location_set_version_list)
        logger.info("...finished adding tasks to the DAG")


def run_como(
    como_dir: Optional[str],
    gbd_round_id: int,
    decomp_step: Optional[str],
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
    change_years: List[int],
    agg_loc_sets: List[int],
    codcorrect_version: int,
    upload: bool,
):
    """
    Workflow entry point.
    """
    agg_loc_sets = agg_loc_sets if agg_loc_sets else list()
    special_sets = set(agg_loc_sets) - {location_set_id}
    special_sets = list(special_sets)
    all_sets = set(agg_loc_sets) | {location_set_id}
    all_sets = list(all_sets)

    if not codcorrect_version and not como_dir:
        logger.error(f"a codcorrect_version is required: {codcorrect_version}")
        raise ValueError("A codcorrect_version is required.")

    if year_id is None:
        year_id = estimation_years_from_gbd_round_id(gbd_round_id)
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
        decomp_step_id = gbd_decomp_step.decomp_step_id_from_decomp_step(
            step=decomp_step, gbd_round_id=gbd_round_id
        )
        cv = ComoVersion.new(
            gbd_round_id=gbd_round_id,
            decomp_step_id=decomp_step_id,
            location_set_id=location_set_id,
            nonfatal_cause_set_id=nonfatal_cause_set_id,
            incidence_cause_set_id=incidence_cause_set_id,
            impairment_rei_set_id=impairment_rei_set_id,
            injury_rei_set_id=injury_rei_set_id,
            year_id=year_id,
            measure_id=measure_id,
            n_draws=n_draws,
            components=components,
            change_years=change_years,
            agg_loc_sets=special_sets,
            codcorrect_version=codcorrect_version,
        )
    logger.info(f"ComoVersion: {cv}")

    logger.info("adding tasks to comoWorkflow...")
    cwf = ComoWorkFlow(cv)
    cwf.add_tasks_to_dag(n_simulants=n_simulants, agg_loc_sets=all_sets)

    try:
        wfr = cwf.workflow.run(seconds_until_timeout=60 * 60 * 24 * 7, resume=True)
        success = wfr.status == WorkflowRunStatus.DONE
    except exceptions.WorkflowAlreadyComplete:
        logger.info("DAG already complete...")
        success = True
    if success:
        if upload:
            logger.info("uploading results...")
            all_locs = []
            for loc_set in all_sets:
                loc_tree = loctree(
                    location_set_id=loc_set,
                    gbd_round_id=cv.gbd_round_id,
                    decomp_step=decomp_step,
                )
                all_locs.extend(loc_tree.node_ids)
            all_locs = list(set(all_locs))
            run_upload(cv, all_locs)
            cv.create_compare_version()
        else:
            logger.info("skipping upload ...")

        cv.mark_best()
    else:
        logger.error("COMO failed")
