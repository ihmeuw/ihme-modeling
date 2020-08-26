import getpass

from db_queries.core.location import active_location_set_version
from gbd.constants import measures, GBD_ROUND_ID
from gbd.estimation_years import estimation_years_from_gbd_round_id
from hierarchies.dbtrees import loctree
from jobmon.client.swarm.workflow.workflow import Workflow, WorkflowAlreadyComplete

from como.version import ComoVersion
from como.upload import run_upload
from como.tasks.incidence_task import IncidenceTaskFactory
from como.tasks.simulation_input_task import SimulationInputTaskFactory
from como.tasks.simulation_task import SimulationTaskFactory
from como.tasks.location_agg_task import LocationAggTaskFactory
from como.tasks.summarize_task import SummarizeTaskFactory
from como.tasks.pct_change_task import PctChangeTaskFactory


class ComoWorkFlow:

    def __init__(self, como_version, project):
        self.como_version = como_version

        # instantiate our factories
        self._task_registry = {}
        self._incidence_task_fac = IncidenceTaskFactory(
            self.como_version, self._task_registry)
        self._simulation_input_task_fac = SimulationInputTaskFactory(
            self.como_version, self._task_registry)
        self._simulation_task_fac = SimulationTaskFactory(
            self.como_version, self._task_registry)
        self._location_agg_task_fac = LocationAggTaskFactory(
            self.como_version, self._task_registry)
        self._summarize_task_fac = SummarizeTaskFactory(
            self.como_version, self._task_registry,
            self._location_agg_task_fac.agg_loc_set_map)
        self._pct_change_task_fac = PctChangeTaskFactory(
            self.como_version, self._task_registry,
            self._location_agg_task_fac.agg_loc_set_map)
        user = getpass.getuser()
        self.workflow = Workflow(
            workflow_args=f'como_v{self.como_version.como_version_id}',
            name=f'COMO v{self.como_version.como_version_id}',
            project=project,
            stderr=f'FILEPATH',
            stdout=f'FILEPATH',
            working_dir=f'FILEPATH',
            seconds_until_timeout=36000*8,
            reset_running_jobs=True,
            resume=True)

    def _add_incidence_tasks(self):
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            incidence_task = self._incidence_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                n_processes=20)
            self.workflow.add_task(incidence_task)

    def _add_simulation_input_tasks(self):
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            simulation_input_task = self._simulation_input_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                n_processes=23)
            self.workflow.add_task(simulation_input_task)

    def _add_simulation_tasks(self, n_simulants):
        parallelism = ["location_id", "sex_id", "year_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            sim_task = self._simulation_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                year_id=slices[2],
                n_simulants=n_simulants,
                n_processes=23)
            self.workflow.add_task(sim_task)

    def _add_loc_aggregation_tasks(self, agg_loc_set_versions):
        parallelism = ["year_id", "sex_id", "measure_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            for component in self.como_version.components:
                if not (component == "impairment" and slices[2] == measures.INCIDENCE):
                    for location_set_version_id in agg_loc_set_versions:
                        agg_task = self._location_agg_task_fac.get_task(
                            component=component,
                            year_id=slices[0],
                            sex_id=slices[1],
                            measure_id=slices[2],
                            location_set_version_id=location_set_version_id)
                        self.workflow.add_task(agg_task)

    def _add_summarization_tasks(self, agg_loc_set_versions):
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_trees = loctree(
                location_set_version_id=location_set_version_id,
                return_many=True)
            for loc_tree in loc_trees:
                all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))

        parallelism = ["measure_id", "year_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            for location_id in all_locs:
                summ_task = self._summarize_task_fac.get_task(
                    measure_id=slices[0],
                    year_id=slices[1],
                    location_id=location_id)
                self.workflow.add_task(summ_task)

    def _add_pct_change_tasks(self, agg_loc_set_versions):
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_trees = loctree(
                location_set_version_id=location_set_version_id,
                return_many=True)
            for loc_tree in loc_trees:
                all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))

        for measure_id in self.como_version.measure_id:
            for location_id in all_locs:
                pct_change_task = self._pct_change_task_fac.get_task(
                    measure_id=measure_id,
                    location_id=location_id)
                self.workflow.add_task(pct_change_task)

    def add_tasks_to_dag(self, n_simulants, agg_loc_sets):
        location_set_version_list = []
        for location_set_id in agg_loc_sets:
            location_set_version_list.append(
                active_location_set_version(
                    set_id=location_set_id,
                    gbd_round_id=self.como_version.gbd_round_id))

        if measures.INCIDENCE in self.como_version.measure_id:
            self._add_incidence_tasks()
        self._add_simulation_input_tasks()
        self._add_simulation_tasks(n_simulants)
        self._add_loc_aggregation_tasks(location_set_version_list)
        self._add_summarization_tasks(location_set_version_list)
        if self.como_version.change_years:
            self._add_pct_change_tasks(location_set_version_list)


def run_como(
        como_dir=None,
        root_dir="FILEPATH",
        gbd_round_id=GBD_ROUND_ID,
        decomp_step_id=None,
        location_set_id=89,
        year_id=None,
        measure_id=[measures.YLD, measures.PREVALENCE, measures.INCIDENCE],
        n_draws=1000,
        n_simulants=20000,
        components=["cause", "sequela", "impairment", "injuries"],
        change_years=[],
        agg_loc_sets=[],
        project="proj_como"):

    special_sets = agg_loc_sets
    all_sets = agg_loc_sets + location_set_id

    if year_id is None:
        year_id = estimation_years_from_gbd_round_id(gbd_round_id)
    change_years = change_years or list()

    if como_dir is not None:
        cv = ComoVersion(como_dir)
        cv.load_cache()
    else:
        cv = ComoVersion.new(root_dir=root_dir,
                             gbd_round_id=gbd_round_id,
                             decomp_step_id=decomp_step_id,
                             location_set_id=location_set_id,
                             year_id=year_id,
                             measure_id=measure_id,
                             n_draws=n_draws,
                             components=components,
                             change_years=change_years,
                             agg_loc_sets=special_sets)

    cwf = ComoWorkFlow(cv, project=project)
    cwf.add_tasks_to_dag(n_simulants=n_simulants, agg_loc_sets=all_sets)
    try:
        success = cwf.workflow.run()
    except WorkflowAlreadyComplete:
        success = 0
    if success == 0:
        # upload
        all_locs = []
        for loc_set in all_sets:
            loc_trees = loctree(location_set_id=loc_set,
                                gbd_round_id=cv.gbd_round_id,
                                return_many=True)
            for loc_tree in loc_trees:
                all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))
        run_upload(cv, all_locs)
        # mark best in epi db
        cv.mark_best()
        # mark active and create compare verison in gbd db
        cv.create_compare_version()
    else:
        raise RuntimeError("como unsuccessful")
