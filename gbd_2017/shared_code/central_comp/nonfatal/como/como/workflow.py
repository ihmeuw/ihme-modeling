
from hierarchies.dbtrees import loctree
from dataframe_io.io_queue import RedisServer
from db_queries.core.location import active_location_set_version
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow

from como.version import ComoVersion
from como.upload import run_upload
from como.tasks.incidence_task import IncidenceTaskFactory
from como.tasks.simulation_input_task import SimulationInputTaskFactory
from como.tasks.simulation_task import SimulationTaskFactory
from como.tasks.location_agg_task import LocationAggTaskFactory
from como.tasks.summarize_task import SummarizeTaskFactory
from como.tasks.pct_change_task import PctChangeTaskFactory


class ComoWorkFlow(object):

    def __init__(self, como_version, redis_server=None):
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

        self.dag = TaskDag(
            name="COMO {}".format(self.como_version.como_version_id))
        self.redis_server = redis_server

    def _add_incidence_tasks(self):
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            incidence_task = self._incidence_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                n_processes=20)
            self.dag.add_task(incidence_task)

    def _add_simulation_input_tasks(self):
        parallelism = ["location_id", "sex_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            simulation_input_task = self._simulation_input_task_fac.get_task(
                location_id=slices[0],
                sex_id=slices[1],
                n_processes=23)
            self.dag.add_task(simulation_input_task)

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
            self.dag.add_task(sim_task)

    def _add_loc_aggregation_tasks(self, agg_loc_set_versions):
        if self.redis_server is None:
            self.redis_server = RedisServer()
            self.redis_server.launch_redis_server()

        parallelism = ["year_id", "sex_id", "measure_id"]
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id)
        for slices in d.index_slices(parallelism):
            for component in self.como_version.components:
                if not (component == "impairment" and slices[2] == 6):
                    for location_set_version_id in agg_loc_set_versions:
                        agg_task = self._location_agg_task_fac.get_task(
                            component=component,
                            year_id=slices[0],
                            sex_id=slices[1],
                            measure_id=slices[2],
                            location_set_version_id=location_set_version_id,
                            redis_host=self.redis_server.hostname)
                        self.dag.add_task(agg_task)

    def _add_summarization_tasks(self, agg_loc_set_versions):
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_tree = loctree(location_set_version_id=location_set_version_id)
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
                self.dag.add_task(summ_task)

    def _add_pct_change_tasks(self, agg_loc_set_versions):
        all_locs = []
        for location_set_version_id in agg_loc_set_versions:
            loc_tree = loctree(location_set_version_id=location_set_version_id)
            all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))

        for measure_id in self.como_version.measure_id:
            for location_id in all_locs:
                pct_change_task = self._pct_change_task_fac.get_task(
                    measure_id=measure_id,
                    location_id=location_id)
                self.dag.add_task(pct_change_task)

    def add_tasks_to_dag(self, n_simulants=20000, agg_loc_sets=[35, 83]):
        location_set_version_list = []
        for location_set_id in agg_loc_sets:
            location_set_version_list.append(
                active_location_set_version(
                    set_id=location_set_id,
                    gbd_round_id=self.como_version.gbd_round_id))

        if 6 in self.como_version.measure_id:
            self._add_incidence_tasks()
        self._add_simulation_input_tasks()
        self._add_simulation_tasks(n_simulants)
        self._add_loc_aggregation_tasks(location_set_version_list)
        self._add_summarization_tasks(location_set_version_list)
        if self.como_version.change_years:
            self._add_pct_change_tasks(location_set_version_list)

    def run_workflow(self, project="proj_como"):
        wf = Workflow(
            self.dag,
            workflow_args=self.como_version.como_dir,
            project=project)
        success = wf.run()
        self.redis_server.kill_redis_server()
        return success


def run_como(
        como_dir=None,
        root_dir="FILEPATH",
        gbd_round_id=5,
        location_set_id=35,
        year_id=list(range(1990, 2018)),
        measure_id=[3, 5, 6],
        n_draws=1000,
        n_simulants=20000,
        components=["cause", "sequela", "injuries", "impairment"],
        change_years=[(1990, 2007), (2007, 2017), (1990, 2017)],
        agg_loc_sets=[35, 83],
        project="proj_como"):

    special_sets = set(agg_loc_sets) - set([location_set_id])
    all_sets = set(agg_loc_sets) | set([location_set_id])

    if como_dir is not None:
        cv = ComoVersion(como_dir)
        cv.load_cache()
    else:
        cv = ComoVersion.new(
            root_dir, gbd_round_id, location_set_id, year_id, measure_id,
            n_draws, components, change_years, special_sets)

    cwf = ComoWorkFlow(cv)
    cwf.add_tasks_to_dag(n_simulants=n_simulants, agg_loc_sets=all_sets)
    if cwf.run_workflow(project=project):
        all_locs = []
        for location_set_id in all_sets:
            loc_tree = loctree(location_set_id=location_set_id,
                               gbd_round_id=cv.gbd_round_id)
            all_locs.extend(loc_tree.node_ids)
        all_locs = list(set(all_locs))
        run_upload(cv, all_locs)
    else:
        raise RuntimeError("como unsuccessful")
