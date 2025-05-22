import os
import sys
import argparse
from loguru import logger
from multiprocessing import Queue, Process

from gbd.constants import measures, sex

from como.lib.version import ComoVersion
from como.legacy.common import name_task, ExceptionWrapper
from como.legacy.summarize import ComoSummaries
from como.lib.tasks.incidence_task import IncidenceTaskFactory
from como.legacy.tasks.simulation_task import SimulationTaskFactory
from como.legacy.tasks.location_agg_task import LocationAggTaskFactory

from como.lib.constants import YearType

THIS_FILE = os.path.realpath(__file__)


class PctChangeTaskFactory:
    def __init__(self, como_version, task_registry, agg_loc_set_map, tool):
        self.como_version = como_version
        self.task_registry = task_registry
        self.agg_loc_set_map = agg_loc_set_map
        self._year_set = {
            item for sublist in self.como_version.change_years for item in sublist
        }
        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--measure_id {measure_id} "
        )
        self.task_template = tool.get_task_template(
            template_name="como_pct_change",
            command_template=command_template,
            node_args=[
                "measure_id",
                "location_id",
            ],
            task_args=["como_dir"],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(measure_id, location_id):
        return name_task("como_pct_change", {"measure_id": measure_id, "location_id": location_id})

    def get_task(self, measure_id, location_id):
        upsteam_tasks = []
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(measure_id)

        # if location is not most detailed then its dependent on loc agg
        if location_id not in d.index_dim.get_level("location_id"):
            location_set_version_id = self.agg_loc_set_map[location_id]
            for component in self.como_version.components:
                for year_id in self._year_set:
                    for sex_id in [sex.MALE, sex.FEMALE]:
                        if not (
                            component == "impairment" and measure_id == measures.INCIDENCE
                        ):
                            task_name = LocationAggTaskFactory.get_task_name(
                                component=component,
                                year_id=year_id,
                                sex_id=sex_id,
                                measure_id=measure_id,
                                location_set_version_id=location_set_version_id,
                            )
                            upsteam_tasks.append(self.task_registry[task_name])
        # otherwise it is dependent on simulation tasks or incidence
        else:
            if measure_id == measures.INCIDENCE:
                for sex_id in [sex.MALE, sex.FEMALE]:
                    task_name = IncidenceTaskFactory.get_task_name(
                        location_id=location_id, sex_id=sex_id
                    )
                    upsteam_tasks.append(self.task_registry[task_name])
            else:
                for year_id in self._year_set:
                    for sex_id in [sex.MALE, sex.FEMALE]:
                        task_name = SimulationTaskFactory.get_task_name(
                            location_id=location_id, sex_id=sex_id, year_id=year_id
                        )
                        upsteam_tasks.append(self.task_registry[task_name])

        # make task
        name = self.get_task_name(measure_id, location_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            measure_id=measure_id,
            name=name,
            compute_resources={
                "cores": 25,
                "memory": "300G",
                "runtime": "6h",
            },
            upstream_tasks=upsteam_tasks,
        )
        self.task_registry[name] = task
        return task


class PctChangeTask(ComoSummaries):
    def __init__(self, como_version, measure_id, location_id):

        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions
        self.measure_id = measure_id

        self.dimensions.simulation_index["year_id"] = {
            item for sublist in self.como_version.change_years for item in sublist
        }
        self.dimensions.simulation_index["location_id"] = location_id

        # inits
        self.io_mock = {}
        self._population = None
        self._std_age_weights = None

    def compute_and_export_single_component_summaries(self, component):
        # skips
        if component == "impairment" and self.measure_id == measures.INCIDENCE:
            return

        self.age_sex_agg_single_component(component)
        logger.info("metric converting: {}".format(component))
        self.metric_convert_single_component(component)

        logger.info("computing multi year estimates: {}".format(component))
        df = self.percent_change_single_component(component)
        df.fillna(value=r"\N", inplace=True)
        self.export_summary(component, YearType.MULTI.value, df)

    def _q_execute(self, method, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                method(*params)
                out_q.put((False, params))
            except Exception as e:
                logger.error(
                    f"q_execute({method}, {in_q}, {out_q}); params: {params}. Exception: {e}"
                )
                out_q.put((ExceptionWrapper(e), params))

    def run_task(self, n_processes=3):
        self.age_sex_agg_single_component("cause")

        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # figure out how many processes we need
        summ_procs = []
        min_procs = min([n_processes, len([c for c in self.components if c != "cause"])])
        for _ in range(min_procs):

            # run summarization and export results in parallel
            p = Process(
                target=self._q_execute,
                args=(self.compute_and_export_single_component_summaries, inq, outq),
            )
            summ_procs.append(p)
            p.start()

        # create summaries
        for component in [c for c in self.components if c != "cause"]:
            inq.put((component,))

        # make the workers die after
        for _ in summ_procs:
            inq.put(None)

        # finish up causes in the main process
        self.metric_convert_single_component("cause")
        df = self.percent_change_single_component("cause")
        df.fillna(value=r"\N", inplace=True)
        self.export_summary("cause", YearType.MULTI.value, df)

        # get results
        results = []
        for _ in [c for c in self.components if c != "cause"]:
            proc_result = outq.get()
            results.append(proc_result)

        # check for errors
        for result, params in results:
            if isinstance(result, ExceptionWrapper):
                logger.info(f"result: {params}")
                raise result.re_raise()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect sequela inputs and move them")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument(
        "--measure_id", type=int, default=[], help="location_ids to include in this run"
    )
    parser.add_argument(
        "--location_id",
        nargs="*",
        type=int,
        default=[],
        help="location_ids to include in this run",
    )
    parser.add_argument(
        "--n_processes", type=int, default=3, help="how many subprocesses to use"
    )
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    task = PctChangeTask(cv, args.measure_id, args.location_id)
    task.run_task(args.n_processes)
