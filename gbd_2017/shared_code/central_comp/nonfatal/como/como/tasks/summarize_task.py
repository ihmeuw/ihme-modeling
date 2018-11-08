import os
import argparse
from multiprocessing import Queue, Process

from jobmon.workflow.python_task import PythonTask

from como.version import ComoVersion
from como.common import name_task, ExceptionWrapper
from como.summarize import ComoSummaries
from como.tasks.incidence_task import IncidenceTaskFactory
from como.tasks.simulation_task import SimulationTaskFactory
from como.tasks.location_agg_task import LocationAggTaskFactory

this_file = os.path.realpath(__file__)


class SummarizeTaskFactory(object):

    def __init__(self, como_version, task_registry, agg_loc_set_map):
        self.como_version = como_version
        self.task_registry = task_registry
        self.agg_loc_set_map = agg_loc_set_map

    @staticmethod
    def get_task_name(measure_id, year_id, location_id):
        return name_task("como_summ", {"measure_id": measure_id,
                                       "year_id": year_id,
                                       "location_id": location_id})

    def get_task(self, measure_id, year_id, location_id):
        upstream_tasks = []
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            measure_id)

        # if location is not most detailed then its dependent on loc agg
        if location_id not in d.index_dim.get_level("location_id"):
            location_set_version_id = self.agg_loc_set_map[location_id]
            for component in self.como_version.components:
                for sex_id in [1, 2]:
                    if not (component == "impairment" and measure_id == 6):
                        task_name = LocationAggTaskFactory.get_task_name(
                            component=component,
                            year_id=year_id,
                            sex_id=sex_id,
                            measure_id=measure_id,
                            location_set_version_id=location_set_version_id)
                    upstream_tasks.append(self.task_registry[task_name])
        # otherwise it is dependent on simulation tasks or incidence
        else:
            if measure_id == 6:
                for sex_id in [1, 2]:
                    task_name = IncidenceTaskFactory.get_task_name(
                        location_id=location_id,
                        sex_id=sex_id)
                    upstream_tasks.append(self.task_registry[task_name])
            else:
                for sex_id in [1, 2]:
                    task_name = SimulationTaskFactory.get_task_name(
                        location_id=location_id,
                        sex_id=sex_id,
                        year_id=year_id)
                    upstream_tasks.append(self.task_registry[task_name])

        name = self.get_task_name(measure_id, year_id, location_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--como_dir", self.como_version.como_dir,
                "--measure_id", measure_id,
                "--year_id", year_id,
                "--location_id", location_id
            ],
            name=name,
            upstream_tasks=upstream_tasks,
            slots=25,
            mem_free=100,
            max_attempts=5,
            max_runtime=(60 * 60 * 3),
            tag="summary")
        self.task_registry[name] = task
        return task


class SummarizeTask(ComoSummaries):

    def compute_and_export_single_component_summaries(self, component):
        # skips
        if component == "impairment" and self.measure_id == 6:
            return

        self.age_sex_agg_single_component(component)
        print("metric converting: {}".format(component))
        self.metric_convert_single_component(component)

        print("computing single year estimates: {}".format(component))
        df = self.estimate_single_component(component)
        df.fillna(value=r'\N', inplace=True)
        self.export_summary(component, "single_year", df)

        # print("computing multi year estimates: {}".format(component))
        # df = self.percent_change_single_component(component)
        # df.fillna(value=r'\N', inplace=True)
        # self.export_summary(component, "multi_year", df)

    def _q_execute(self, method, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                method(*params)
                out_q.put((False, params))
            except Exception as e:
                out_q.put((ExceptionWrapper(e), params))

    def run_task(self, n_processes=3):
        self.age_sex_agg_single_component("cause")

        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # figure out how many processes we need
        summ_procs = []
        min_procs = min(
            [n_processes,
             len([c for c in self.components if c != "cause"])])
        for i in range(min_procs):

            # run summarization and export results in parallel
            p = Process(
                target=self._q_execute,
                args=(self.compute_and_export_single_component_summaries,
                      inq,
                      outq))
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
        df = self.estimate_single_component("cause")
        df.fillna(value=r'\N', inplace=True)
        self.export_summary("cause", "single_year", df)
        # df = self.percent_change_single_component("cause")
        # df.fillna(value=r'\N', inplace=True)
        # self.export_summary("cause", "multi_year", df)

        # get results
        results = []
        for _ in [c for c in self.components if c != "cause"]:
            proc_result = outq.get()
            results.append(proc_result)

        # check for errors
        for result in results:
            if result[0]:
                print(result[1:])
                raise result[0].re_raise()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="collect sequela inputs and move them")
    parser.add_argument(
        "--como_dir",
        type=str,
        help="directory of como run")
    parser.add_argument(
        "--measure_id",
        type=int,
        default=[],
        help="location_ids to include in this run")
    parser.add_argument(
        "--year_id",
        nargs='*',
        type=int,
        default=[],
        help="location_ids to include in this run")
    parser.add_argument(
        "--location_id",
        nargs='*',
        type=int,
        default=[],
        help="location_ids to include in this run")
    parser.add_argument(
        "--n_processes",
        type=int,
        default=3,
        help="how many subprocesses to use")
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    print(args.como_dir, args.measure_id, args.year_id, args.location_id,
          args.n_processes)
    task = SummarizeTask(cv, args.measure_id, args.year_id, args.location_id)
    task.run_task(args.n_processes)
