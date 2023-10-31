import os
import sys
import argparse
from loguru import logger
from multiprocessing import Queue, Process

from gbd.constants import measures, sex
from jobmon.client.api import ExecutorParameters

from como.legacy.version import ComoVersion
from como.legacy.common import name_task, ExceptionWrapper
from como.legacy.summarize import ComoSummaries
from como.legacy.tasks.incidence_task import IncidenceTaskFactory
from como.legacy.tasks.simulation_task import SimulationTaskFactory
from como.legacy.tasks.location_agg_task import LocationAggTaskFactory

THIS_FILE = os.path.realpath(__file__)


class SummarizeTaskFactory:
    def __init__(self, como_version, task_registry, agg_loc_set_map, tool):
        self.como_version = como_version
        self.task_registry = task_registry
        self.agg_loc_set_map = agg_loc_set_map

        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--measure_id {measure_id} "
            "--year_id {year_id} "
        )
        self.task_template = tool.get_task_template(
            template_name="como_summ",
            command_template=command_template,
            node_args=[
                "measure_id",
                "year_id",
                "location_id",
            ],
            task_args=[
                "como_dir",
            ],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(measure_id, year_id, location_id):
        return name_task(
            "como_summ",
            {"measure_id": measure_id, "year_id": year_id, "location_id": location_id},
        )

    def get_task(self, measure_id, year_id, location_id):
        upstream_tasks = []
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(measure_id)

        if location_id not in d.index_dim.get_level("location_id"):
            location_set_version_id = self.agg_loc_set_map[location_id]
            for component in self.como_version.components:
                for sex_id in [sex.MALE, sex.FEMALE]:
                    if not (component == "impairment" and measure_id == measures.INCIDENCE):
                        task_name = LocationAggTaskFactory.get_task_name(
                            component=component,
                            year_id=year_id,
                            sex_id=sex_id,
                            measure_id=measure_id,
                            location_set_version_id=location_set_version_id,
                        )
                    upstream_tasks.append(self.task_registry[task_name])
        else:
            if measure_id == measures.INCIDENCE:
                for sex_id in [sex.MALE, sex.FEMALE]:
                    task_name = IncidenceTaskFactory.get_task_name(
                        location_id=location_id, sex_id=sex_id
                    )
                    upstream_tasks.append(self.task_registry[task_name])
            else:
                for sex_id in [sex.MALE, sex.FEMALE]:
                    task_name = SimulationTaskFactory.get_task_name(
                        location_id=location_id, sex_id=sex_id, year_id=year_id
                    )
                    upstream_tasks.append(self.task_registry[task_name])

        name = self.get_task_name(measure_id, year_id, location_id)
        exec_params = ExecutorParameters(
            executor_class="SGEExecutor",
            num_cores=25,
            m_mem_free="50G",
            max_runtime_seconds=(60 * 60 * 3),
            queue="all.q",
        )
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            year_id=year_id,
            measure_id=measure_id,
            name=name,
            max_attempts=5,
            executor_parameters=exec_params,
            upstream_tasks=upstream_tasks,
        )

        self.task_registry[name] = task
        return task


class SummarizeTask(ComoSummaries):
    def compute_and_export_single_component_summaries(self, component):
        if component == "impairment" and self.measure_id == measures.INCIDENCE:
            return

        self.age_sex_agg_single_component(component)
        logger.info("metric converting: {}".format(component))
        self.metric_convert_single_component(component)

        logger.info("computing single year estimates: {}".format(component))
        df = self.estimate_single_component(component)
        df.fillna(value=r"\N", inplace=True)
        self.export_summary(component, "single_year", df)

    def _q_execute(self, method, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                method(*params)
                out_q.put((False, params))
            except Exception as e:
                logger.error(f"q_execute({method}, {in_q}, {out_q}); exception: {e}")
                out_q.put((ExceptionWrapper(e), params))

    def run_task(self, n_processes=3):
        self.age_sex_agg_single_component("cause")

        inq = Queue()
        outq = Queue()

        summ_procs = []
        min_procs = min([n_processes, len([c for c in self.components if c != "cause"])])
        for i in range(min_procs):

            p = Process(
                target=self._q_execute,
                args=(self.compute_and_export_single_component_summaries, inq, outq),
            )
            summ_procs.append(p)
            p.start()

        for component in [c for c in self.components if c != "cause"]:
            inq.put((component,))

        for _ in summ_procs:
            inq.put(None)

        self.metric_convert_single_component("cause")
        df = self.estimate_single_component("cause")
        df.fillna(value=r"\N", inplace=True)
        self.export_summary("cause", "single_year", df)

        results = []
        for _ in [c for c in self.components if c != "cause"]:
            proc_result = outq.get()
            results.append(proc_result)

        for result in results:
            if result[0]:
                logger.info(f"result: {result[1:]}")
                raise result[0].re_raise()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect sequela inputs and move them")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument(
        "--measure_id", type=int, default=[], help="location_ids to include in this run"
    )
    parser.add_argument(
        "--year_id",
        nargs="*",
        type=int,
        default=[],
        help="location_ids to include in this run",
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
    logger.info(
        args.como_dir, args.measure_id, args.year_id, args.location_id, args.n_processes
    )
    task = SummarizeTask(cv, args.measure_id, args.year_id, args.location_id)
    task.run_task(args.n_processes)
