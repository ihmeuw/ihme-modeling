import argparse
import os
import sys
from multiprocessing import Process, Queue
from typing import Callable, Dict

from loguru import logger

from gbd.constants import measures, sex
from jobmon.client.api import Tool
from jobmon.client.task import Task

from como.legacy.common import ExceptionWrapper, name_task
from como.legacy.summarize import ComoSummaries
from como.legacy.tasks.location_agg_task import LocationAggTaskFactory
from como.legacy.tasks.simulation_task import SimulationTaskFactory
from como.lib import constants as como_constants
from como.lib.tasks.incidence_task import IncidenceTaskFactory
from como.lib.version import ComoVersion

THIS_FILE = os.path.realpath(__file__)


class SummarizeTaskFactory:
    """Generates a series of summarize tasks."""

    def __init__(
        self,
        como_version: ComoVersion,
        task_registry: Dict[str, Task],
        agg_loc_set_map: Dict[int, int],
        tool: Tool,
    ) -> None:
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
            node_args=["measure_id", "year_id", "location_id"],
            task_args=["como_dir"],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(measure_id: int, year_id: int, location_id: int) -> str:
        """Get task name based on parameters."""
        return name_task(
            "como_summ",
            {"measure_id": measure_id, "year_id": year_id, "location_id": location_id},
        )

    def get_task(self, measure_id: int, year_id: int, location_id: int) -> Task:
        """Create a task with upstream dependencies, based on measure, year, and
        location.
        """
        upstream_tasks = []
        d = self.como_version.nonfatal_dimensions.get_simulation_dimensions(measure_id)

        # if location is not most detailed then its dependent on loc agg
        if location_id not in d.index_dim.get_level("location_id"):
            location_set_version_id = self.agg_loc_set_map[location_id]
            for component in self.como_version.components:
                if (
                    component == como_constants.Component.IMPAIRMENT.value
                    and measure_id == measures.INCIDENCE
                ) or (
                    component == como_constants.Component.SEQUELA.value
                    and self.como_version.skip_sequela_summary
                ):
                    continue
                for sex_id in [sex.MALE, sex.FEMALE]:
                    task_name = LocationAggTaskFactory.get_task_name(
                        component=component,
                        year_id=year_id,
                        sex_id=sex_id,
                        measure_id=measure_id,
                        location_set_version_id=location_set_version_id,
                    )
                    upstream_tasks.append(self.task_registry[task_name])
        # otherwise it is dependent on simulation tasks or incidence
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

        # make task
        name = self.get_task_name(measure_id, year_id, location_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            year_id=year_id,
            measure_id=measure_id,
            name=name,
            compute_resources={
                "cores": 25,
                "memory": "50G",  # Note: allocate ~5G/year
                "runtime": "3h",
            },
            upstream_tasks=upstream_tasks,
        )

        self.task_registry[name] = task
        return task


class SummarizeTask(ComoSummaries):
    """Task for summarizing results by component (injuries, cause, etc.)."""

    def compute_and_export_single_component_summaries(self, component: str) -> None:
        """Per-component execution of summary task."""
        if (
            component == como_constants.Component.IMPAIRMENT.value
            and self.measure_id == measures.INCIDENCE
        ) or (
            component == como_constants.Component.SEQUELA.value
            and self.como_version.skip_sequela_summary
        ):
            return

        self.age_sex_agg_single_component(component)
        logger.info("metric converting: {}".format(component))
        self.metric_convert_single_component(component)

        logger.info("computing single year estimates: {}".format(component))
        df = self.estimate_single_component(component)
        df.fillna(value=r"\N", inplace=True)
        self.export_summary(component, como_constants.YearType.SINGLE.value, df)

    def _q_execute(self, method: Callable, in_q: Queue, out_q: Queue) -> None:
        """Execute method for process queue."""
        for params in iter(in_q.get, None):
            try:
                method(*params)
                out_q.put((False, params))
            except Exception as e:
                logger.error(f"q_execute({method}, {in_q}, {out_q}); exception: {e}")
                out_q.put((ExceptionWrapper(e), params))

    def run_task(self, n_processes: int = 3) -> None:
        """Execute the summarize task."""
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
        df = self.estimate_single_component("cause")
        df.fillna(value=r"\N", inplace=True)
        self.export_summary("cause", como_constants.YearType.SINGLE.value, df)

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
