import argparse
import pathlib
import sys
from typing import Dict, List

import networkx as nx

from gbd import constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task

from epic.legacy.util.constants import DAG, Params
from epic.lib.logic import ex_adjust
from epic.lib.util.common import get_upstreams, name_task, startup_jitter

_THIS_FILE = pathlib.Path(__file__).resolve()


def parse_arguments() -> argparse.Namespace:
    """Parse exclusivity adjustment task arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--process_name",
        help="process to pull out of json style string map of ops",
        required=True,
    )
    parser.add_argument("--output_dir", help="root directory to save stuff", required=True)
    parser.add_argument(
        "--release_id",
        type=int,
        help="id for the release to adjust",
        default=gbd_constants.RELEASE_ID,
    )
    parser.add_argument(
        "--location_id", type=int, nargs="*", help="location(s) to adjust", default=None
    )
    parser.add_argument(
        "--year_id", type=int, nargs="*", help="year(s) to adjust", default=None
    )
    parser.add_argument(
        "--age_group_id", type=int, nargs="*", help="age group(s) to adjust", default=None
    )
    parser.add_argument(
        "--sex_id", type=int, nargs="*", help="sex(es) to adjust", default=None
    )
    parser.add_argument(
        "--measure_id", type=int, nargs="*", help="measure_id(s) to adjust", default=None
    )
    parser.add_argument(
        "--location_set_id",
        type=int,
        help="the location_set_id to use if location_id is None",
        default=35,
    )
    parser.add_argument(
        "--n_draws",
        type=int,
        help="number of draws on which to run the adjustment",
        default=1000,
    )
    parser.add_argument(
        "--annual",
        type=int,
        help="Whether annual draws should be pulled, if available",
        default=0,
    )

    return parser.parse_args()


class ExAdjustFactory(object):
    """Exclusivity adjustment task creation class."""

    def __init__(self, task_registry: Dict[str, Task], tool: Tool) -> None:
        self.task_registry = task_registry
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.EX_ADJUST,
            command_template=(
                "{python} {script} --process_name {process_name} --output_dir {output_dir} "
                "--release_id {release_id} "
                "--year_id {year_id} --n_draws {n_draws} --annual {annual}"
            ),
            node_args=[
                Params.PROCESS_NAME,
                Params.OUTPUT_DIR,
                Params.RELEASE_ID,
                Params.YEAR_ID,
                Params.N_DRAWS,
                Params.ANNUAL,
            ],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(node: str) -> str:
        """Returns task name given node name."""
        # The exclusivity adjustment tasks are not parallelized by
        # demographics so we just need to pass an empty dictionary
        return name_task(node, {})

    def get_task(
        self,
        node: str,
        process_graph: nx.DiGraph,
        output_dir: str,
        release_id: int,
        year_id: List[int],
        n_draws: int,
        annual: bool,
    ) -> Task:
        """Returns an EPIC exclusivity adjustment task given task args."""
        # make task
        upstream_tasks = get_upstreams(node, process_graph, self.task_registry)
        node_args = {
            Params.PROCESS_NAME: node,
            Params.OUTPUT_DIR: output_dir,
            Params.RELEASE_ID: release_id,
            Params.YEAR_ID: " ".join([str(x) for x in year_id]),
            Params.N_DRAWS: str(n_draws),
            Params.ANNUAL: int(annual),
        }
        compute_resources = {
            DAG.ArgNames.MEMORY: "50Gb",
            DAG.ArgNames.NUM_CORES: 12,
            DAG.ArgNames.RUNTIME: f"{60 * 60 * 3}s",  # 3 hours
        }
        task = self.task_template.create_task(
            script=_THIS_FILE,
            compute_resources=compute_resources,
            fallback_queues=DAG.Workflow.FALLBACK_QUEUES,
            upstream_tasks=upstream_tasks,
            max_attempts=DAG.Workflow.MAX_ATTEMPTS,
            name=self.get_task_name(node),
            python=self.python,
            **node_args,
        )
        return task


if __name__ == "__main__":
    """Entry point to call ex_adjust.ExAdjust from ex_adjust.py"""
    args = parse_arguments()
    startup_jitter()
    adjuster = ex_adjust.ExAdjust(
        process_name=args.process_name,
        output_dir=args.output_dir,
        release_id=args.release_id,
        location_id=args.location_id,
        year_id=args.year_id,
        age_group_id=args.age_group_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        location_set_id=args.location_set_id,
        n_draws=args.n_draws,
        annual=bool(args.annual),
    )
    adjuster.run_all_adjustments_mp()
