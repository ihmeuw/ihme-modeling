import argparse
import os
import sys
from typing import Dict, List, Union

import networkx as nx

from gbd import constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task

from epic.legacy.util.constants import DAG, Params
from epic.lib.logic import modeled_sev_split
from epic.lib.util.common import get_upstreams, name_task, startup_jitter

_THIS_FILE = os.path.realpath(__file__)


def parse_arguments() -> argparse.Namespace:
    """Parse modeled sev split task arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument("--parent_id", type=int, required=True)
    parser.add_argument("--child_id", type=int, required=True)
    parser.add_argument("--proportion_id", type=int, required=True)
    parser.add_argument(
        "--output_dir", type=str, required=True, help="root directory to save stuff"
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
        "--proportion_measure_id",
        type=int,
        nargs="*",
        help="proportion measure_id(s) to adjust",
        default=None,
    )
    parser.add_argument(
        "--location_set_id",
        type=int,
        help="the location_set_id to use if location_id is None",
        default=35,
    )
    parser.add_argument(
        "--release_id",
        type=int,
        help="id for the release to adjust",
        default=gbd_constants.RELEASE_ID,
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


class ModeledSevSplitTaskFactory(object):
    """Modeled severity split task creation class."""

    def __init__(self, task_registry: Dict[str, Task], tool: Tool) -> None:
        self.task_registry = task_registry
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.MODELED_PROPORTION,
            command_template=(
                "{python} {script} --parent_id {parent_id} --child_id {child_id} "
                "--proportion_id {proportion_id} --output_dir {output_dir} "
                "--release_id {release_id} "
                "--year_id {year_id} --n_draws {n_draws} --annual {annual}"
            ),
            node_args=[
                Params.PARENT_ID,
                Params.CHILD_ID,
                Params.PROPORTION_ID,
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
        return name_task(node, {})

    def get_task(
        self,
        node: str,
        process_graph: nx.DiGraph,
        parent_id: int,
        child_id: int,
        proportion_id: int,
        output_dir: str,
        release_id: int,
        year_id: Union[int, List[int]],
        n_draws: int,
        annual: bool,
    ) -> Task:
        """Returns an EPIC modeled sev split task given task args."""
        # make task
        upstream_tasks = get_upstreams(node, process_graph, self.task_registry)
        node_args = {
            Params.PARENT_ID: parent_id,
            Params.CHILD_ID: child_id,
            Params.PROPORTION_ID: proportion_id,
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
    """Entry point to call modeled_sev_split.ModeledSevSplitter from modeled_sev_split.py"""
    args = parse_arguments()
    startup_jitter()
    splitter = modeled_sev_split.ModeledSevSplitter(
        parent_id=args.parent_id,
        child_id=args.child_id,
        proportion_id=args.proportion_id,
        output_dir=args.output_dir,
        location_id=args.location_id,
        year_id=args.year_id,
        age_group_id=args.age_group_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        proportion_measure_id=args.proportion_measure_id,
        location_set_id=args.location_set_id,
        release_id=args.release_id,
        n_draws=args.n_draws,
        annual=bool(args.annual),
    )
    splitter.run_all_splits_mp()
