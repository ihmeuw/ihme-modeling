import argparse
import os
import sys
from typing import Dict, List

import gbd.constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task

from epic.legacy.super_squeeze import run_squeeze
from epic.legacy.util.constants import DAG, Params
from epic.lib.util.common import name_task, startup_jitter

_THIS_FILE = os.path.realpath(__file__)


def parse_arguments() -> argparse.Namespace:
    """Parse super squeeze task arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir", required=True, type=str, help="output directory to save results to"
    )
    parser.add_argument(
        "--location_id", required=True, type=int, help="location ID of results"
    )
    parser.add_argument("--year_id", required=True, type=int, help="year ID of results")
    parser.add_argument("--sex_id", required=True, type=int, help="sex ID of results")
    parser.add_argument(
        "--release_id",
        type=int,
        default=gbd_constants.RELEASE_ID,
        help="Release ID of results",
    )
    parser.add_argument(
        "--n_draws", type=int, default=1000, help="number of draws in results"
    )
    args = parser.parse_args()

    return args


class SuperSqueezeFactory(object):
    """Super squeeze task creation class."""

    def __init__(self, task_registry: Dict[str, Task], tool: Tool) -> None:
        self.task_registry = task_registry
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.SUPER_SQUEEZE,
            command_template=(
                "{python} {script} --output_dir {output_dir} --location_id {location_id} "
                "--year_id {year_id} --sex_id {sex_id} --release_id {release_id} "
                "--n_draws {n_draws}"
            ),
            node_args=[
                Params.OUTPUT_DIR,
                Params.LOCATION_ID,
                Params.YEAR_ID,
                Params.SEX_ID,
                Params.RELEASE_ID,
                Params.N_DRAWS,
            ],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(node: str, location_id: int, year_id: int, sex_id: int) -> str:
        """Returns task name given node, location, year, and sex."""
        return name_task(
            node,
            {Params.LOCATION_ID: location_id, Params.SEX_ID: sex_id, Params.YEAR_ID: year_id},
        )

    def get_task(
        self,
        node: str,
        output_dir: str,
        location_id: int,
        year_id: int,
        sex_id: int,
        release_id: int,
        n_draws: int,
        upstream_tasks: List[Task],
    ) -> Task:
        """Returns an EPIC super squeeze task given task args."""
        # make task
        node_args = {
            Params.OUTPUT_DIR: output_dir,
            Params.LOCATION_ID: str(location_id),
            Params.YEAR_ID: str(year_id),
            Params.SEX_ID: str(sex_id),
            Params.RELEASE_ID: release_id,
            Params.N_DRAWS: str(n_draws),
        }
        compute_resources = {
            DAG.ArgNames.MEMORY: "50Gb",
            DAG.ArgNames.NUM_CORES: 10,
            DAG.ArgNames.RUNTIME: f"{60 * 60 * 3}s",  # 3 hours
        }
        task = self.task_template.create_task(
            script=_THIS_FILE,
            compute_resources=compute_resources,
            fallback_queues=DAG.Workflow.FALLBACK_QUEUES,
            upstream_tasks=upstream_tasks,
            max_attempts=DAG.Workflow.MAX_ATTEMPTS,
            name=self.get_task_name(node, location_id, year_id, sex_id),
            python=self.python,
            **node_args,
        )
        return task


if __name__ == "__main__":
    """Entry point to call run_squeeze from super_squeeze.py"""
    args = parse_arguments()
    startup_jitter()
    run_squeeze(
        output_dir=args.output_dir,
        location_id=args.location_id,
        year_id=args.year_id,
        sex_id=args.sex_id,
        release_id=args.release_id,
        n_draws=args.n_draws,
    )
