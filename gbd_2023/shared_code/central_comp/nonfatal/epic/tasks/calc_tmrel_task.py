import argparse
import pathlib
import sys
from typing import List

from jobmon.client.api import Tool
from jobmon.client.task import Task

from epic.legacy.util.constants import DAG, Params
from epic.lib.logic import calc_tmrel
from epic.lib.util.common import name_task, startup_jitter

_THIS_FILE = pathlib.Path(__file__).resolve()


def parse_arguments() -> argparse.Namespace:
    """Parse TMREL calculation arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--location_id", required=True, type=int)
    parser.add_argument("--release_id", required=True, type=int)
    parser.add_argument("--output_dir", required=True, type=str)
    parser.add_argument("--n_draws", required=True, type=int)
    args = parser.parse_args()
    return args


class TMRELTaskFactory:
    """TMREL calculation task creation class."""

    def __init__(self, tool: Tool) -> None:
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.TMREL,
            command_template=(
                "{python} {script} --location_id {location_id} --release_id {release_id} "
                "--output_dir {output_dir} --n_draws {n_draws}"
            ),
            node_args=["location_id", "release_id", "output_dir", "n_draws"],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(node: str, location_id: int) -> str:
        """Returns task name given node name and args."""
        return name_task(node, {Params.LOCATION_ID: location_id})

    def get_task(
        self,
        node: str,
        location_id: int,
        release_id: int,
        output_dir: str,
        n_draws: int,
        upstream_tasks: List[Task],
    ) -> Task:
        """Returns an EPIC TMREL calculation task given task args."""
        node_args = {
            Params.LOCATION_ID: location_id,
            Params.RELEASE_ID: release_id,
            Params.OUTPUT_DIR: output_dir,
            Params.N_DRAWS: n_draws,
        }
        compute_resources = {
            DAG.ArgNames.MEMORY: "20Gb",
            DAG.ArgNames.NUM_CORES: 5,
            DAG.ArgNames.RUNTIME: f"{60 * 60 * 1}s",  # 1 hour
        }
        task = self.task_template.create_task(
            script=_THIS_FILE,
            compute_resources=compute_resources,
            fallback_queues=DAG.Workflow.FALLBACK_QUEUES,
            upstream_tasks=upstream_tasks,
            max_attempts=DAG.Workflow.MAX_ATTEMPTS,
            name=self.get_task_name(node=node, location_id=location_id),
            python=self.python,
            **node_args,
        )
        return task


if __name__ == "__main__":
    """Entry point to call TMREL calculation code."""
    args = parse_arguments()
    startup_jitter()
    calc_tmrel.main(
        location_id=args.location_id,
        release_id=args.release_id,
        output_dir=args.output_dir,
        n_draws=args.n_draws,
    )
