import os
import sys
import argparse
from loguru import logger

from como.legacy.common import name_task
from como.lib.disability_weights.combine_epilepsy_any import epilepsy_any
from como.lib.disability_weights.combine_epilepsy_subcombos import epilepsy_combos
from como.lib.version import ComoVersion

THIS_FILE = os.path.realpath(__file__)


class DisabilityWeightTaskFactory:
    def __init__(self, como_version, task_registry, tool):
        self.como_version = como_version
        self.task_registry = task_registry
        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--location_id {location_id} "
            "--n_processes {n_processes}"
        )
        self.task_template = tool.get_task_template(
            template_name="como_dw",
            command_template=command_template,
            node_args=["location_id"],
            task_args=["como_dir"],
            op_args=["python", "script", "n_processes"],
        )

    @staticmethod
    def get_task_name(location_id):
        return name_task(
            "como_dw",
            {"location_id": location_id},
        )

    def get_task(self, location_id, n_processes):
        # make task
        name = self.get_task_name(location_id)
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            n_processes=n_processes,
            name=name,
            compute_resources={
                "cores": 5,
                "memory": "3G",
                "runtime": "3h",
            },
        )
        self.task_registry[name] = task
        return task


class DisabilityWeightTask:
    def __init__(self, como_version, location_id):
        self.como_version = como_version
        self.location_id = location_id

    def run_task(self, n_processes):
        # Generate epilepsy_any disability weights
        logger.info("starting epilepsy_any processing...")
        epilepsy_any(
            self.como_version,
            self.location_id,
            n_processes,
        )

        # Generate epilepsy_subcombos disability_weights
        logger.info("starting epilepsy_subcombos processing...")
        epilepsy_combos(
            self.como_version.como_dir,
            self.location_id,
            n_processes,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="collecting epilepsy disability weight inputs"
    )
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument(
        "--location_id",
        type=int,
        help="location_id to include in this run",
    )
    parser.add_argument("--n_processes", type=int, help="how many subprocesses to use")
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()
    task = DisabilityWeightTask(cv, args.location_id)
    task.run_task(args.n_processes)
