import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import ihme_cc_cache
from db_queries.api.internal import get_location_hierarchy_by_version
from jobmon.client.api import Tool
from jobmon.client.task import Task

from como.legacy.common import name_task
from como.lib import constants
from como.lib import minimum_incidence as mi
from como.lib.version import ComoVersion

THIS_FILE = os.path.realpath(__file__)


class MinimumIncidenceCodTaskFactory:
    """Factory to create a task for minimum incidence cod."""

    def __init__(
        self, como_version: ComoVersion, task_registry: Dict[str, Task], tool: Tool
    ) -> None:
        self.como_version = como_version
        self.task_registry = task_registry
        command_template = (
            "{python} {script} " "--cause_id {cause_id} " "--como_dir {como_dir}"
        )
        self.task_template = tool.get_task_template(
            template_name="como_minimum_inc_cod",
            command_template=command_template,
            node_args=["cause_id"],
            task_args=["como_dir"],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(cause_id: int) -> str:
        """Get task name based on parameters."""
        return name_task("como_min_inc_cod", {"cause_id": cause_id})

    def get_task(self, cause_id: int, upstream_cause_id: Optional[int]) -> Task:
        """Create a task with upstream dependencies, based on cause_id."""
        # make task
        name = self.get_task_name(cause_id)
        upstream_tasks: List[Task] = []
        if upstream_cause_id:
            upstream_task_name = self.get_task_name(upstream_cause_id)
            upstream_tasks.append(self.task_registry[upstream_task_name])
        task = self.task_template.create_task(
            python=sys.executable,
            script=THIS_FILE,
            como_dir=self.como_version.como_dir,
            cause_id=cause_id,
            name=name,
            compute_resources={"cores": 5, "memory": "3G", "runtime": "3h"},
            upstream_tasks=upstream_tasks,
        )
        self.task_registry[name] = task
        return task


class MinIncidenceCodTask:
    """Task for minimum incidence cod."""

    def __init__(self, como_dir: str):
        self.como_dir = como_dir
        self.como_version = ComoVersion(como_dir=self.como_dir)
        self.como_version.load_cache()

    def run_task(self, cause_id: int) -> None:
        """Execute task."""
        self.cause_id = cause_id
        tmi_cache_path = Path(self.como_dir) / constants.TMI_CACHE_PATH
        manifest_path = tmi_cache_path / constants.TMI_CACHE_MANIFEST_PATH
        tmi_cache = ihme_cc_cache.FileBackedCacheReader(manifest_path)
        columns = ["cause_id", "location_id", "sex_id"]
        location_ids = get_location_hierarchy_by_version(
            location_set_version_id=self.como_version.location_set_version_id
        )["location_id"].to_list()
        min_inc_df = mi.minimum_incidence_from_codcorrect(
            cause_id=cause_id,
            codcorrect_process_version_id=self.como_version.codcorrect_process_version,
            release_id=self.como_version.release_id,
            case_fatality_df=tmi_cache.get(constants.TMI_CACHE_CFR),
            year_id=self.como_version.year_id,
            location_id=location_ids,
        )

        for (cause_id, location_id, sex_id), group in min_inc_df.groupby(columns):
            obj_name = constants.TMI_FILEPATH.format(
                cause_id=cause_id, location_id=location_id, sex_id=sex_id
            )
            hdf_path = "FILEPATH"
            group.to_hdf(hdf_path, key="data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument(
        "--cause_id", nargs="*", type=int, default=[], help="cause_id to include in this run"
    )
    args = parser.parse_args()
    task = MinIncidenceCodTask(args.como_dir)
    task.run_task(args.cause_id)
