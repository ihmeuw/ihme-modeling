import argparse
import os
import sys
from typing import Dict, Optional

from gbd import constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task

from como.legacy.common import name_task
from como.lib import constants as como_constants
from como.lib.tasks.public_sort_task import PublicSortTaskFactory
from como.lib.upload import public_upload, upload_utils
from como.lib.version import ComoVersion

_THIS_FILE = os.path.realpath(__file__)
_PUBLIC_UPLOAD_COMPUTE_RESOURCES = {"cores": 1, "memory": "1G", "runtime": "30m"}


class PublicUploadTaskFactory:
    """Public upload task factory, creates and returns tasks and associated task
    names.
    """

    def __init__(
        self, como_version: ComoVersion, task_registry: Dict[str, Task], tool: Tool
    ) -> None:
        self.como_version = como_version
        self.task_registry = task_registry
        command_template = (
            "{python} {script} "
            "--como_dir {como_dir} "
            "--measure_id {measure_id} "
            "--component {component} "
            "--location_id {location_id} "
            "--year_type {year_type}"
        )
        self.task_template = tool.get_task_template(
            template_name="como_public_upload",
            command_template=command_template,
            node_args=["measure_id", "component", "location_id", "year_type"],
            task_args=["como_dir"],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(
        measure_id: int, component: str, location_id: int, year_type: como_constants.YearType
    ) -> str:
        """Return a task name given a measure ID, component, location ID, and year
        type.
        """
        return name_task(
            "como_public_upload",
            {
                gbd_constants.columns.MEASURE_ID: measure_id,
                "component": component,
                "location_id": location_id,
                "year_type": year_type,
            },
        )

    def get_task(
        self,
        measure_id: int,
        component: str,
        location_id: int,
        year_type: como_constants.YearType,
        upstream_location_id: Optional[int] = None,
    ) -> Task:
        """Create/return a task given a measure ID, component, location ID, year type,
        and optional upstream location ID.
        """
        upstream_tasks = []
        upstream_task_name = PublicSortTaskFactory.get_task_name(
            measure_id=measure_id,
            component=component,
            location_id=location_id,
            year_type=year_type,
        )
        upstream_tasks.append(self.task_registry[upstream_task_name])
        if upstream_location_id is not None:
            upstream_task_name = self.get_task_name(
                measure_id=measure_id,
                component=component,
                location_id=upstream_location_id,
                year_type=year_type,
            )
            upstream_tasks.append(self.task_registry[upstream_task_name])

        # get task name
        name = self.get_task_name(
            measure_id=measure_id,
            component=component,
            location_id=location_id,
            year_type=year_type,
        )
        # create task
        task = self.task_template.create_task(
            python=sys.executable,
            script=_THIS_FILE,
            como_dir=self.como_version.como_dir,
            measure_id=measure_id,
            component=component,
            location_id=location_id,
            year_type=year_type,
            name=name,
            compute_resources=_PUBLIC_UPLOAD_COMPUTE_RESOURCES,
            upstream_tasks=upstream_tasks,
            task_attributes={
                gbd_constants.columns.LOCATION_ID: location_id,
                gbd_constants.columns.MEASURE_ID: measure_id,
                como_constants.CommonArg.COMPONENT.value: component,
                como_constants.CommonArg.YEAR_TYPE.value: year_type,
            },
        )
        self.task_registry[name] = task
        return task


def main(
    como_version: ComoVersion,
    measure_id: int,
    component: como_constants.Component,
    location_id: int,
    year_type: str,
) -> None:
    """Run public upload task."""
    upload_task = upload_utils.UploadTask(
        component=component,
        measure_id=measure_id,
        public_upload_test=como_version.public_upload_test,
        process_version_id=como_version.gbd_process_version_id,
        como_dir=como_version.como_dir,
        location_id=location_id,
        year_type=year_type,
    )
    public_upload.run_upload(upload_task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Public upload task.")
    parser.add_argument("--como_dir", type=str, help="directory of como run")
    parser.add_argument("--measure_id", type=int, help="measure_id to upload")
    parser.add_argument("--component", type=str, help="component to upload")
    parser.add_argument("--location_id", type=str, help="location_id to upload")
    parser.add_argument("--year_type", type=str, help="year_type to upload")
    args = parser.parse_args()

    cv = ComoVersion(args.como_dir)
    cv.load_cache()

    main(
        como_version=cv,
        measure_id=args.measure_id,
        component=args.component,
        location_id=args.location_id,
        year_type=args.year_type,
    )
