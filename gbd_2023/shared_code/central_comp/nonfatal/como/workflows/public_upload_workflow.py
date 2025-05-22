import itertools
import logging

import click
import numpy as np

from gbd import constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.workflow import Workflow

from como.lib import constants as como_constants
from como.lib.tasks.public_sort_task import PublicSortTaskFactory
from como.lib.tasks.public_upload_task import PublicUploadTaskFactory
from como.lib.upload import public_upload as public_upload_lib
from como.lib.version import ComoVersion
from como.lib.workflows import utils as workflow_utils

CONCURRENT_PUBLIC_UPLOADS = 4

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_public_upload_tasks(
    como_version: ComoVersion, tool: Tool, workflow: Workflow
) -> Workflow:
    """Create a public upload workflow given a COMO dir."""
    sorted_locs = workflow_utils.get_sorted_locations(como_version.como_dir)

    year_types = [como_constants.YearType.SINGLE]
    if len(como_version.change_years) > 0:
        year_types.append(como_constants.YearType.MULTI)

    _task_registry = {}
    _public_sort_task_fac = PublicSortTaskFactory(como_version, _task_registry, tool)
    _public_upload_task_fac = PublicUploadTaskFactory(como_version, _task_registry, tool)
    component_measure_list = [
        cm_pair
        for cm_pair in itertools.product(
            como_version.components, np.atleast_1d(como_version.measure_id)
        )
        if cm_pair != (como_constants.Component.IMPAIRMENT, gbd_constants.measures.INCIDENCE)
    ]

    # Add public sort tasks
    for component, measure_id in component_measure_list:
        for location_id in sorted_locs:
            public_sort_task = _public_sort_task_fac.get_task(
                measure_id=measure_id,
                component=component,
                location_id=location_id,
                year_type=como_constants.YearType.SINGLE,
                find_upstreams=False,
            )
            workflow.add_task(public_sort_task)

            if como_version.change_years:
                public_sort_task = _public_sort_task_fac.get_task(
                    measure_id=measure_id,
                    component=component,
                    location_id=location_id,
                    year_type=como_constants.YearType.MULTI,
                    find_upstreams=False,
                )
                workflow.add_task(public_sort_task)
        logger.info(
            f"Added public sort tasks for component {component}, measure {measure_id}."
        )

    # Add public upload tasks
    for year_type in year_types:
        for component, measure_id in component_measure_list:
            for index, location_id in enumerate(sorted_locs):
                upstream_location_id = None if index == 0 else sorted_locs[index - 1]
                public_upload_task = _public_upload_task_fac.get_task(
                    measure_id=measure_id,
                    component=component,
                    location_id=location_id,
                    year_type=year_type,
                    upstream_location_id=upstream_location_id,
                )
                workflow.add_task(public_upload_task)
        logger.info(
            f"Added public upload tasks for component {component}, measure {measure_id}, "
            f"year_type {year_type}."
        )

    workflow.set_task_template_max_concurrency_limit(
        task_template_name=_public_upload_task_fac.task_template.template_name,
        limit=CONCURRENT_PUBLIC_UPLOADS,
    )

    return workflow


@click.command
@click.option(
    "como_dir",
    "--como_dir",
    type=str,
    required=True,
    help="Root directory of a COMO run where summaries already exist.",
)
@click.option("--resume", is_flag=True, help="whether to resume the workflow.")
@click.option(
    "--create_tables",
    is_flag=True,
    help=(
        "whether to create the tables in the public upload host."
        "Ignored if --resume is set."
    ),
)
@click.option("--no_slack", is_flag=True, help="whether to post to slack.")
def run_public_upload_workflow(
    como_dir: str, resume: bool, create_tables: bool, no_slack: bool
) -> None:
    """Creates and runs an public upload.

    Notes:
        - this workflow has no heavy setup, so a simple rerun will always resume.
    """
    input_args = locals()
    cv = ComoVersion(como_dir)
    cv.load_cache()

    workflow, tool = workflow_utils.create_workflow(
        tool_name="como_public_upload",
        workflow_name=f"COMO public upload v{cv.como_version_id}",
        resume=resume,
    )

    workflow = add_public_upload_tasks(como_version=cv, tool=tool, workflow=workflow)

    if create_tables and not resume:
        public_upload_lib.configure_upload(como_version=cv)

    workflow_utils.run_workflow(
        workflow=workflow, resume=resume, input_args=input_args, no_slack=no_slack
    )


if __name__ == "__main__":
    run_public_upload_workflow()
