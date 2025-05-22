import itertools
import logging

import click

from gbd import constants as gbd_constants
from gbd.enums import Cluster
from jobmon.client.api import Tool
from jobmon.client.workflow import Workflow

from como.lib import constants as como_constants
from como.lib.tasks.internal_upload_task import InternalUploadTaskFactory
from como.lib.version import ComoVersion
from como.lib.workflows import utils as workflow_utils

CLUSTER_NAME = Cluster.SLURM.value
JOBMON_URL = "URL"

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_internal_upload_tasks(
    como_version: ComoVersion, tool: Tool, workflow: Workflow, concurrency: int
) -> Workflow:
    """Create an internal upload workflow given a COMO dir."""
    sorted_locs = workflow_utils.get_sorted_locations(como_version.como_dir)

    year_types = [como_constants.YearType.SINGLE]
    if len(como_version.change_years) > 0:
        year_types.append(como_constants.YearType.MULTI)

    task_factory_names = dict()
    for component, measure_id, year_type in itertools.product(
        como_version.components, como_version.measure_id, year_types
    ):
        # every (component, measure, year_type) is a specific table
        if (component == "impairment") and (measure_id == gbd_constants.measures.INCIDENCE):
            # There is no impairment incidence
            continue
        task_factory = InternalUploadTaskFactory(
            como_version=como_version,
            component=component,
            measure_id=measure_id,
            year_type=year_type,
            tool=tool,
        )
        task_factory_names[(component, measure_id, year_type)] = (
            task_factory.task_template.template_name
        )
        for location_id in sorted_locs:
            # for a given table, we make a linear DAG based on location_id
            # only one concurrent task per table
            workflow.add_task(
                task_factory.get_task(location_id=location_id, upstream_tasks=[])
            )
        logger.info(
            f"Added tasks for component {component}, measure {measure_id}, year_type "
            f"{year_type}."
        )

    for _, tt_name in task_factory_names.items():
        workflow.set_task_template_max_concurrency_limit(
            task_template_name=tt_name, limit=concurrency
        )

    return workflow


@click.command
@click.option(
    "como_dir", "--como_dir", type=str, required=True, help="Directory of prior COMO run"
)
@click.option(
    "concurrency", "--concurrency", type=int, required=True, help="Per-table concurrency."
)
@click.option("--resume", is_flag=True, help="whether to resume the workflow.")
@click.option("--no_slack", is_flag=True, help="whether to post to slack.")
def run_internal_upload_workflow(
    como_dir: str, concurrency: int, resume: bool, no_slack: bool
) -> None:
    """Creates and runs an internal upload.

    Notes:
        - this workflow has no heavy setup, so a simple rerun will always resume.
    """
    input_args = locals()

    cv = ComoVersion(como_dir)
    cv.load_cache()

    workflow, tool = workflow_utils.create_workflow(
        tool_name="como_internal_upload",
        workflow_name=f"COMO internal upload v{cv.como_version_id}",
        resume=resume,
    )

    workflow = add_internal_upload_tasks(
        como_version=cv, tool=tool, workflow=workflow, concurrency=concurrency
    )

    workflow_utils.run_workflow(
        workflow=workflow, resume=resume, input_args=input_args, no_slack=no_slack
    )


if __name__ == "__main__":
    run_internal_upload_workflow()
