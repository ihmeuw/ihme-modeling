import getpass
import logging
import sys
import uuid
from pathlib import Path
from typing import List, Tuple

import httpx
import pandas as pd

from gbd.enums import Cluster
from jobmon.client.api import Tool
from jobmon.client.workflow import Workflow
from jobmon.core.constants import MaxConcurrentlyRunning, WorkflowRunStatus
from jobmon.core.exceptions import WorkflowAlreadyComplete

from como.lib import constants as como_constants
from como.lib import jobmon_utils
from como.lib.version import ComoVersion

CLUSTER_NAME = Cluster.SLURM.value
JOBMON_URL = "URL"
SLACK_URL = "URL"
SLACK_TIMEOUT = 60  # 1 minute

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def post_slack(message: str) -> None:
    """Post a slack message to #como_status."""
    try:
        response = httpx.post(url=SLACK_URL, json={"text": message}, timeout=SLACK_TIMEOUT)
        logger.info(f"slack post: {response}")
    except httpx.RequestError as exc:
        logger.warning(f"slack post failed: {exc}")


def get_sorted_locations(como_dir: str) -> List[int]:
    """Provides a sorted list of all locations.

    Notes:
        - Reads the cached population file.
    """
    cv = ComoVersion(como_dir, read_only=True)
    cv.load_cache()
    population_path = Path(como_dir) / "info" / "population.h5"
    population_df = pd.read_hdf(population_path)
    sorted_locs = sorted(population_df["location_id"].unique())
    return sorted_locs


def create_workflow(
    tool_name: str,
    workflow_name: str,
    resume: bool,
    max_concurrently_running: int = MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING,
) -> Tuple[Workflow, Tool]:
    """Create a COMO to parquet task workflow."""
    tool = Tool(tool_name)
    user = getpass.getuser()

    workflow_args = workflow_name.replace(" ", "_") + f"_{uuid.uuid1()}"
    if resume:
        workflow_args, status = jobmon_utils.get_failed_workflow_args(
            workflow_name=workflow_name, return_status=True
        )
        logger.info(
            f"Resuming workflow {workflow_name} with args {workflow_args} "
            f"and status {status}."
        )

    workflow = tool.create_workflow(
        name=workflow_name,
        default_cluster_name=CLUSTER_NAME,
        default_max_attempts=como_constants.JOBMON_MAX_ATTEMPTS,
        default_compute_resources_set={
            CLUSTER_NAME: {
                "project": "proj_como",
                "standard_error": "FILEPATH",
                "standard_output": "FILEPATH",
                "stderr": "FILEPATH",
                "stdout": "FILEPATH",
                "queue": "all.q",
            }
        },
        max_concurrently_running=max_concurrently_running,
    )

    logger.info(f"Created workflow {workflow_name}.")
    return workflow, tool


def run_workflow(workflow: Workflow, resume: bool, input_args: dict, no_slack: bool) -> None:
    """Runs a jobmon workflow."""
    # this call allows us to access the workflow_id before run()
    workflow.bind()
    message_header = f"workflow: {workflow.name}, args: {workflow.workflow_args}\n\t"
    try:
        logger.info("launching jobmon DAG...")
        launch_message = message_header + (
            f"user: {getpass.getuser()}\n\t"
            f"starting or resuming DAG on {CLUSTER_NAME}\n\t"
            f"- jobmon workflow ID: {workflow.workflow_id}\n\t"
            f"- jobmon GUI URL: {JOBMON_URL.format(workflow_id=workflow.workflow_id)}\n"
            f"*input arguments*:\n"
        )
        launch_message += "\n".join([f"\t{k}: {v}" for k, v in input_args.items()])
        if not no_slack:
            post_slack(launch_message)
        logger.info(launch_message)
        wfr = workflow.run(seconds_until_timeout=60 * 60 * 24 * 7, resume=resume)
        success = wfr == WorkflowRunStatus.DONE
    except WorkflowAlreadyComplete:
        logger.info("DAG already complete...")
        success = True

    if success:
        complete_message = message_header + "jobmon DAG finished successfully."
        if not no_slack:
            post_slack(complete_message)
        logger.info(complete_message)
    else:
        fail_message = message_header + "jobmon DAG failed."
        if not no_slack:
            post_slack(fail_message)
        logger.error(fail_message)
        sys.exit(1)
