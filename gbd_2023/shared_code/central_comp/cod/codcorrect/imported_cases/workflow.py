"""Jobmon jobswarm for imported cases."""

import datetime
import logging
import os
from typing import List, Optional, Tuple

from jobmon.client.tool import Tool

from imported_cases.lib import constants, core, file_utils, log_utils

logger = logging.getLogger(__name__)


def run_workflow(
    release_id: int, test: bool, resume: bool, version_id: Optional[int]
) -> None:
    """Create and runs the imported cases jobmon workflow.

    Args:
        release_id: ID of the release of the run
        test: False if not test, True if test
        resume: True if run is being resumed
        version_id: version id of run if resuming
    """
    log_utils.configure_logging()
    version_id, out_dir, cause_ids = _setup(release_id, resume, version_id)

    # Get workflow-specific info
    timestamp = datetime.datetime.now().isoformat()

    logger.info("Instantiating workflow")
    tool = Tool("Imported cases")
    workflow = tool.create_workflow(
        name=f"Imported cases generator v{version_id}",
        workflow_args=f"imported_cases_v{version_id}_{timestamp}",
        default_cluster_name="slurm",
        default_compute_resources_set={
            "slurm": {
                "stderr": os.path.join(out_dir, "logs", "stderr"),
                "stdout": os.path.join(out_dir, "logs", "stdout"),
                "project": "PROJECT",
                "queue": "QUEUE",
            }
        },
    )

    template = tool.get_task_template(
        template_name="imported_cases",
        command_template=(
            "{executable} "
            "{cause_id} "
            "{version_id} "
            "{release_id} "
            "{out_dir} "
            "{test}"
        ),
        node_args=["cause_id"],
        task_args=["version_id", "release_id", "out_dir", "test"],
        op_args=["executable"],
    )
    compute_resources = {
        "cores": constants.NUM_CORES,
        "memory": "120G",
        "runtime": (7 * 24 * 60 * 60),  # 1 week
        "queue": "QUEUE",
    }

    logger.info("Building workflow")
    for cause_id in cause_ids:
        task = template.create_task(
            cluster_name="slurm",
            compute_resources=compute_resources,
            name=f"imported_cases_{version_id}_{cause_id}",
            max_attempts=2,
            executable=file_utils.get_command_executable_path("run_cause"),
            cause_id=cause_id,
            version_id=version_id,
            release_id=release_id,
            out_dir=out_dir,
            test=int(test),
        )
        workflow.add_task(task)

    logger.info("Running workflow")
    result = workflow.run(seconds_until_timeout=(7 * 24 * 60 * 60), resume=resume)

    logger.info(f"Imported cases finished with status '{result}'.")


def _setup(
    release_id: int, resume: bool, version_id: Optional[int]
) -> Tuple[int, str, List[int]]:
    """Set up for imported cases workflow run.

    Includes:
        * Build out directory space
        * Pull spacetime restricted cause ids

    Args:
        release_id: ID of the release for the run
        resume: True if run is being resumed
        version_id: version id of run if resuming

    Returns:
        Tuple of version_id, output directory, spacetime restricted cause ids
    """
    if resume:
        if version_id is None:
            raise ValueError("If resuming, must provide a version id.")

        out_dir = constants.VERSION_DIR.format(version_id)
    else:
        version_id = file_utils.get_new_version_id()
        out_dir = constants.VERSION_DIR.format(version_id)

    logger.info(
        "Starting Imported Cases Generator: "
        f"params-- version_id: {version_id}, release_id: {release_id}, "
        f"resume: {resume}"
    )
    # Set up directories
    logger.info("Getting cause ids to process")
    restricted_causes = core.get_spacetime_restricted_cause_ids(release_id)
    logger.info(f"Restricted cause ids: {restricted_causes}")

    # Set up file system if not resuming
    if not resume:
        logger.info("Set up folders")
        file_utils.setup_folders(out_dir, restricted_causes)

    return version_id, out_dir, restricted_causes
