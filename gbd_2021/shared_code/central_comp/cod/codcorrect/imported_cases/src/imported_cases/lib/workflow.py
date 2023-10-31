"""Jobmon jobswarm for imported cases."""
import datetime
import getpass
import logging
import os
import time
from typing import Any, List, Tuple

from jobmon import client

from imported_cases.lib import constants, core, file_utils, log_utils

logger = logging.getLogger(__name__)


def run_workflow(gbd_round_id: int, decomp_step: str) -> None:
    """Create and runs the imported cases jobmon workflow.

    Args:
        gbd_round_id: ID of the GBD round
        decomp_step: decomp step
    """
    version_id, out_dir, cause_ids = _setup(gbd_round_id, decomp_step)

    # Get workflow-specific info
    username = getpass.getuser()
    timestamp = datetime.datetime.now().isoformat()

    logging.info("Submitting Jobs")
    workflow = client.Workflow(
        workflow_args=f"imported_cases_v{version_id}_{timestamp}",
        name="Imported Cases Generator",
        project="proj_codcorrect",
        stdout=f"/ROOT/{username}/output",
        stderr=f"/ROOT/{username}/errors",
        seconds_until_timeout=(7 * 24 * 60 * 60),  # 1 week
    )

    for cause_id in cause_ids:
        command = _build_command(
            "run_cause", [cause_id, version_id, gbd_round_id, decomp_step, out_dir]
        )
        task = client.BashTask(
            command=command,
            name="imported_cases_{}_{}".format(version_id, cause_id),
            num_cores=constants.NUM_CORES,
            m_mem_free="120.0G",
            max_attempts=2,
            tag="imported_cases",
            queue="long.q",
        )
        workflow.add_task(task)

    logging.info("Running swarm")
    workflow.run()


def _setup(gbd_round_id: int, decomp_step: str,) -> Tuple[int, str, List[int]]:
    """Set up for imported cases workflow run.

    Includes:
        * Start logging
        * Build out directory space
        * Pull spacetime restricted cause ids

    Args:
        gbd_round_id: ID of the GBD round
        decomp_step: decomp step

    Returns:
        Tuple of version_id, output directory, spacetime restricted cause ids
    """
    version_id = file_utils.get_new_version_id()
    out_dir = constants.VERSION_DIR.format(version_id)

    log_dir = os.path.join(out_dir, "logs")
    log_utils.setup_logging(log_dir, "launch_imported_cases", time.strftime("%Y_%m_%d_%H"))

    logging.info(
        "Starting Imported Cases Generator: "
        f"params-- version_id: {version_id}, gbd_round_id: {gbd_round_id}, "
        f"decomp_step: {decomp_step}"
    )
    # Set up directories
    logging.info("Getting cause ids to process")
    restricted_causes = core.get_spacetime_restricted_cause_ids(gbd_round_id, decomp_step)
    logging.info(f"Restricted cause ids: {restricted_causes}")

    logging.info("Set up folders")
    file_utils.setup_folders(out_dir, restricted_causes)

    return version_id, out_dir, restricted_causes


def _build_command(command: str, command_args: List[Any]) -> str:
    """Prepares a linux command from this project for a jobmon bash task.

    Converts lists into comma-separated strings for CLI parsing.
    Navigates to this project's root directory so that poetry can find the command to execute.
    Runs the command using conda-installed poetry.

    Args:
        command: the actual command to run, corresponding to the commands specified in
            pyproject.toml. E.g. "generate" or "upload".
        command_args: list of arguments to pass to the command.

    Returns:
        Command to pass to jobmon bash task.
    """
    executable_path = file_utils.get_command_executable_path(command)
    formatted_args = [
        ",".join([str(x) for x in arg]) if isinstance(arg, list) else str(arg)
        for arg in command_args
    ]
    return f"{executable_path} {' '.join(formatted_args)}"
