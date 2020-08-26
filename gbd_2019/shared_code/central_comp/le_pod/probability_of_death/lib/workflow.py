import getpass
import os
import shutil
import uuid
from typing import Any, List, Set

import db_queries
from jobmon import client

from probability_of_death.lib.constants import columns, paths


def run_workflow(
    location_set_ids: List[int],
    year_ids: List[int],
    sex_ids: List[int],
    age_group_ids: List[int],
    gbd_round_id: int,
    decomp_step: str,
    cluster_project: str,
    deaths_version: str,
) -> None:
    """Creates and runs probability of death jobmon workflow."""
    _create_temp_dir()
    location_ids = _get_locations(gbd_round_id, decomp_step, location_set_ids)

    workflow_id = uuid.uuid1()
    workflow = client.Workflow(
        workflow_args=f"pod_{workflow_id}",
        project=cluster_project,
        stderr=f"/share/temp/sgeoutput/{getpass.getuser()}/errors",
        stdout=f"/share/temp/sgeoutput/{getpass.getuser()}/output",
        resume=True,
    )

    upload_task = client.BashTask(
        command=_build_command("upload", [gbd_round_id, decomp_step]),
        name=f"pod_{workflow_id}_upload",
        num_cores=1,
        m_mem_free="100G",
        max_attempts=1,
        max_runtime_seconds=24 * 60 * 60,
        queue="all.q",
    )
    workflow.add_task(upload_task)
    for location_id in location_ids:
        command = _build_command(
            "generate",
            [
                location_id,
                year_ids,
                sex_ids,
                age_group_ids,
                gbd_round_id,
                decomp_step,
                deaths_version,
            ],
        )
        task = client.BashTask(
            command=command,
            name=f"pod_{workflow_id}_generate_{location_id}",
            num_cores=1,
            m_mem_free="10G",
            max_attempts=2,
            max_runtime_seconds=30 * 60,
            queue="all.q",
        )
        upload_task.add_upstream(task)
        workflow.add_task(task)

    workflow.run()


def _create_temp_dir() -> None:
    """Creates a temporary directory for this run, wiping any existing files."""
    if os.path.exists(paths.POD_TMP):
        shutil.rmtree(paths.POD_TMP)
    os.mkdir(paths.POD_TMP)


def _get_locations(
    gbd_round_id: int, decomp_step: str, location_set_ids: List[int]
) -> Set[int]:
    """Pulls set of locations for this probability of death run."""
    return set(
        int(location_id)
        for location_set_id in location_set_ids
        for location_id in db_queries.get_location_metadata(
            location_set_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step
        )[columns.LOCATION_ID]
    )


def _build_command(command: str, command_args: List[Any]) -> str:
    """Prepares a poetry command from this project for a jobmon bash task.

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
    formatted_args = [
        ",".join([str(x) for x in arg]) if isinstance(arg, list) else str(arg)
        for arg in command_args
    ]
    return (
        f"cd {paths.CODE_ROOT} && "
        f"{paths.POETRY_EXECUTABLE} run {command} {' '.join(formatted_args)}"
    )
