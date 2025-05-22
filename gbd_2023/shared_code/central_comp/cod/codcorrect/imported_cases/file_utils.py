"""Utilities module for files, constants, and folders."""

import os
import re
import shutil
from typing import List

from imported_cases.lib import constants


def setup_folders(out_dir: str, cause_ids: List[int]) -> None:
    """Create output directories to store draws.

    Directories created in the form of:
        OUTDIR:
            CAUSE ID A
            CAUSE ID B
            CAUSE ID c
            ...

    Args:
        out_dir: output directory for the run
        cause_ids: the restricted cause ids as a list
    """
    sub_dirs = cause_ids + ["output", "errors", "logs", "logs/stderr", "logs/stdout"]
    new_folders = [f"{out_dir}/{x}" for x in sub_dirs]

    for folder in new_folders:
        if not os.path.exists(folder):
            os.makedirs(folder)


def get_new_version_id() -> int:
    """Return a new version id for the imported cases run.

    Incremented by 1 from the last run. Note: imported cases
    is not tracked in the database, so this function checks the
    run directories in the filesystem.

    Returns:
        version id for the run
    """
    pattern = re.compile(r"\d+")
    version_dirs = [int(x) for x in os.listdir(constants.ROOT_DIR) if pattern.match(x)]
    return int(max(version_dirs)) + 1


def get_file_directory() -> str:
    """Return the directory of the file being run.

    Returns:
        directory of the file running
    """
    return os.path.dirname(os.path.realpath(__file__))


def get_command_executable_path(command: str) -> str:
    """Return the path to the command's executable file.

    This function should be used for running click commands within
    applications.

    Args:
        command: str, command. Ex: "python", "run_cause"

    Returns:
        path to the executable for the command

    Raises:
        RuntimeError: if no executable can be found for the given command
    """
    path = shutil.which(command)

    if path is None:
        raise RuntimeError(f"No executable found for command '{command}'")

    return path
