"""
Infrastructure for automating run deletions.

Run deletions consists primarily of:
    * Deleting all draws, files, logs, etc from the filesystem
    * Doing the appropirate database work to drop run summaries
    and mark runs as deleted
"""
from os import path
import shutil
import subprocess

from db_tools.ezfuncs import get_session
from db_tools.query_tools import exec_query

from fauxcorrect.parameters.machinery import MachineParameters
from fauxcorrect.queries.queries import GbdDatabase
from fauxcorrect.utils import helpers
from fauxcorrect.utils.constants import ConnectionDefinitions, FilePaths
from fauxcorrect.validations import input_args


def delete_run(
    process: str,
    version_id: int,
    delete_db_summaries: bool = False,
    force: bool = False
) -> None:
    """
    Delete a run of Faux/CodCorrect.

    User will be prompted to confirm deletion is desired.
    If draws have already been deleted, will move smoothly
    to db sumamry deletion. If there's nothing to delete in
    the db, function will exit. By default, the function
    will only delete draws and will not touch database summaries.

    Deletions include:
        * Deleting all draws, logs, inputs, outputs etc
        (the entire directory) for a run
        * Deleting the summaries in the database
        * Marking the gbd process version as deleted

    Args:
        process: the name of the process, "codcorrect" or "fauxcorrect"
        version_id: the internal version of the process. Ie. 135
            for CoDCorrect v135
        delete_database_summaries: True iff user also wants to delete
            the summaries of the version in the GBD database. Defaults to False
        force: True if you want to skip confirmation for the delete

    Raises:
        ValueError if process is not either "codcorrect" or "fauxcorrect"
    """
    input_args.validate_process_name(process)

    parent_dir = MachineParameters.create_parent_dir(
        version_id=version_id, process_name=process
    )

    if not force:
        if not _confirm_delete_is_intended(process, version_id, parent_dir):
            return

    # If there are files to delete, delete them. Otherwise, move on
    if path.exists(parent_dir):
        print("Counting files...")
        num_files = _count_files(parent_dir)

        print(
            f"\nDeleting {num_files} files in {parent_dir}. "
            "This may take a while."
        )
        _delete_run_directory(parent_dir)

        print(f"Successfully deleted {num_files} files.")
    else:
        print(f"Run directory '{parent_dir}' does not exist.")

    # check for gbd process versions to delete
    try:
        gbd_process_version_id = helpers.get_gbd_process_version_id(
            process, version_id, return_all=True)
    except ValueError as e:
        # no process versions to delete
        print(str(e) + "\nExiting.\n")
        return

    # Mark database summaries for deletion if asked for
    if delete_db_summaries:
        print(
            "Marking database summaries for deletion for GBD process version(s)"
            f"{', '.join(str(x) for x in gbd_process_version_id)}.\n"
        )
        _delete_database_summaries(gbd_process_version_id)
    else:
        print(
            "'delete_db_summaries' set to False. "
            "Skipping marking summaries for deletion.\n"
        )


def _confirm_delete_is_intended(
        process: str,
        version_id: int,
        parent_dir: str
) -> bool:
    """
    Function to communicate with user so they are forced to confirm
    they want to delete the run. If users say no, the top-level function
    exits early.

    Answer must be either "yes", "y", "no", "n".

    Returns:
        True if delete is confirmed, False if not.
    """
    print(
        f"Deleting draws and database summaries for {process} {version_id} "
        f"in {parent_dir}.\n"
        f"Are you sure you want to proceed? [y/n]", end=" "
    )
    while True:
        choice = input().lower()
        if choice in ["yes", "y"]:
            return True
        elif choice in ["no", "n"]:
            print("Quitting.")
            return False
        else:
            print("Please respond with 'yes' or 'no'.", end=" ")


def _count_files(directory: str) -> int:
    """
    Counts the number of files in the directory.

    Explanation:
        find . -type f finds all files ( -type f ) in this ( . ) directory and
        in all sub directories, the filenames are then printed to standard out
        one per line.

        This is then piped | into wc (word count) the -l option tells wc to only
        count lines of its input.

        Together they count all your files.

    Args:
        directory: directory to recursively count the files of
    """
    res = subprocess.run(
        [f"find {directory} -type f | wc -l"],
        stdout=subprocess.PIPE,
        check=True,
        shell=True
    ).stdout.decode("utf-8")

    # drop the newline and return
    return int(res.split()[0])


def _delete_run_directory(parent_dir: str) -> None:
    """
    Runs deletions of every file in the parent dir (recursive), including
    the parent_dir itself.

    BE VERY CAREFUL.

    These deletes will be done swiftly and silently. Parent dir MUST
    be vetted before being passed into this function.

    Raises:
        RuntimeError if some OSError is hit along the way
    """
    if FilePaths.ROOT_DIR not in parent_dir:
        raise RuntimeError(f"Invalid value for parent_dir: {parent_dir}")

    try:
        shutil.rmtree(parent_dir)
    except OSError as e:
        raise RuntimeError(
            f"Something went wrong when deleting files from {parent_dir}. "
            f"Received exit status {e.errno}: {e.strerror}") from e


def _delete_database_summaries(gbd_process_version_id: int) -> None:
    """
    Launches the sproc to mark process versions as deleted and to
    drop the summaries as needed.
    """
    exec_query(
        GbdDatabase.DELETE_PROCESS_VERSION,
        parameters={
            "gbd_process_version_id": gbd_process_version_id
        },
        session=get_session(ConnectionDefinitions.GBD),
        close=True
    )
