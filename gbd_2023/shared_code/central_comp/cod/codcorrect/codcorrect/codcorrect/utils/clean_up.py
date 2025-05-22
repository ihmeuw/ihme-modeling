"""
Infrastructure for automating run deletions.

Run deletions consists primarily of:
    * Deleting all draws, files, logs, etc from the filesystem
    * Doing the appropriate database work to drop run summaries
    and mark runs as deleted
"""

import shutil
from os import path
from typing import Optional

from sqlalchemy import orm

import gbd_outputs_versions

from codcorrect.legacy.utils.constants import FilePaths
from codcorrect.lib import db
from codcorrect.lib.utils import files


def delete_run(
    version_id: int,
    gbd_session: orm.Session,
    codcorrect_session: orm.Session,
    delete_db_summaries: bool = False,
    force: bool = False,
) -> None:
    """
    Delete a run of CoDCorrect.

    User will be prompted to confirm deletion is desired.
    If draws have already been deleted, will move smoothly
    to db summary deletion. If there's nothing to delete in
    the GBD db, function will then try to delete diagnostics.
    By default, the function will only delete draws and will not touch database results.

    Deletions include:
        * Deleting all draws, logs, inputs, outputs etc
        (the entire directory) for a run
        * Deleting the summaries in gbd db and diagnostics in codcorrect db
        * Marking the gbd process version as deleted

    Args:
        version_id: the internal version of the process. Ie. 135
            for CoDCorrect v135
        gbd_session: SQLAlchemy session with the GBD db
        codcorrect_session: SQLAlchemy session with the CodCorrect db
        delete_db_summaries: True iff user also wants to delete
            the summaries of the version in the GBD database. Defaults to False
        force: True if you want to skip confirmation for the delete
    """
    parent_dir = path.join(files.ROOT, str(version_id))

    if not force:
        if not _confirm_delete_is_intended(version_id, parent_dir):
            return

    print(f"Begin deletion process for CodCorrect {version_id}")

    # If there are files to delete, delete them. Otherwise, move on
    if path.exists(parent_dir):
        print(f"Deleting all files in {parent_dir}. This may take a while.")
        _delete_run_directory(parent_dir)

        print("Successfully deleted files.")
    else:
        print(f"Run directory '{parent_dir}' does not exist.")

    # Mark database summaries for deletion and delete diagnostics if asked for
    if delete_db_summaries:
        # Check for gbd process versions to delete
        try:
            gbd_process_version_id = db.get_gbd_process_version_id(
                version_id, gbd_session, raise_if_multiple=True
            )
        except ValueError as e:
            # No process versions to delete
            gbd_process_version_id = None
            print(str(e))

        _delete_db_summaries_and_diagnostics(
            version_id, gbd_process_version_id, codcorrect_session
        )
    else:
        print(
            "'delete_db_summaries' set to False. "
            "Skipping deleting summaries and diagnostics.\n"
        )


def _confirm_delete_is_intended(version_id: int, parent_dir: str) -> bool:
    """
    Function to communicate with user so they are forced to confirm
    they want to delete the run. If users say no, the top-level function
    exits early.

    Answer must be either "yes", "y", "no", "n".

    Returns:
        True if delete is confirmed, False if not.
    """
    print(
        f"Deleting draws and database summaries for CodCorrect {version_id} "
        f"in {parent_dir}.\nAre you sure you want to proceed? [y/n]",
        end=" ",
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


def _delete_run_directory(parent_dir: str) -> None:
    """
    Runs deletions of every file in the parent dir (recursive), including
    the parent_dir itself.

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
            f"Received exit status {e.errno}: {e.strerror}"
        ) from e


def _delete_db_summaries_and_diagnostics(
    version_id: int, gbd_process_version_id: Optional[int], codcorrect_session: orm.Session
) -> None:
    """Delete GBD db summaries and Codcorrect diagnostic rows.

    All deletion attempts are expected to fail gracefully if they cannot complete.
    """
    if gbd_process_version_id:
        print(
            "Marking database summaries for deletion for GBD process version "
            f"{gbd_process_version_id}."
        )

        # Attempt to mark the process version for deletion, failing gracefully
        try:
            gbd_outputs_versions.GBDProcessVersion(
                gbd_process_version_id
            ).delete_process_version()
        except Exception as e:
            print(f"Failed to mark GBD process version for deletion:\n{e}")

    print("Deleting before-correction diagnostics from codcorrect.diagnostic.")
    rows_deleted = db.delete_diagnostics(version_id, codcorrect_session)
    print(f"{rows_deleted} diagnostic rows deleted.\n")

    # Commit deletion before moving on
    if rows_deleted:
        try:
            codcorrect_session.commit()
        except Exception as e:
            codcorrect_session.rollback()
            raise RuntimeError(
                f"Could not commit deletions, rolling back. Error:\n{str(e)}"
            ) from e
