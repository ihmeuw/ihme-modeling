"""Functions for monitoring CoDCorrect progress through the jobmon workflow."""

import logging
import time
from typing import Optional

import pandas as pd
from sqlalchemy import orm

import db_tools_core

from codcorrect.legacy.utils.constants import ConnectionDefinitions, Jobmon
from codcorrect.lib.utils import slack

logger = logging.getLogger(__name__)


def get_failed_workflow_args(version_id: int, session: Optional[orm.Session] = None) -> str:
    """From given CodCorrect version id, get the workflow
    args from the most recent workflow. If that workflow did not fail,
    throw an error.

    Args:
        version_id: CodCorrect version id
        session: SQLAlchemy session

    Raises:
        ValueError: if a workflow can't be found for the version id
        RuntimeError: if the most recent workflow for the CodCorrect version did not fail.
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.JOBMON, session=session
    ) as session:
        results = db_tools_core.query_2_df(
            """
            SELECT workflow_args, status
            FROM docker.workflow
            WHERE name = :name
            ORDER BY id DESC
            """,
            session=session,
            parameters={"name": Jobmon.WORKFLOW_NAME.format(version_id=version_id)},
        )

    if results.empty:
        raise ValueError(
            "No workflow found matching the name '"
            f"{Jobmon.WORKFLOW_NAME.format(version_id=version_id)}'"
        )

    if results.status.iat[0] == "D":
        raise RuntimeError(
            f"CodCorrect version {version_id} finished successfully and therefore cannot "
            f"be resumed. Did you mean to restart the run from the beginning?"
        )

    return results.workflow_args.iat[0]


def get_workflow_id(workflow_args: str, session: Optional[orm.Session] = None) -> int:
    """
    Get workflow_id from the given workflow_args.

    If we've resumed a run but built a different dag, we'll have more than one workflow_id
    associated with the same workflow args. We should avoid this happening in the first
    place, but to keep the monitor jobs from failing, we grab the most recent workflow_id.

    Raises:
        RuntimeError if no matching workflow_id is found in the database
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.JOBMON, session=session
    ) as session:
        results = db_tools_core.query_2_df(
            """
            SELECT id
            FROM workflow
            WHERE workflow_args = :workflow_args
            ORDER BY id DESC
            """,
            session=session,
            parameters={"workflow_args": workflow_args},
        )

    if results.empty:
        raise RuntimeError(
            f"Error retrieving workflow_id from workflow args {workflow_args}: "
            "no results found matching workflow args."
        )

    return results.id.iat[0]


def get_task_data(workflow_id: int, stage_name: Optional[str] = None) -> pd.DataFrame:
    """Get all task/task instance data for the workflow.

    Optional filtering on stage name (task template name).
    Excludes the monitor job itself.
    """
    q = """
        SELECT
           t.name as task_name,
           t.status,
           t.num_attempts,
           ti.usage_str,
           ti.wallclock,
           ti.cpu,
           ti.io,
           tt.name as stage_name
        FROM docker.task t
        LEFT JOIN docker.task_instance ti
            ON t.id = ti.task_id
        JOIN docker.node n
            ON t.node_id = n.id
        JOIN docker.task_template_version ttv
            ON n.task_template_version_id = ttv.id
        JOIN docker.task_template tt
            ON ttv.task_template_id = tt.id
        WHERE
            t.workflow_id = :workflow_id
            AND tt.name != 'monitor'
    """

    if stage_name:
        q += f" AND tt.name = '{stage_name}'"

    with db_tools_core.session_scope(ConnectionDefinitions.JOBMON) as session:
        results = db_tools_core.query_2_df(
            q, session=session, parameters={"workflow_id": workflow_id}
        )

    if results.empty:
        raise RuntimeError(f"No task data found for workflow_id {workflow_id}")

    return results


def monitor_workflow(workflow_id: int, resume: int, test: int, wait_period: int = 60) -> int:
    """Monitor the CodCorrect workflow.

    If any steps have a failure that cannot be recovered from, a Slack message will
    be sent and the function will return 1. Otherwise, returns 0
    upon all steps finishing successfully. Waits 'wait_period'
    seconds in between check-ups.

    Args:
        workflow_id: id of the workflow
        resume: was the run resumed (1)? If so, exclude out the stages that already finished
        test: is the run a test (1)?
        wait_period: time to wait between checks, in seconds
    """
    logger.info(f"Monitoring workflow_id {workflow_id}.")

    task_data = get_task_data(workflow_id)
    running_stages = task_data.stage_name.unique().tolist()
    finished_stages = []

    while running_stages:
        for stage in running_stages:
            task_data = get_task_data(workflow_id, stage)

            if task_data.status.isin(["D"]).all():
                # All tasks finished, add stage to finished stages in all cases;
                # Don't send anything if it's a resume and our first time in the loop
                finished_stages.append(stage)

            elif task_data.status.isin(["F"]).any():
                # Unrecoverable failure
                failed_jobs = task_data.status.isin(["F"]).sum()
                slack.send_slack_update(
                    f"Step '{stage}' had an unrecoverable failure "
                    f"({failed_jobs}/{len(task_data)} jobs).",
                    test=test,
                )
                return 1

        for stage in finished_stages:
            if stage in running_stages:
                running_stages.remove(stage)

        if running_stages:
            logger.info(
                f"Run in progress. Waiting {wait_period} seconds for steps: "
                f"{running_stages}"
            )
            time.sleep(wait_period)

    return 0
