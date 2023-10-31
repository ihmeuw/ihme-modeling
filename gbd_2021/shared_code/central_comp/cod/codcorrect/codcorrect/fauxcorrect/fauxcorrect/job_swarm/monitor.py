"""Functions for monitoring Cod/FauxCorrect progress through the jobmon workflow."""
import logging
import time
from typing import Dict, List, Optional

import pandas as pd

from db_tools.ezfuncs import query

from fauxcorrect.job_swarm import task_templates
from fauxcorrect.validations.queries import one_row_returned
from fauxcorrect.utils import slack
from fauxcorrect.utils.constants import ConnectionDefinitions, Jobmon


def get_failed_workflow_args(version_id: int) -> str:
    """From given CodCorrect version id, get the workflow
    args from the most recent workflow. If that workflow did not fail,
    throw an error.

    Args:
        version_id: CodCorrect version id

    Raises:
        ValueError: if a workflow can't be found for the version id
        RuntimeError: if the most recent workflow for the CodCorrect version did not fail.
    """
    results = query(
        """
        SELECT workflow_args, status
        FROM docker.workflow
        WHERE name = :name
        ORDER BY id DESC
        """,
        conn_def=ConnectionDefinitions.JOBMON,
        parameters={
            "name": Jobmon.WORKFLOW_NAME.format(version_id=version_id)
        }
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

def get_workflow_id(workflow_args: str) -> int:
    """
    Get workflow_id from the given workflow_args.

    If we've resumed a run but built a different dag, we'll have more than one workflow_id
    associated with the same workflow args. We should avoid this happening in the first
    place, but to keep the monitor jobs from failing, we grab the most recent workflow_id.

    Raises:
        RuntimeError if no matching workflow_id is found in the database
    """
    results = query(
        """
        SELECT id
        FROM workflow
        WHERE workflow_args = :workflow_args
        ORDER BY id DESC
        """,
        conn_def=ConnectionDefinitions.JOBMON,
        parameters={"workflow_args": workflow_args}
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

    results = query(
        q,
        conn_def=ConnectionDefinitions.JOBMON,
        parameters={"workflow_id": workflow_id}
    )

    if results.empty:
        raise RuntimeError(f"No task data found for workflow_id {workflow_id}")

    return results


def construct_tag_summary(
        task_data: pd.DataFrame,
        stage_name: str,
        ignore_na: bool = True
) -> str:
    """Return a formatted string with summary stats on the finished tag."""

    if len(task_data[task_data.isna().any(axis=1)]) > 0:
        if ignore_na:
            task_data = task_data[~task_data.isna().any(axis=1)]
        else:
            raise RuntimeError(
                f"There are NAs in the database for stage '{stage_name}'. "
                "These jobs may still be running. To ignore these NAs, "
                "set ignore_na=True.")

    result_str = f"Stage '{stage_name}' has finished.\n"
    result_str += f">*Jobs*: {len(task_data)}\n"
    result_str += _construct_confidence_interval_string(
        task_data.num_attempts,
        measure_name="Attempts")
    result_str += "\n" + _construct_confidence_interval_string(
        task_data.wallclock.astype(int),
        measure_name="Run time", unit="s")
    result_str += "\n" + _construct_confidence_interval_string(
        task_data.io.str.strip(' GB').astype(float),
        measure_name="IO/s", unit="GB/s")
    result_str += "\n" + _construct_confidence_interval_string(
        task_data.usage_str.str.extract(r'maxvmem=([\d\.]+)').astype(float)[0],
        measure_name="Max vmem", unit="GB")

    return result_str


def monitor_workflow(
        workflow_id: int,
        wait_period: int = 60
) -> int:
    """Monitor the CodCorrect workflow.

    Sends Slack messages as steps complete. If any steps have
    a failure that cannot be recovered from, a Slack message will
    be sent and the function will return 1. Otherwise, returns 0
    upon all steps finishing successfully. Waits 'wait_period'
    seconds in between check-ups.

    Args:
        workflow_id: id of the workflow
        wait_period: time to wait between checks, in seconds
    """
    logging.info(f"Monitoring workflow_id {workflow_id}.")
    task_data = get_task_data(workflow_id)

    running_stages = task_data.stage_name.unique().tolist()
    finished_stages = []

    while running_stages:
        for stage in running_stages:
            task_data = get_task_data(workflow_id, stage)

            if task_data.status.isin(["D"]).all():
                # All tasks finished
                slack.send_slack_update(construct_tag_summary(task_data, stage))
                finished_stages.append(stage)
            elif task_data.status.isin(["F"]).any():
                # Unrecoverage failure
                failed_jobs = task_data.status.isin(["F"]).sum()
                slack.send_slack_update(
                    f"Step '{stage}' had an unrecoverable failure "
                    f"({failed_jobs}/{len(task_data)} jobs)."
                )
                return 1

        for stage in finished_stages:
            if stage in running_stages:
                running_stages.remove(stage)

        if running_stages:
            logging.info(
                f"Run in progress. Waiting {wait_period} seconds for steps: "
                f"{running_stages}"
            )
            time.sleep(wait_period)

    return 0


def _construct_confidence_interval_string(
        data: pd.Series,
        measure_name: str,
        unit: str = "",
        decimals: int = 1
) -> str:
    """Construct a string representing a 95% confidence interval.

    Example: "Memory: 10.1 GB (9.4 GB - 15.2 GB)"

    Args:
        data: pandas Series, must be numeric
        measure_name: str to define the data. "Memory" in the above example
        unit: what unit is the data in? "GB" in the example above. Defaults
            to an empty string.
        decimals: how many decimals the mean, upper, and lower should be
            rounded to. Defaults to 1.
    """
    if unit != "":
        unit = " " + unit

    mean, lower, upper = 0.5, 0.025, 0.975

    result = ">*{measure_name}*: {mean}{unit} ({lower}{unit} - {upper}{unit})"
    return result.format(
        measure_name=measure_name,
        unit=unit,
        mean=data.quantile(mean).round(decimals=decimals),
        lower=data.quantile(lower).round(decimals=decimals),
        upper=data.quantile(upper).round(decimals=decimals)
    )
