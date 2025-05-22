from typing import Tuple, Union

import db_tools_core

# Use 'privileged' jobmon connection to avoid LDAP credential overwriting via
# user-credentials.cfg. Has read-only privs on jobmon database
JOBMON_CONN_DEF: str = "CONN_DEF"


class NoWorkflowFound(Exception):
    """Custom exception."""

    pass


class WorkflowFinished(Exception):
    """Custom exception."""

    pass


def get_failed_workflow_args(
    workflow_name: str, return_status: bool
) -> Union[str, Tuple[str, str]]:
    """Retrieves the workflow arguments for the most recent use of a workflow name."""
    with db_tools_core.session_scope(JOBMON_CONN_DEF) as session:
        results = db_tools_core.query_2_df(
            """
            SELECT workflow_args, status
            FROM docker.workflow
            WHERE name = :name
            ORDER BY id DESC
            """,
            session=session,
            parameters={"name": workflow_name},
        )

    if results.empty:
        raise NoWorkflowFound(f"No workflow found matching the name {workflow_name}.")

    if results["status"].iat[0] == "D":
        raise WorkflowFinished(
            f"The workflow with name {workflow_name} finished successfully and can not "
            "be resumed."
        )

    if return_status:
        return (results["workflow_args"].iat[0], results["status"].iat[0])

    return results["workflow_args"].iat[0]
