"""Helper functions for sending Slack messages about the machinery."""

from typing import Optional

from sqlalchemy import orm

import db_tools_core
from gbd_outputs_versions import slack
from jobmon.client.workflow import WorkflowRunStatus

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import ConnectionDefinitions, Slack
from codcorrect.lib.db.queries import GbdDatabase


def send_slack_greeting(params: MachineParameters, restart: bool, resume: bool) -> None:
    """Send greeting for start of run via Slack.

    Restart and resume cannot both be true.

    Args:
        params: machinery parameters of a CodCorrect run
        restart: was the run restarted?
        resume: was the run resumed?
    """
    url = Slack.SHADES_DMS if params.test else slack.MACHINERY_RUNS
    slack.send_slack_message(construct_run_greeting(params, restart, resume), url=url)


def send_slack_goodbye(
    params: MachineParameters, status: str, session: Optional[orm.Session] = None
) -> None:
    """Send goodbye for end of run via Slack.

    Args:
        params: machinery parameters of a CodCorrect run
        status: the status of the jobmon workflow upon completion, where 'D'
            is success. All are defined at FILEPATH
        session: SQLAlchemy ORM session for the GBD database

    """
    url = Slack.SHADES_DMS if params.test else slack.MACHINERY_RUNS

    if status == WorkflowRunStatus.DONE:
        # success case, retrieve compare version and description
        with db_tools_core.session_scope(
            ConnectionDefinitions.GBD, session=session
        ) as session:
            res = (
                session.execute(
                    GbdDatabase.GET_COMPARE_VERSION_AND_DESCRIPTION,
                    params={"gbd_process_version_id": params.gbd_process_version_id},
                )
                .mappings()
                .first()
            )
        if res:
            compare_version_id = res["compare_version_id"]
            compare_version_description = res["compare_version_description"]
        else:
            # no matching compare version exists
            compare_version_id = "DOES NOT EXIST"
            compare_version_description = None
    else:
        # fail case, there won't be a compare version created
        compare_version_id = None
        compare_version_description = None

    slack.send_slack_message(
        construct_run_goodbye(
            params, status, compare_version_id, compare_version_description
        ),
        url=url,
    )


def send_slack_update(text: str, test: int) -> None:
    """Send a CodCorrect run update via Slack."""
    url = Slack.SHADES_DMS if test else slack.MACHINERY_RUNS
    slack.send_slack_message(construct_update(text), url=url)


def construct_run_greeting(
    params: MachineParameters, restart: bool, resume: bool
) -> slack.SlackPost:
    """
    Creates a Slack-friendly post for the start of a CoDCorrect run.

    For more information on constructing a dictionary that's convertable
    to a Slack-friendly json, see FILEPATH
    """
    indent = " " * 8
    percent_change_str = (
        indent
        + f"\n{indent}".join(
            f"{x} - {y}" for x, y in zip(params.year_start_ids, params.year_end_ids)
        )
        if params.year_start_ids
        else ""
    )

    title = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"CodCorrect version {params.version_id} ",
        },
    }
    divider = {"type": "divider"}
    internal_parameters = {
        "type": "section",
        "block_id": "section1",
        "text": {
            "type": "mrkdwn",
            "text": "_Internal parameters:_\n"
            f">*Release {params.release_id}*\n"
            f">*Draws:* {params.n_draws}\n"
            f">*Years:* {slack.to_str(params.year_ids)}\n"
            f">*Percent change years:*\n{percent_change_str}\n"
            f">*Location sets:* {slack.to_str(params.location_set_ids)}\n"
            f">*Cause sets:* {slack.to_str(params.cause_set_ids)}\n"
            f">*Measure ids:* {slack.to_str(params.measure_ids)}\n"
            f">*Databases:* {slack.to_str(params.databases)}\n"
            f">*Scatter version:* {params.scatter_version_id}",
        },
    }
    external_parameters = {
        "type": "section",
        "block_id": "section2",
        "text": {
            "type": "mrkdwn",
            "text": "_External parameters:_\n"
            f">*Population:* {params.population_version_id}\n"
            f">*Mortality:* {params.envelope_version_id}\n"
            f">*Life table:* {params.life_table_run_id}\n"
            f">*TMRLT:* {params.tmrlt_run_id}",
        },
    }

    return {"blocks": [title, divider, internal_parameters, external_parameters]}


def construct_run_goodbye(
    params: MachineParameters,
    status: str,
    compare_version_id: Optional[int],
    compare_version_description: Optional[str],
) -> slack.SlackPost:
    """
    Constructs a Slack-friendly post for the end of a CoDCorrect run.

    For more information on constructing a dictionary that's convertible
    to a Slack-friendly json, see FILEPATH
    """
    if status == WorkflowRunStatus.DONE:
        # Success case
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"CodCorrect version {params.version_id} " "has finished!",
            },
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {
                "type": "mrkdwn",
                "text": f"*COD output version:* {params.cod_output_version_id}\n"
                f"*GBD process version:* {params.gbd_process_version_id}\n"
                f"*Compare version:* {compare_version_id}\n"
                f">{compare_version_description}\n",
            },
        }
        diagnostic_link = {
            "type": "section",
            "block_id": "section2",
            "text": {
                "type": "mrkdwn",
                "text": f"<{Slack.CODCORRECT_DIAGNOSTIC}|CodCorrect results diagnostic>",
            },
        }
        return {"blocks": [title, info, diagnostic_link]}
    else:
        # Fail case
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"CodCorrect version {params.version_id} "
                f"did not complete successfully",
            },
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {"type": "mrkdwn", "text": f"*Status:* {status}"},
        }
        return {"blocks": [title, info]}


def construct_update(text: str) -> slack.SlackPost:
    """
    Construct the json-formatted SlackPost for a
    CodCorrect run update from the monitor.

    Args:
        text: formatted text to stick into a Slack block
    """
    title = {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    return {"blocks": [title]}
