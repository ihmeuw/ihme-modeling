"""Helper functions for sending Slack messages about the machinery."""
import json
import requests
from typing import Any, Dict, List, Optional
import warnings

from db_tools.ezfuncs import query
from jobmon.client.workflow import WorkflowRunStatus

from fauxcorrect.parameters.machinery import MachineParameters
from fauxcorrect.utils.constants import ConnectionDefinitions, Slack
from fauxcorrect.queries.queries import GbdDatabase

SlackPost = Dict[str, Dict[str, str]]


def send_slack_json(url: str, data: SlackPost) -> None:
    """Send a Slack message in the form of a json to the given url.

    Args:
        url: url to use to curl Slack message to
        data: json-friendly dictionary object that conforms to Slack's
            rules for webhook messages

    Raises:
        RuntimeError: if the HTML request response code is not 200
    """
    response = requests.post(
        url,
        data=json.dumps(data),
        headers={'Content-Type': "application/json"}
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Slack message request returned error {response.status_code} with response: "
            f"{response.text}"
        )


def send_slack_greeting(params: MachineParameters, restart: bool, resume: bool) -> None:
    """Send greeting for start of run via Slack.

    Restart and resume cannot both be true.

    Args:
        params: machinery parameters of a CodCorrect run
        restart: was the run restarted?
        resume: was the run resumed?
    """
    url = Slack.CHANNEL if params.test else Slack.MACHINERY_RUNS

    try:
        send_slack_json(url, construct_run_greeting(params, restart, resume))
    except Exception as e:
        warnings.warn(f"Failed to send Slack greeting: {e}")


def send_slack_goodbye(params: MachineParameters, status: str) -> None:
    """Send goodbye for end of run via Slack.

    Args:
        params: machinery parameters of a CodCorrect run
        status: the status of the jobmon workflow upon completion, where 'D'
            is success.

    """
    url = Slack.CHANNEL if params.test else Slack.MACHINERY_RUNS

    if status == WorkflowRunStatus.DONE:
        # success case, retrieve compare version and description
        res = query(
            GbdDatabase.GET_COMPARE_VERSION_AND_DESCRIPTION,
            conn_def=ConnectionDefinitions.GBD,
            parameters={
                "gbd_process_version_id": params.gbd_process_version_id
            }
        )
        if not res.empty:
            compare_version_id = res.compare_version_id.iat[0]
            compare_version_description = res.compare_version_description.iat[0]
        else:
            # no matching compare version exists; probably a test run
            compare_version_id = "DOES NOT EXIST"
            compare_version_description = None
    else:
        # fail case, there won't be a compare version created
        compare_version_id = None
        compare_version_description = None

    try:
        send_slack_json(
            url,
            construct_run_goodbye(
                params, status, compare_version_id, compare_version_description
            )
        )
    except Exception as e:
        warnings.warn(f"Failed to send Slack goodbye: {e}")


def send_slack_update(text: str) -> None:
    """Send a CodCorrect run update via Slack."""
    try:
        send_slack_json(Slack.CHANNEL, construct_update(text))
    except Exception as e:
        warnings.warn(f"Failed to send Slack update: {e}")


def construct_run_greeting(params: MachineParameters, restart: bool, resume: bool) -> SlackPost:
    """
    Creates a Slack-friendly post for the start of a Cod/FauxCorrect run.

    For more information on constructing a dictionary that's convertable
    to a Slack-friendly json, see https://api.slack.com/tools/block-kit-builder
    """
    def to_str(a: List[Any]) -> str:
        """
        Short-hand for converting a list of anything into
        a comma-separated string. Helpful since list comprehensions
        don't play well within formatted strings.

        Ex:
            [1, 2, 3] -> "1, 2, 3"
        """
        if a is None:
            return ""

        return ", ".join(str(item) for item in a)

    indent = " " * 8
    percent_change_str = indent + f"\n{indent}".join(
        f"{x} - {y}" for x, y in
        zip(params.year_start_ids, params.year_end_ids)
    ) if params.year_start_ids else ""

    # What has happened? Run either submitted, restarted or resumed.
    if restart:
        action, emoji = "restarted", ":recycle:"
    elif resume:
        action, emoji = "resumed", ":children_crossing:"
    else:
        action, emoji = "submitted", ":tada:"

    title = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text":
                f"CodCorrect version {params.version_id} "
                f"has been {action} {emoji}"
        }
    }
    divider = {
        "type": "divider"
    }
    internal_parameters = {
        "type": "section",
        "block_id": "section1",
        "text": {
            "type": "mrkdwn",
            "text":
                "_Internal parameters:_\n" +
                f">*GBD round {params.gbd_round_id}, {params.decomp_step}*\n" +
                f">*Draws:* {params.n_draws}\n" +
                f">*Years:* {to_str(params.year_ids)}\n" +
                f">*Percent change years:*\n{percent_change_str}\n" +
                f">*Location sets:* {to_str(params.location_set_ids)}\n" +
                f">*Cause sets:* {to_str(params.cause_set_ids)}\n" +
                f">*Measure ids:* {to_str(params.measure_ids)}\n" +
                f">*Databases:* {to_str(params.databases)}\n" +
                f">*Scatter version:* {params.scatter_version_id}"
        }
    }
    external_parameters = {
        "type": "section",
        "block_id": "section2",
        "text": {
            "type": "mrkdwn",
            "text":
                "_External parameters:_\n" +
                f">*Population:* {params.population_version_id}\n" +
                f">*Mortality:* {params.envelope_version_id}\n" +
                f">*Life table:* {params.life_table_run_id}\n" +
                f">*TMRLT:* {params.tmrlt_run_id}"
        }
    }

    return {"blocks": [title, divider, internal_parameters, external_parameters]}


def construct_run_goodbye(
        params: int,
        status: str,
        compare_version_id: Optional[int],
        compare_version_description: Optional[str]
) -> SlackPost:
    """
    Constructs a Slack-friendly post for the end of a Faux/CodCorrect run.

    For more information on constructing a dictionary that's convertable
    to a Slack-friendly json, see https://api.slack.com/tools/block-kit-builder
    """
    if status == WorkflowRunStatus.DONE:
        # Success case
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":
                    f"CodCorrect version {params.version_id} "
                    "has finished! :clap:"
            }
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {
                "type": "mrkdwn",
                "text":
                    f"*COD output version:* {params.cod_output_version_id}\n" +
                    f"*GBD process version:* {params.gbd_process_version_id}\n" +
                    f"*Compare version:* {compare_version_id}\n"
                    f">{compare_version_description}\n"
            }
        }
        diagnostic_link = {
            "type": "section",
            "block_id": "section2",
            "text": {
                "type": "mrkdwn",
                "text":
                    f"<{Slack.CODCORRECT_DIAGNOSTIC}|CodCorrect results diagnostic>"
            }
        }
        return {"blocks": [title, info, diagnostic_link]}
    else:
        # Fail case
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text":
                    f"CodCorrect version {params.version_id} "
                    f"did not complete successfully :cry:"
            }
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {
                "type": "mrkdwn",
                "text":
                    f"*Status:* {status}"
            }
        }
        return {"blocks": [title, info]}


def construct_update(text: str) -> SlackPost:
    """
    Construct the json-formatted SlackPost for a
    CodCorrect run update from the monitor.

    Args:
        text: formatted text to stick into a Slack block
    """
    title = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text
        }
    }

    return {"blocks": [title]}
