from typing import Optional

import gbd_outputs_versions
from gbd_outputs_versions import slack
from jobmon.client.workflow import WorkflowRunStatus

from ihme_cc_sev_calculator.lib import constants, io, parameters, upload


def send_slack_greeting(params: parameters.Parameters) -> None:
    """Send greeting for start of run via Slack.

    TODO: Consider not mentioning
    input machinery versions for RRmax only runs where machinery isn't used.

    Args:
        params: run parameters
    """
    slack.send_slack_message(construct_run_greeting(params), raise_on_error=False)


def construct_run_greeting(params: parameters.Parameters) -> slack.SlackPost:
    """Creates a Slack-friendly post for the start of a run."""
    # What has happened? Run either submitted or resumed.
    if params.resume:
        action, emoji = "resumed", ":children_crossing:"
    else:
        action, emoji = "submitted", ":bar_chart:"

    by_cause_text = ">*By cause*\n" if params.by_cause else ""
    percent_change_text = (
        f">*Percent change years:* {slack.to_str(params.percent_change_years)}\n"
        if params.percent_change
        else ""
    )

    title = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"SEV Calculator version {params.version_id} has been {action} {emoji}",
        },
    }
    divider = {"type": "divider"}
    parameters = {
        "type": "section",
        "block_id": "section1",
        "text": {
            "type": "mrkdwn",
            "text": "_Parameters:_\n"
            f">*Release:* {params.release_id}\n"
            f">*Draws:* {params.n_draws}\n"
            f">*Years:* {slack.to_str(params.year_ids)}\n\n"
            f">*Location sets:* {slack.to_str(params.location_set_ids)}\n"
            f">*Measures:* {slack.to_str(params.measures)}\n"
            f">*Number of risks:* {len(params.all_rei_ids)}\n"
            f"{percent_change_text}"
            f"{by_cause_text}\n"
            "_Machinery versions:_\n"
            f">*Compare version:* {params.compare_version_id}\n"
            f">*PAF version:* {params.paf_version_id}\n"
            f">*COMO version:* {params.como_version_id}\n"
            f">*CodCorrect version:* {params.codcorrect_version_id}\n",
        },
    }

    return {"blocks": [title, divider, parameters]}


def send_slack_goodbye(params: parameters.Parameters, status: str) -> None:
    """Send goodbye for end of run via Slack.

    Args:
        params: dictionary of parameters
        status: the status of the jobmon workflow upon completion, where 'D'
            is success. All are defined at 
    """
    compare_version_id = io.read_final_compare_version_id(params.version_id)
    slack.send_slack_message(
        construct_run_goodbye(params, status, compare_version_id), raise_on_error=False
    )


def construct_run_goodbye(
    params: parameters.Parameters, status: str, compare_version_id: Optional[int]
) -> slack.SlackPost:
    """Constructs a Slack-friendly post for the end of a run."""
    if status == WorkflowRunStatus.DONE:
        # Success case

        # Pull GBD process version IDs for RRmax and SEVs, if possible
        # RRmax is always in measures, SEVs are optional
        rr_max_pv_id = upload.get_process_version_id(constants.RR_MAX, params)

        if constants.SEV in params.measures:
            sev_pv_id = upload.get_process_version_id(constants.SEV, params)
            sev_pv_text = f">*SEV process version:* {sev_pv_id}\n"
        else:
            sev_pv_text = ""

        # Pull CompareVersion with description, allowing for null compare_version_id
        if compare_version_id:
            cv = gbd_outputs_versions.CompareVersion(compare_version_id)
            cv_description = cv.compare_version_description
        else:
            cv = None
            cv_description = "NONE"

        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"SEV Calculator version {params.version_id} has finished! :clap:",
            },
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {
                "type": "mrkdwn",
                "text": f">*RRmax process version:* {rr_max_pv_id}\n"
                f"{sev_pv_text}"
                f">*Compare version:* {compare_version_id}\n"
                f">{cv_description}\n",
            },
        }
        return {"blocks": [title, info]}
    else:
        # Fail case
        title = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"SEV Calculator version {params.version_id} "
                f"did not complete successfully :cry:",
            },
        }
        info = {
            "type": "section",
            "block_id": "section1",
            "text": {"type": "mrkdwn", "text": f"*Status:* {status}"},
        }
        return {"blocks": [title, info]}
