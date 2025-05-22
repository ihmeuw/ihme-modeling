"""Monitor jobmon form the sidelines, sending updates via Slack."""
from argparse import ArgumentParser, Namespace
import logging

import gbd_outputs_versions

from codcorrect.legacy.parameters import machinery
from codcorrect.legacy.utils.constants import Jobmon, Slack
from codcorrect.lib.workflow import monitor
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='CodCorrect version id of the run'
    )
    parser.add_argument(
        "--workflow_args",
        type=str,
        required=True,
        help="Run's workflow args in jobmon."
    )
    parser.add_argument(
        "--resume",
        type=int,
        required=True,
        help="Whether the run was resumed or not."
    )
    parser.add_argument(
        "--test",
        type=int,
        required=True,
        help="Whether the run is a test."
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    logger.info(f"Spinning up monitor for workflow_args {args.workflow_args}")

    parameters = machinery.MachineParameters.load(args.version_id)
    workflow_id = monitor.get_workflow_id(args.workflow_args)

    gbd_outputs_versions.slack.send_jobmon_gui_link(
        machinery_name=Jobmon.TOOL,
        version_id=args.version_id,
        url=gbd_outputs_versions.slack.MACHINERY_RUNS if not args.test else Slack.SHADES_DMS,
        workflow_id=workflow_id,
    )
    res = monitor.monitor_workflow(
        workflow_id=workflow_id,
        resume=args.resume,
        test=args.test,
    )

    # Report result and fail in the case the workflow failed so monitoring can be resumed
    # with other failed jobs
    logger.info(f"Monitoring complete. End status: {res}")
    if res:
        raise RuntimeError(f"Workflow failed, failing monitor job so it can be resumed.")


if __name__ == '__main__':
    main()
