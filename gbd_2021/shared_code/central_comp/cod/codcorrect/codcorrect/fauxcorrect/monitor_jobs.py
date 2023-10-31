"""Monitor jobmon form the sidelines, sending updates via Slack."""
from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.job_swarm import task_templates
from fauxcorrect.job_swarm import monitor
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base directory for the run'
    )
    parser.add_argument(
        "--workflow_args",
        type=str,
        required=True,
        help="Run's workflow args in jobmon."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    logger = setup_logging(
        args.parent_dir,
        task_templates.Monitor.template_name,
        args
    )
    logger.info(f"Spinning up monitor for workflow_args {args.workflow_args}")

    res = monitor.monitor_workflow(
        workflow_id=monitor.get_workflow_id(args.workflow_args)
    )

    logger.info(f"Monitoring complete. End status: {res}")


if __name__ == '__main__':
    main()
