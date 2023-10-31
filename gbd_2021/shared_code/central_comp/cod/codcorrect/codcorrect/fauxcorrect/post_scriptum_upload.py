from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.job_swarm import task_templates
from fauxcorrect import post_scriptum
from fauxcorrect.utils import constants
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
        '--process_version_id',
        type=int,
        required=True,
        help='Process version id created at the start of the run'
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        choices=constants.GBD.Process.Name.OPTIONS,
        help='codcorrect or fauxcorrect'
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        required=True,
        help='gbd_round_id of the run'
    )
    parser.add_argument(
        '--decomp_step',
        type=str,
        required=True,
        help='decomp step of the run'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    logger = setup_logging(
        args.parent_dir,
        task_templates.PostScriptum.template_name,
        args
    )
    logger.info(
        f"Making post-scriptum updates for {args.machine_process}, process "
        f"version {args.process_version_id} for GBD round {args.gbd_round_id} "
        f"{args.decomp_step}"
    )
    compare_version_id = post_scriptum.post_scriptum_upload(
        process_version_id=args.process_version_id,
        machine_process=args.machine_process,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step
    )
    logger.info(f"Compare version {compare_version_id} has been created.")


if __name__ == '__main__':
    main()
