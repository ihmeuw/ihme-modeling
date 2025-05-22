from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

from codcorrect.legacy.correct import apply_correction
from codcorrect.legacy.parameters import machinery
from codcorrect.lib import db
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


class Action(Enum):
    CACHE = 'cache'
    CORRECT = 'correct'


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--action',
        type=Action,
        required=True,
        choices=list(Action),
        help='cache or correct'
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base codcorrect directory'
    )
    parser.add_argument('--release_id', type=int, required=True)
    parser.add_argument('--version_id', type=int, required=True)
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--sex_id', type=int)
    parser.add_argument(
        '--env_version_id',
        type=int,
        help='mortality envelope version id used to apply deaths correction'
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()

    parameters = machinery.MachineParameters.load(args.version_id)

    if args.action == Action.CACHE:
        logger.info("Caching spacetime restrictions")
        restrictions = db.get_all_spacetime_restrictions(
            args.release_id
        )
        parameters.file_system.cache_spacetime_restrictions(restrictions)
        logger.info("Completed.")
    elif args.action == Action.CORRECT:
        logger.info(
            f"Applying deaths correction to location_id: {args.location_id} "
            f"and sex_id: {args.sex_id} using all-cause mortality envelope "
            f"run_id: {args.env_version_id}."
        )
        apply_correction(args.location_id, args.sex_id, parameters)
        logger.info("Deaths correction completed.")


if __name__ == '__main__':
    main()
