from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

from fauxcorrect.parameters import master
from fauxcorrect.correct import apply_correction, cache_spacetime_restrictions
from fauxcorrect.utils.constants import DAG
from fauxcorrect.utils.logging import setup_logging


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
    parser.add_argument('--gbd_round_id', type=int, required=True)
    parser.add_argument('--version_id', type=int)
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--sex_id', type=int)
    parser.add_argument(
        '--env_version_id',
        type=int,
        help='mortality envelope version id used to apply deaths correction'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(args.parent_dir, DAG.Tasks.Type.CORRECT, args)
    if args.action == Action.CACHE:
        logging.info("Caching spacetime restrictions")
        cache_spacetime_restrictions(args.parent_dir, args.gbd_round_id)
        logging.info("Completed.")
    elif args.action == Action.CORRECT:
        codcorrect = master.CoDCorrectParameters.recreate_from_version_id(
            args.version_id
        )
        logging.info(
            f"Applying deaths correction to location_id: {args.location_id} "
            f"and sex_id: {args.sex_id} using all-cause mortality envelope "
            f"run_id: {args.env_version_id}."
        )
        apply_correction(
            args.parent_dir, args.location_id, args.sex_id, codcorrect
        )
        logging.info("Deaths correction completed.")


if __name__ == '__main__':
    main()