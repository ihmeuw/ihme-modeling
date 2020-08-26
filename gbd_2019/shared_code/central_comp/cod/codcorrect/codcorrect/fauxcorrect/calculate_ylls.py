from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging
from fauxcorrect.ylls import cache_pred_ex, calculate_ylls


class Action(Enum):
    CACHE = 'cache'
    CALC = 'calc'


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--action',
        type=Action,
        required=True,
        choices=list(Action),
        help='cache or calc'
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        choices=constants.GBD.Process.Name.OPTIONS,
        help='codcorrect or fauxcorrect'
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base fauxcorrect directory'
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        help='GBD round ID - required to cache pred_ex'
    )
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--sex_id', type=int)
    return parser.parse_args()


def validate_arguments(
        action: Action,
        location_id: int,
        sex_id: int,
        machine_process: str
) -> None:
    missing_id = not location_id or not sex_id or not machine_process
    if action == Action.CALC and missing_id:
        raise ValueError(
            'Must provide location_id, sex_id, and machine_process '
            'for calculating YLLs.'
        )


def main() -> None:
    args = parse_arguments()
    setup_logging(
        args.parent_dir,
        constants.DAG.Tasks.Type.VALIDATE,
        args
    )
    validate_arguments(
        args.action, args.location_id, args.sex_id, args.machine_process
    )
    if args.action == Action.CACHE:
        cache_pred_ex(args.parent_dir, args.gbd_round_id)
    elif args.action == Action.CALC:
        logging.info(
            f"Calculating YLLs for location_id: {args.location_id}, "
            f"sex_id: {args.sex_id}, machine_process: {args.machine_process}."
        )
        calculate_ylls(
            args.machine_process,
            args.parent_dir,
            args.location_id,
            args.sex_id
        )
        logging.info("Calculating YLLs completed.")


if __name__ == '__main__':
    main()
