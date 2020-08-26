from argparse import ArgumentParser, Namespace
from enum import Enum

from fauxcorrect.backfill import (
    backfill_location_or_cause,
    backfill_locations_or_causes,
    create_backfill_mapping
)


class Action(Enum):
    MAP = 'create_mapping'
    BACKFILL = 'backfill'


def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean value, must be "True" or "False"')
    return s == 'True'


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--action',
        type=Action,
        required=True,
        choices=list(Action),
        help='create_mapping or backfill'
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base fauxcorrect directory'
    )
    parser.add_argument(
        '--is_location_backfill',
        type=boolean_string,
        required=True,
        help='whether this is being called with location or cause'
    )
    parser.add_argument(
        '--previous_gbd_round_id',
        type=int,
        help='GBD round ID with which to backfill'
    )
    parser.add_argument(
        '--current_gbd_round_id',
        type=int,
        help='GBD round ID containing new locations or causes'
    )
    parser.add_argument(
        '--set_id',
        type=int,
        help='location set ID or cause set iD'
    )
    parser.add_argument(
        '--ids',
        type=int,
        nargs='+',
        help='one or multiple location IDs or cause IDs'
    )
    parser.add_argument(
        '--as_dict',
        type=bool,
        default=False,
        help='whether backfill should be returned as dict (default list)'
    )
    return parser.parse_args()


def validate_arguments(args: Namespace) -> None:
    if (
        args.action == Action.MAP and
        (not args.previous_gbd_round_id or
         not args.current_gbd_round_id or
         not args.set_id)
    ):
        raise ValueError(
            "Must provide previous_gbd_round_id, current_gbd_round_id, and "
            "set_id when creating mapping."
        )
    elif args.action == Action.BACKFILL and not args.ids:
        raise ValueError("Must provide ids when running backfill.")


def main():
    args = parse_arguments()
    validate_arguments(args)
    if args.action == Action.MAP:
        create_backfill_mapping(
            args.parent_dir,
            args.previous_gbd_round_id,
            args.current_gbd_round_id,
            args.set_id if args.is_location_backfill else None,
            args.set_id if not args.is_location_backfill else None
        )
    elif args.action == Action.BACKFILL:
        backfill_func = (
            backfill_locations_or_causes
            if len(args.ids) > 1
            else backfill_location_or_cause
        )
        backfill_args = [
            args.parent_dir,
            args.ids[0] if len(args.ids) == 1 else args.ids,
            args.is_location_backfill
        ]
        if args.as_dict:
            backfill_args.append(True)
        return backfill_func(*backfill_args)


if __name__ == '__main__':
    main()
