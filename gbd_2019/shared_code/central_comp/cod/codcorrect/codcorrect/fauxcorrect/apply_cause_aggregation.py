from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.parameters import master
from fauxcorrect.aggregate_causes import AggregateCauses
from fauxcorrect.utils.constants import DAG
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base codcorrect directory'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to apply cause aggregation'
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        required=True,
        help='sex_id to apply cause aggregation'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(args.parent_dir, DAG.Tasks.Type.CAUSE_AGG, args)

    codcorrect = master.CoDCorrectParameters.recreate_from_version_id(
        args.version_id)
    logging.info(
        f"Aggregating causes for location_id: {args.location_id}, sex_id: "
        f"{args.sex_id}"
    )
    ac = AggregateCauses(
        args.parent_dir, args.location_id, args.sex_id, codcorrect)
    ac.run()
    logging.info("Cause aggregation completed.")


if __name__ == '__main__':
    main()
