from argparse import ArgumentParser, Namespace
import logging

from codcorrect.legacy.aggregate_causes import AggregateCauses
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


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
    logs.configure_logging()
    args = parse_arguments()

    parameters = machinery.MachineParameters.load(args.version_id)
    logger.info(
        f"Aggregating causes for location_id: {args.location_id}, sex_id: "
        f"{args.sex_id}"
    )
    ac = AggregateCauses(
        args.parent_dir, args.location_id, args.sex_id, parameters)
    ac.run()
    logger.info("Cause aggregation completed.")


if __name__ == '__main__':
    main()
