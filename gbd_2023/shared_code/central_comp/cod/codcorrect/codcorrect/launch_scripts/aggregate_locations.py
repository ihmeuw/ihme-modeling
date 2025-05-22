from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

import db_queries

from codcorrect.legacy.location_aggregation import aggregate_locations
from codcorrect.lib import db
from codcorrect.lib.utils import logs, files
from codcorrect.legacy.parameters import machinery
from codcorrect.legacy.utils.constants import Columns

logger = logging.getLogger(__name__)


class Action(Enum):
    CACHE = 'cache'
    LOC_AGG = 'location_aggregation'


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--action',
        type=Action,
        required=True,
        choices=list(Action),
        help='cache or location_aggregation'
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base fauxcorrect directory'
    )
    parser.add_argument('--release_id', type=int, required=True)
    parser.add_argument('--aggregation_type', type=str)
    parser.add_argument('--location_set_id', type=int)
    parser.add_argument('--year_id', type=int)
    parser.add_argument('--measure_id', type=int)
    parser.add_argument('--version_id', type=int, required=True)
    return parser.parse_args()


def validate_arguments(
        action: Action,
        measure_id: int,
        location_set_id: int,
        aggregation_type: str,
        year_id: int
) -> None:
    missing_id = (
        not aggregation_type or not measure_id or not location_set_id or
        not year_id
    )
    if action == Action.LOC_AGG and missing_id:
        raise ValueError(
            'Must provide aggregation_type, measure_id, location_set_id, '
            'and year_id for aggregating locations'
        )


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    validate_arguments(
        args.action,
        args.measure_id,
        args.location_set_id,
        args.aggregation_type,
        args.year_id
    )
    parameters = machinery.MachineParameters.load(args.version_id)

    if args.action == Action.CACHE:
        logger.info("Caching regional scalars.")
        reg_scalars = db_queries.get_regional_scalars(args.release_id)
        cache_columns = [Columns.YEAR_ID, Columns.LOCATION_ID, Columns.MEAN]
        reg_scalars = reg_scalars[cache_columns]
        parameters.file_system.cache_regional_scalars(reg_scalars)
        logger.info("Completed.")
    elif args.action == Action.LOC_AGG:
        # Decipher aggregation type
        logger.info(
            f"Beginning location aggregation for aggregation_type: "
            f"{args.aggregation_type}, location_set_id: "
            f"{args.location_set_id}, measure_id: {args.measure_id}, "
            f"and year_id: {args.year_id}."
        )
        aggregate_locations(
            args.aggregation_type,
            args.measure_id,
            args.release_id,
            args.location_set_id,
            args.year_id,
            parameters,
        )
        logger.info("Location aggregation completed.")


if __name__ == '__main__':
    main()
