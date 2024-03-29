from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

from fauxcorrect.job_swarm import task_templates
from fauxcorrect.location_aggregation import (
    cache_regional_scalars,
    aggregate_locations
)
from fauxcorrect.parameters import machinery
from fauxcorrect.utils.logging import setup_logging



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
    parser.add_argument('--gbd_round_id', type=int, required=True)
    parser.add_argument('--decomp_step', type=str, required=True)
    parser.add_argument('--aggregation_type', type=str)
    parser.add_argument('--location_set_id', type=int)
    parser.add_argument('--year_id', type=int)
    parser.add_argument('--measure_id', type=int)
    parser.add_argument('--machine_process', type=str, required=True)
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
    args = parse_arguments()
    logger = setup_logging(
        args.parent_dir,
        task_templates.LocationAggregation.template_name,
        args
    )
    validate_arguments(
        args.action,
        args.measure_id,
        args.location_set_id,
        args.aggregation_type,
        args.year_id
    )
    params = machinery.recreate_from_process_and_version_id(
        version_id=args.version_id, machine_process=args.machine_process
    )

    if args.action == Action.CACHE:
        logger.info("Caching regional scalars.")
        cache_regional_scalars(args.parent_dir, args.gbd_round_id)
        logger.info("Completed.")
    elif args.action == Action.LOC_AGG:
        # Deciper aggregation type
        aggregation_type = args.aggregation_type.replace("_", "/")

        logger.info(
            f"Beginning location aggregation for aggregation_type: "
            f"{args.aggregation_type}, location_set_id: "
            f"{args.location_set_id}, measure_id: {args.measure_id}, "
            f"and year_id: {args.year_id}."
        )
        aggregate_locations(
            aggregation_type,
            args.parent_dir,
            args.measure_id,
            args.gbd_round_id,
            args.decomp_step,
            args.location_set_id,
            args.year_id,
            params
        )
        logger.info("Location aggregation completed.")


if __name__ == '__main__':
    main()
