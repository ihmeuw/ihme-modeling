from argparse import ArgumentParser, Namespace
from enum import Enum
import logging

from codcorrect.lib import db
from codcorrect.legacy.utils import constants
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs
from codcorrect.legacy.ylls import calculate_ylls

logger = logging.getLogger(__name__)


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
        '--version_id',
        type=str,
        required=True,
        help='Version of the codcorrect run'
    )
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base codcorrect directory'
    )
    parser.add_argument(
        '--release_id',
        type=int,
        help='Release ID - required to cache pred_ex'
    )
    parser.add_argument('--location_id', type=int)
    parser.add_argument('--sex_id', type=int)
    return parser.parse_args()


def validate_arguments(
        action: Action,
        location_id: int,
        sex_id: int
) -> None:
    missing_id = not location_id or not sex_id 
    if action == Action.CALC and missing_id:
        raise ValueError(
            'Must provide location_id and sex_id for calculating YLLs.'
        )


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    validate_arguments(
        args.action, args.location_id, args.sex_id
    )
    parameters = machinery.MachineParameters.load(args.version_id)

    if args.action == Action.CACHE:
        logger.info("Caching predicted life expectancy.")
        predicted_expectancy = db.get_pred_ex(
            life_table_run_id=parameters.life_table_run_id,
            tmrlt_run_id=parameters.tmrlt_run_id,
            expected_age_group_ids=parameters.most_detailed_age_group_ids
        )
        parameters.file_system.cache_pred_ex(predicted_expectancy)
        logger.info("Caching complete.")
    elif args.action == Action.CALC:
        logger.info(
            f"Calculating YLLs for location_id: {args.location_id}, "
            f"sex_id: {args.sex_id}."
        )
        calculate_ylls(
            args.location_id,
            args.sex_id,
            parameters,
        )
        logger.info("Calculating YLLs completed.")


if __name__ == '__main__':
    main()
