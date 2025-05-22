from argparse import ArgumentParser, Namespace
import logging

from gbd.constants import measures

from codcorrect.legacy.shocks import append_shocks
from codcorrect.legacy.utils import constants
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base directory for the run'
    )
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='The version id of the codcorrect run'
    )
    parser.add_argument(
        '--measure_ids',
        type=int,
        nargs="+",
        required=True,
        choices=[measures.DEATH, measures.YLL],
        help='1 and/or 4, e.g. [1], [4], or [1,4]'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to append shocks'
    )
    parser.add_argument(
        '--most_detailed_location',
        type=bool,
        required=True,
        help='whether or not the location_id is a most-detailed location'
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        required=True,
        help='sex_id to append shocks'
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    parameters = machinery.MachineParameters.load(args.version_id)
    logger.info(
        f"Appending shocks for location_id: {args.location_id}, "
        f"sex_id: {args.sex_id}, measure_ids: {args.measure_ids}."
    )
    append_shocks(
        args.measure_ids,
        args.location_id,
        args.sex_id,
        parameters,
    )
    logger.info("Appending shocks completed.")


if __name__ == '__main__':
    main()
