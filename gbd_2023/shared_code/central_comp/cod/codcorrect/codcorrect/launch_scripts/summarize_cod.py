from argparse import ArgumentParser, Namespace
import logging

from codcorrect.legacy.summarize import summarize_cod
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
        help='path to base directory'
    )
    parser.add_argument(
        '--release_id',
        type=int,
        required=True,
        help='release_id to summarize.'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='location_id to summarize'
    )
    parser.add_argument(
        '--year_id',
        type=int,
        required=True,
        help='year_id to summarize'
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    logger.info(f"Summarizing location_id: {args.location_id}, and year_id: {args.year_id}.")
    parameters = machinery.MachineParameters.load(args.version_id)

    summarize_cod(
        args.release_id,
        args.location_id,
        args.year_id,
        parameters,
    )
    logger.info("Summarization completed.")


if __name__ == '__main__':
    main()
