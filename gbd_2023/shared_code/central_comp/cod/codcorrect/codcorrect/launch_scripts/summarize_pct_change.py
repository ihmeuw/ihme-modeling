from argparse import ArgumentParser, Namespace
import logging

from codcorrect.legacy.summarize import summarize_pct_change
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--parent_dir',
        type=str,
        required=True,
        help='path to base codcorrrect directory'
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
        '--year_start_id',
        type=int,
        required=True,
        help='start year to use for pct_change'
    )
    parser.add_argument(
        '--year_end_id',
        type=int,
        required=True,
        help='end year to use for pct_change'
    )
    parser.add_argument(
        '--measure_id',
        type=int,
        required=True,
        help='measure_id to summarize'
    )
    parser.add_argument(
        '--version_id',
        type=str,
        required=True,
        help='Version of codcorrect run')
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    parameters = machinery.MachineParameters.load(args.version_id)

    logger.info(
        f"Summarizing location_id: {args.location_id}, measure_id: {args.measure_id}, "
        f"for year_start_id: {args.year_start_id} to year_end_id {args.year_end_id}."
    )
    summarize_pct_change(
        args.release_id,
        args.location_id,
        args.measure_id,
        args.year_start_id,
        args.year_end_id,
        parameters,
    )
    logger.info("Summarization completed.")


if __name__ == '__main__':
    main()
