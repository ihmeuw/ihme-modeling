"""Appends all diagnostics together that already exist from the most-detailed
location level, and creates diagnostics for location aggregate level.

All diagnostics only exist for deaths, not YLLs.
"""
import argparse
import logging

from codcorrect.lib import diagnostics
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--location_id',
        type=int,
        help="Location id the diagnostics job is parallelized for.",
        required=True
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        help="Sex id the diagnostics job is parallelized for.",
        required=True
    )
    parser.add_argument(
        '--version_id',
        type=int,
        help="The CodCorrect version we are uploading diagnostics for.",
        required=True
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_args()
    parameters = machinery.MachineParameters.load(args.version_id)

    logger.info(
        f"Begin diagnostic creation for location id {args.location_id}, sex id {args.sex_id}."
    )
    diagnostics.create_diagnostics(args.location_id, args.sex_id, parameters)
    logger.info("Finished.")


if __name__ == '__main__':
    main()
