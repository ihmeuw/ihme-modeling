"""Appends all diagnostics together that already exist from the most-detailed
location level, and creates diagnostics for location aggregate level.

All diagnostics only exist for deaths, not YLLs.
"""
import argparse

import pandas as pd

from fauxcorrect import diagnostics
from fauxcorrect.job_swarm import task_templates
from fauxcorrect.parameters.machinery import CoDCorrectParameters
from fauxcorrect.utils.logging import setup_logging


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
    args = parse_args()
    params = CoDCorrectParameters.recreate_from_version_id(
        version_id=args.version_id
    )

    logger = setup_logging(
        params.parent_dir, task_templates.Diagnostics.template_name, args
    )

    logger.info(
        f"Begin diagnostic creation for location id {args.location_id}, sex id {args.sex_id}."
    )
    diagnostics.create_diagnostics(args.location_id, args.sex_id, params)
    logger.info("Finished.")


if __name__ == '__main__':
    main()
