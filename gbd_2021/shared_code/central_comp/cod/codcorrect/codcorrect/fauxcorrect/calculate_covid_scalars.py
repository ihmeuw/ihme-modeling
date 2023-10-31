"""Calculate COVID mortality scalar for non-fatal work."""
import argparse

import pandas as pd

from fauxcorrect import covid_scalars
from fauxcorrect.job_swarm import task_templates
from fauxcorrect.parameters.machinery import CoDCorrectParameters
from fauxcorrect.utils.constants import Columns, ModelVersionTypeId
from fauxcorrect.utils.logging import setup_logging


def parse_args() -> argparse.Namespace:
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--location_id',
        type=int,
        help="Location id the covid scalars job is parallelized for.",
        required=False
    )
    parser.add_argument(
        '--sex_id',
        type=int,
        help="Sex id the covid scalars job is parallelized for.",
        required=False
    )
    parser.add_argument(
        '--version_id',
        type=int,
        help="The CodCorrect version we are calculating covid scalars for.",
        required=True
    )
    parser.add_argument(
        '--compile',
        action='store_true',
        help=("Flag to compile all COVID scalar summaries into a single file. "
              "Only run after all COVID scalars have been generated.")
    )
    args = parser.parse_args()

    # Only if 'compile' is passed can the other arguments be None
    if not args.compile and None in [args.version_id]:
        raise ValueError(
            "'compile' flag not passed; must supply arguments for location_id and sex_id"
        )

    return args


def main() -> None:
    args = parse_args()
    params = CoDCorrectParameters.recreate_from_version_id(
        version_id=args.version_id
    )

    if not params.covid_cause_ids:
        return

    if not args.compile:
        logger = setup_logging(
            params.parent_dir, task_templates.CovidScalarsCalculation.template_name, args
        )
        logger.info(
            f"Begin COVID scalar calculation for location id {args.location_id}, "
            f"sex id {args.sex_id}."
        )
        logger.info(f"Creating scalars for the following causes: {params.covid_cause_ids}")

        covid_scalars.calculate_covid_scalars(
            params.parent_dir,
            params.covid_cause_ids,
            args.sex_id,
            args.location_id,
            params.draw_cols,
            params.test,
        )
    else:
        logger = setup_logging(
            params.parent_dir, task_templates.CovidScalarsCompilation.template_name, args
        )
        covid_scalars.compile_covid_scalars_summaries(params.parent_dir, params.version_id)
        logger.info(f"Compiling COVID scalar summaries")

    logger.info("Finished.")


if __name__ == '__main__':
    main()
