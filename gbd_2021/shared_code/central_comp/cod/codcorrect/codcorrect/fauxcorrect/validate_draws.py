from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.draw_validations import (
    read_input_draws, save_validated_draws, validate_draws
)
from fauxcorrect.job_swarm import task_templates
from fauxcorrect.parameters import machinery
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='The machine version id associated with this run.'
    ),
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        choices=constants.GBD.Process.Name.OPTIONS,
        help='codcorrect or fauxcorrect'
    )
    parser.add_argument('--model_version_id', type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    params = machinery.recreate_from_process_and_version_id(
        version_id=args.version_id, machine_process=args.machine_process
    )

    logger = setup_logging(
        params.parent_dir,
        task_templates.Validate.template_name,
        args
    )

    logger.info(
        f"Validating input draws for model_version_id {args.model_version_id}."
    )
    logger.info("Reading input draws.")

    draws = read_input_draws(
        params,
        model_version_id=args.model_version_id
    )
    logger.info("Validating input draws.")
    draws = validate_draws(
        params,
        args.model_version_id,
        draws
    )
    logger.info("Saving validated draws to filesystem.")
    save_validated_draws(
        params,
        args.model_version_id,
        draws
    )
    logger.info("Validation complete.")


if __name__ == '__main__':
    main()
