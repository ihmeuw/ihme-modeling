from argparse import ArgumentParser, Namespace
import logging

from codcorrect.legacy.draw_validations import read_input_draws, validate_draws
from codcorrect.legacy.utils import constants
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='The machine version id associated with this run.'
    )
    parser.add_argument('--model_version_id', type=int)
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    parameters = machinery.MachineParameters.load(args.version_id)

    logger.info(f"Validating input draws for model_version_id {args.model_version_id}.")
    logger.info("Reading input draws.")
    draws = read_input_draws(parameters, model_version_id=args.model_version_id)

    logger.info("Validating input draws.")
    draws = validate_draws(parameters, args.model_version_id, draws)

    logger.info("Saving validated draws to filesystem.")
    parameters.file_system.save_validated_draws(draws, args.model_version_id)

    logger.info("Validation complete.")


if __name__ == '__main__':
    main()
