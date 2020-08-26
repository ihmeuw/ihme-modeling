from argparse import ArgumentParser, Namespace
import logging

from fauxcorrect.draw_validations import (
    read_input_draws, save_validated_draws, validate_draws
)
from fauxcorrect.parameters.master import (
    CoDCorrectParameters,
    FauxCorrectParameters
)
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
    if args.machine_process == constants.GBD.Process.Name.CODCORRECT:
        params = CoDCorrectParameters.recreate_from_version_id(
            version_id=args.version_id
        )
    elif args.machine_process == constants.GBD.Process.Name.FAUXCORRECT:
        params = FauxCorrectParameters.recreate_from_version_id(
            version_id=args.version_id
        )
    else:
        raise ValueError(
            f'--machine_process argument must be '
            f'"{constants.GBD.Process.Name.CODCORRECT}" or '
            f'"{constants.GBD.Process.Name.FAUXCORRECT}". '
            f'Recieved "{args.machine_process}".'
        )
    setup_logging(
        params.parent_dir,
        constants.DAG.Tasks.Type.VALIDATE,
        args
    )
    logging.info(
        f"Validating input draws for model_version_id {args.model_version_id}."
    )
    logging.info("Reading input draws.")

    draws = read_input_draws(
        params,
        model_version_id=args.model_version_id
    )
    logging.info("Validating input draws.")
    draws = validate_draws(
        params,
        args.model_version_id,
        draws
    )
    logging.info("Saving validated draws to filesystem.")
    save_validated_draws(
        params,
        args.model_version_id,
        draws
    )
    logging.info("Validation complete.")


if __name__ == '__main__':
    main()
