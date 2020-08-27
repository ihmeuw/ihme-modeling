import argparse
import logging

from fauxcorrect import mortality_inputs
from fauxcorrect.parameters.master import (
    CoDCorrectParameters,
    FauxCorrectParameters
)
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--mort_process',
        type=str,
        required=True,
        choices=mortality_inputs.ValidMortalityProcessInputs.OPTIONS,
        help='cache population or envelope inputs'
    )
    parser.add_argument(
        '--machine_process',
        type=str,
        required=True,
        choices=constants.GBD.Process.Name.OPTIONS,
        help='codcorrect or fauxcorrect'
    )
    parser.add_argument(
        '--version_id',
        type=int,
        required=True,
        help='death machine version id the pop/env inputs are for'
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    if args.machine_process == constants.GBD.Process.Name.CODCORRECT:
        version = CoDCorrectParameters.recreate_from_version_id(
            version_id=args.version_id
        )
    elif args.machine_process == constants.GBD.Process.Name.FAUXCORRECT:
        version = FauxCorrectParameters.recreate_from_version_id(
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
        version.parent_dir,
        constants.DAG.Tasks.Type.CACHE,
        args
    )

    logging.info("Validating input arguments")
    mortality_inputs.validate_mortality_process_argument(args.mort_process)

    logging.info(f"Caching {args.mort_process}.")
    mortality_inputs.cache_data(args.mort_process, version=version)
    logging.info("Completed.")


if __name__ == '__main__':
    main()
