import argparse

from fauxcorrect import mortality_inputs
from fauxcorrect.parameters import machinery
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
    params = machinery.recreate_from_process_and_version_id(
        version_id=args.version_id, machine_process=args.machine_process
    )
    logger = setup_logging(
        params.parent_dir,
        "cache",
        args
    )

    logger.info("Validating input arguments")
    mortality_inputs.validate_mortality_process_argument(args.mort_process)

    logger.info(f"Caching {args.mort_process}.")
    mortality_inputs.cache_data(args.mort_process, version=params)
    logger.info("Completed.")


if __name__ == '__main__':
    main()
