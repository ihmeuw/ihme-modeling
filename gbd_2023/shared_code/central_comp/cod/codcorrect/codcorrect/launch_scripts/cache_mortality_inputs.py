import argparse
import logging

from codcorrect.legacy import mortality_inputs
from codcorrect.legacy.utils import constants
from codcorrect.legacy.parameters import machinery
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


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
        '--version_id',
        type=int,
        required=True,
        help='death machine version id the pop/env inputs are for'
    )
    return parser.parse_args()


def main() -> None:
    logs.configure_logging()
    args = parse_arguments()
    parameters = machinery.MachineParameters.load(args.version_id)

    logger.info("Validating input arguments")
    mortality_inputs.validate_mortality_process_argument(args.mort_process)

    logger.info(f"Caching {args.mort_process}.")
    mortality_inputs.cache_data(args.mort_process, version=parameters)
    logger.info("Completed.")


if __name__ == '__main__':
    main()
