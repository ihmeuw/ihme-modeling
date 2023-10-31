import argparse
import dataclasses

from hale import calculate_hale
from hale.common import logging_utils


@dataclasses.dataclass
class HaleCalculationArgs:
    hale_version: int
    location_id: int


def parse_args() -> HaleCalculationArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--hale_version',
        type=int,
        required=True,
        help='HALE version of this run'
    )
    parser.add_argument(
        '--location_id',
        type=int,
        required=True,
        help='Location ID to calculate HALE for'
    )
    args = vars(parser.parse_args())
    return HaleCalculationArgs(**args)


def main() -> None:
    logging_utils.configure_logging()
    args = parse_args()
    calculate_hale.calculate(args.hale_version, args.location_id)


if __name__ == '__main__':
    main()
