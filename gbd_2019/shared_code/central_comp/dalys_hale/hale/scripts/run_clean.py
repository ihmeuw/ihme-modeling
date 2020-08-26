import argparse
import dataclasses

from hale import clean
from hale.common import logging_utils


@dataclasses.dataclass
class CleanArgs:
    hale_version: int


def parse_args() -> CleanArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--hale_version',
        type=int,
        required=True,
        help='HALE version of this run'
    )
    args = vars(parser.parse_args())
    return CleanArgs(**args)


def main() -> None:
    logging_utils.configure_logging()
    args = parse_args()
    clean.delete_temp_files(args.hale_version)


if __name__ == '__main__':
    main()
