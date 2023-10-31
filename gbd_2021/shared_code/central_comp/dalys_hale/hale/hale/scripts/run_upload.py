import argparse
import dataclasses

from hale import upload
from hale.common import logging_utils


@dataclasses.dataclass
class UploadArgs:
    hale_version: int
    conn_def: str


def parse_args() -> UploadArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--hale_version',
        type=int,
        required=True,
        help='HALE version to upload'
    )
    parser.add_argument(
        '--conn_def',
        type=str,
        required=True,
        help='Connection definition for uploading to the GBD database'
    )
    args = vars(parser.parse_args())
    return UploadArgs(**args)


def main() -> None:
    logging_utils.configure_logging()
    args = parse_args()
    upload.upload_hale(args.hale_version, args.conn_def)


if __name__ == '__main__':
    main()
