import argparse
import dataclasses
import logging
from typing import Optional

from hale import launch
from hale.common import logging_utils


@dataclasses.dataclass
class LaunchArgs:
    hale_version: int
    project: str
    error_dir: Optional[str]
    output_dir: Optional[str]


def parse_args() -> LaunchArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--hale_version',
        type=int,
        required=True,
        help='HALE version to run'
    )
    parser.add_argument(
        '--project',
        type=str,
        required=True,
        help='Cluster project to run HALE on'
    )
    parser.add_argument(
        '--error_dir',
        required=False,
        help='Location to write error logs'
    )
    parser.add_argument(
        '--output_dir',
        required=False,
        help='Location to write output logs'
    )
    args = vars(parser.parse_args())
    return LaunchArgs(**args)


def main() -> None:
    logging_utils.configure_logging()
    args = parse_args()
    logging.info('Building HALE workflow')
    workflow = launch.create_hale_workflow(
        args.hale_version,
        args.project,
        args.error_dir,
        args.output_dir
    )
    logging.info('Running HALE worfklow')
    workflow.run()


if __name__ == '__main__':
    main()
