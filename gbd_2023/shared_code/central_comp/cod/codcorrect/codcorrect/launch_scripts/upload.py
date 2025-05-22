import argparse
import logging

from codcorrect.legacy import uploaders
from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils import constants
from codcorrect.lib.utils import logs

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        help="The CoDCorrect version we are uploading.",
        required=True
    )
    parser.add_argument(
        '--database',
        type=str,
        help="The database to upload to. One of 'gbd', 'cod', or 'codcorrect'",
        choices=constants.DataBases.DATABASES,
        required=True
    )
    parser.add_argument(
        '--measure_id',
        type=int,
        help="Which measure are we uploading?",
        required=True
    )
    parser.add_argument(
        '--upload_type',
        type=str,
        default=constants.FilePaths.SINGLE_DIR,
        choices=[constants.FilePaths.SINGLE_DIR,
                 constants.FilePaths.MULTI_DIR],
        help="'single' or 'multi' for parallelizing upload types")
    return parser.parse_args()


def upload_summaries(
        database: str,
        version: MachineParameters,
        measure_id: int,
        upload_type: str
) -> None:
    logger.info(f"Creating uploader class for {database}.")
    upload_factory = uploaders.Uploader.create_constructor(database=database)
    uploader = upload_factory(version=version, measure_id=measure_id,
                              upload_type=upload_type)
    logger.info(f"Uploading summaries to {database}.")
    uploader.upload()


def main() -> None:
    logs.configure_logging()
    args: argparse.Namespace = parse_arguments()
    parameters = MachineParameters.load(args.version_id)

    logger.info(
        f"Beginning upload to {args.database}"
        f"database for measure {args.measure_id}, upload_type "
        f"{args.upload_type}."
    )
    upload_summaries(
        database=args.database,
        version=parameters,
        measure_id=args.measure_id,
        upload_type=args.upload_type
    )
    logger.info("Completed.")


if __name__ == '__main__':
    main()
