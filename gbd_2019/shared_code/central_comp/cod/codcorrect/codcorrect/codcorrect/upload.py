import argparse
import logging

from fauxcorrect import uploaders
from fauxcorrect.parameters.master import MachineParameters
from fauxcorrect.utils import constants
from fauxcorrect.utils.logging import setup_logging


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
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
        help="The Faux,CodCorrect version we are uploading.",
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
    logging.info(f"Creating uploader class for {database}.")
    upload_factory = uploaders.Uploader.create_constructor(database=database)
    uploader = upload_factory(version=version, measure_id=measure_id,
                              upload_type=upload_type)
    logging.info(f"Uploading summaries to {database}.")
    uploader.upload()


def main() -> None:
    args: argparse.Namespace = parse_arguments()
    version: MachineParameters = MachineParameters.recreate_from_version_id(
        version_id=args.version_id, process_name=args.machine_process
    )
    setup_logging(
        version.parent_dir,
        constants.DAG.Tasks.Type.UPLOAD,
        args
    )

    logging.info(
        f"Beginning upload for {args.machine_process} to {args.database}"
        f"database for measure {args.measure_id}, upload_type "
        f"{args.upload_type}."
    )
    upload_summaries(
        database=args.database,
        version=version,
        measure_id=args.measure_id,
        upload_type=args.upload_type
    )
    logging.info("Completed.")


if __name__ == '__main__':
    main()
