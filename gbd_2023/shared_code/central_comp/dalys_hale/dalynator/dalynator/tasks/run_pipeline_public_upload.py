import logging
import os

from dual_upload.lib import upload
from dual_upload.lib.utils import constants as upload_constants
from gbd import enums as gbd_enums
from dual_upload.lib.utils.constants import PROCESS_NAMES

from dalynator import constants
from dalynator import get_input_args


logger = logging.getLogger('dalynator.tasks.run_pipeline_public_upload')


def main():
    """ Public columnstore upload

    Args:
        gbd_process_id (int): ID of the process, e.g. 4 for risk
        table_type (str): single_year or multi_year (for % change estimates)
        year_id (int): The year to upload summaries for. Required for single-year
            summaries and unused for multi-year
        start_year (int): The start year for % change summaries. Required for
            multi-year summaries and unused for single-year
        end_year (int): The end year for % change summaries. Required for
            multi-year summaries and unused for single-year
        measure_id (int): The measure to upload
        gbd_process_version_id (int): The process version ID, used to determine
            the table name to upload to
        out_dir (str): The output directory of this 'nator run
        upload_to_test (optional flag): If included, sorts the summaries in
            preparation to upload to the test database. Production otherwise
    """

    get_input_args.create_logging_directories()
    parser = get_input_args.construct_parser_public_upload()
    args = get_input_args.construct_args_public_upload(parser)

    if args.upload_to_test:
        public_upload_env = gbd_enums.Environment.TEST.value
    else:
        public_upload_env = gbd_enums.Environment.PRODUCTION.value

    if args.table_type == constants.SINGLE_YEAR_TABLE_TYPE:
        base_shape = upload_constants.BaseShape.SINGLE_YEAR
    else:
        base_shape = upload_constants.BaseShape.MULTI_YEAR

    # Check that the summary file exists
    upload_root = os.path.join(args.out_dir, constants.PUBLIC_UPLOAD_SUBDIR)
    summary_path = (
        f"{upload_root}/{args.measure_id}_{args.location_id}_output_"
        f"{PROCESS_NAMES[args.gbd_process_id]}_{args.table_type}.csv"
    )
    if not os.path.exists(summary_path):
        raise RuntimeError(f"Sorted summary file {summary_path} is missing")

    upload.upload(
        process_id=args.gbd_process_id,
        compare_context_id=constants.COMPARE_CONTEXTS[args.gbd_process_id],
        process_version_id=args.gbd_process_version_id,
        base_shape=base_shape,
        measure_id=args.measure_id,
        location_id=args.location_id,
        upload_root=upload_root,
        environment=public_upload_env,
        load_from_remote=False,
        raise_on_failure=True,
        delete_after_upload=True,
    )


if __name__ == "__main__":
    main()
