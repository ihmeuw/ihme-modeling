import logging
import os

from dual_upload.lib import sort
from dual_upload.lib.utils import constants as upload_constants
from gbd import enums

from dalynator import constants, get_input_args
from dalynator.makedirs_safely import makedirs_safely


logger = logging.getLogger(__name__)


def main(cli_args=None):
    """ Sort summaries and save for public upload. The input summaries are by
    location, measure, and year. The sorted outputs are by measure and year. We run
    this sorter separately for each gbd process (e.g. risk, etiology) and for
    single-year vs. multi-year estimates.

    Args:
        gbd_process_id (int): ID of the process, e.g. 4 for risk
        table_type (str): single_year or multi_year (for % change estimates)
        measure_id (int): The measure to sort summaries for
        location_id (int): The location ID to sort summaries for
        out_dir (str): The output directory of this 'nator run
        upload_to_test (optional flag): If included, sorts the summaries in
            preparation to upload to the test database. Production otherwise
    """
    parser = get_input_args.construct_parser_public_sort()
    args = get_input_args.construct_args_public_sort(parser, cli_args=cli_args)

    environment = (
        enums.Environment.TEST
        if args.upload_to_test
        else enums.Environment.PRODUCTION
    )

    # Set up dirs
    input_dir = os.path.join(args.out_dir, "draws")
    output_dir = os.path.join(args.out_dir, constants.PUBLIC_UPLOAD_SUBDIR)
    makedirs_safely(output_dir)

    # We pass different year arguments for single-year vs multi-year sorting
    if args.table_type == constants.SINGLE_YEAR_TABLE_TYPE:
        base_shape = upload_constants.BaseShape.SINGLE_YEAR.value
        file_pattern = (
            f"{args.location_id}/upload/{args.measure_id}/{args.table_type}/"
            f"upload_{constants.PROCESS_NAMES_SHORT[args.gbd_process_id]}_"
            f"{args.location_id}_{{year_id}}.csv"
        )
    else:
        base_shape = upload_constants.BaseShape.MULTI_YEAR.value
        file_pattern = (
            f"{args.location_id}/upload/{args.measure_id}/{args.table_type}/"
            f"upload_{constants.PROCESS_NAMES_SHORT[args.gbd_process_id]}_"
            f"{args.location_id}_{{year_start_id}}_{{year_end_id}}.csv"
        )

    sort.sort(
        process_id=args.gbd_process_id,
        compare_context_id=constants.COMPARE_CONTEXTS[args.gbd_process_id],
        measure_id=[args.measure_id],
        location_id=[args.location_id],
        base_shape=base_shape,
        input_dir=input_dir,
        file_pattern=file_pattern,
        output_dir=output_dir,
        environment=environment,
    )


if __name__ == "__main__":
    main()
