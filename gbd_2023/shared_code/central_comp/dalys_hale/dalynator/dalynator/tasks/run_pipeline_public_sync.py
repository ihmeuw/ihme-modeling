import logging

from dalynator import get_input_args
from gbd import enums

from dual_upload.lib import prep


logger = logging.getLogger(__name__)


def main(cli_args=None):
    """ Sync db metadata to the public db. This is the process
    that creates the gbd output tables on the public host in preparation
    for upload.

    Args:
        gbd_process_version_ids (List[int]): the process version(s) that we
            are uploading summaries for
        upload_to_test (optional flag): If included, preps the test database
            instead of production
    """
    parser = get_input_args.construct_parser_public_sync()
    args = get_input_args.construct_args_public_sync(parser, cli_args=cli_args)
    upload_to_test = args.upload_to_test
    process_version_ids = args.gbd_process_version_ids

    if upload_to_test:
        upload_env = enums.Environment.TEST.value
    else:
        upload_env = enums.Environment.PRODUCTION.value

    for index, process_version_id in enumerate(process_version_ids):
        refresh_metadata = True if index == 0 else False
        prep.create_columnstore_output_tables(
            process_version_id=process_version_id,
            environment=upload_env,
            refresh_metadata=refresh_metadata,
        )
