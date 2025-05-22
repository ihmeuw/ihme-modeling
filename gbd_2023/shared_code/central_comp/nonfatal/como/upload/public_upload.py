"""Uploads to public gbd host."""

import logging

from loguru import logger

from dual_upload.lib import prep, sort, upload
from gbd import enums

from como.lib import constants as como_constants
from como.lib.upload import upload_utils
from como.lib.version import ComoVersion

logging.basicConfig(level=logging.INFO)


def configure_upload(como_version: ComoVersion) -> None:
    """Configure public gbd host upload.

    Public upload step 1:
        CSPrepper syncs metadata across the internal and public hosts, as well as create the
        relevant upload tables on the public host. Also handles rollback of sync and table
        creation.
    """
    logger.info("configuring public host gbd tables")

    if como_version.public_upload_test:
        upload_env = enums.Environment.TEST.value
    else:
        upload_env = enums.Environment.PRODUCTION.value

    prep.create_columnstore_output_tables(
        process_version_id=como_version.gbd_process_version_id,
        environment=upload_env,
        refresh_metadata=True,
    )


def sort_upload(upload_task: upload_utils.UploadTask) -> None:
    """Sort public gbd upload.

    Public upload step 2:
        Sorts and resaves written summary files for upload to the public host. Requires
        summaries to be saved by measure, component, location, and year type (single/multi).
    """
    logger.info("sorting summaries for public host gbd tables")

    input_dir = ("FILEPATH")

    if upload_task.year_type == como_constants.YearType.MULTI.value:
        file_pattern = f"{upload_task.location_id}.csv"
    else:
        file_pattern = f"{upload_task.location_id}" + "/{year_id}.csv"

    if upload_task.public_upload_test:
        upload_env = enums.Environment.TEST.value
    else:
        upload_env = enums.Environment.PRODUCTION.value

    sort.sort(
        process_id=upload_task.process_id,
        compare_context_id=como_constants.COMPONENT_TO_COMPARE_CONTEXT[upload_task.component],
        measure_id=[upload_task.measure_id],
        location_id=[upload_task.location_id],
        base_shape=upload_task.year_type.value,
        input_dir=input_dir,
        file_pattern=file_pattern,
        output_dir=f"{upload_task.como_dir}/public_upload/summaries",
        environment=upload_env,
    )


def run_upload(upload_task: upload_utils.UploadTask) -> None:
    """Run public gbd host upload.

    Public upload step 3:
        Uploads the sorted summary files to the upload tables on the public host.
    """
    logger.info("uploading to public host gbd tables")

    if upload_task.public_upload_test:
        upload_env = enums.Environment.TEST.value
    else:
        upload_env = enums.Environment.PRODUCTION.value

    upload.upload(
        process_id=upload_task.process_id,
        compare_context_id=como_constants.COMPONENT_TO_COMPARE_CONTEXT[upload_task.component],
        process_version_id=upload_task.process_version_id,
        base_shape=upload_task.year_type.value,
        measure_id=upload_task.measure_id,
        location_id=upload_task.location_id,
        upload_root="FILEPATH",
        environment=upload_env,
        load_from_remote=False,
        raise_on_failure=True,
        delete_after_upload=True,
    )
