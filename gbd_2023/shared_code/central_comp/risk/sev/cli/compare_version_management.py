import click

from gbd.constants import gbd_process_version_status
from gbd_outputs_versions import CompareVersion

from ihme_cc_sev_calculator.lib import (
    compare_version_management,
    io,
    logging_utils,
    parameters,
)

logger = logging_utils.module_logger(__name__)


@click.command
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def manage_compare_version(version_id: int) -> None:
    """Create/update the compare version with inputs to and outputs from this SEV calculator
    run.
    """
    # Read parameters from cache.
    params = parameters.Parameters.read_from_cache(version_id)

    # Determine if we should update the passed compare version, or create a new one.
    updating_cv = compare_version_management.should_update_compare_version(
        compare_version_id=params.compare_version_id,
        paf_version_id=params.paf_version_id,
        como_version_id=params.como_version_id,
        codcorrect_version_id=params.codcorrect_version_id,
        measures=params.measures,
        overwrite_compare_version=params.overwrite_compare_version,
    )

    # Obtain process versions to add to compare version, validating active or best status.
    process_version_ids = compare_version_management.get_validated_process_versions_to_add(
        version_id=params.version_id,
        paf_version_id=params.paf_version_id,
        como_version_id=params.como_version_id,
        codcorrect_version_id=params.codcorrect_version_id,
        measures=params.measures,
        updating_cv=updating_cv,
    )

    # Construct CompareVersion to which we will be adding process versions.
    if updating_cv:
        cv = CompareVersion(params.compare_version_id)
    else:
        cv = CompareVersion.add_new_version(release_id=params.release_id)

    # Add process versions.
    cv.add_process_version(process_version_ids, overwrite=params.overwrite_compare_version)

    # Activate compare version, if new.
    if not updating_cv:
        cv.update_status(gbd_process_version_status.ACTIVE)

    # Write out final compare version id to a file.
    io.write_final_compare_version_id(version_id, cv.compare_version_id)

    # Warn if compare version passed but not updated.
    if params.compare_version_id is not None and not updating_cv:
        logger.warning(
            f"Compare version {params.compare_version_id} was passed, but was not updated."
        )

    logger.info(
        f"Compare version {cv.compare_version_id} {'updated' if updating_cv else 'created'}: "
        f"'{cv.compare_version_description}'"
    )


if __name__ == "__main__":
    manage_compare_version()
