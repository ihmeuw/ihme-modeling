import click

from gbd.constants import gbd_process_version_status

from ihme_cc_sev_calculator.lib import constants, logging_utils, parameters, upload

logger = logging_utils.module_logger(__name__)


@click.command()
@click.option(
    "--measure",
    required=True,
    type=str,
    help="The measure to upload for. Either 'sev' or 'rr_max'.",
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def run_upload(measure: str, version_id: int) -> None:
    """Run the upload process."""
    params = parameters.Parameters.read_from_cache(version_id)

    # Set up GBD process version
    pv = upload.get_process_version(measure, params)

    # Run upload depending on measure
    if measure == constants.SEV:
        table_types = (
            ["single_year", "multi_year"] if params.percent_change else ["single_year"]
        )
        for table_type in table_types:
            upload.upload_sevs(
                params.version_id, pv.gbd_process_version_id, table_type, params.output_dir
            )

        logger.info(
            f"SEV upload complete for GBD process version {pv.gbd_process_version_id}"
        )
    else:
        upload.upload_rr_max(params.version_id, pv.gbd_process_version_id, params.output_dir)

        logger.info(
            f"RR max upload complete for GBD process version {pv.gbd_process_version_id}"
        )

    if params.test:
        # Mark the GBD process version as a test.
        pv.update_status(gbd_process_version_status.INTERNAL_TEST)
    else:
        # Activate the GBD process version.
        pv.update_status(gbd_process_version_status.ACTIVE)


if __name__ == "__main__":
    run_upload()
