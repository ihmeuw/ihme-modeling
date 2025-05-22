import click

from ihme_cc_sev_calculator.lib import (
    constants,
    io,
    logging_utils,
    parameters,
    risks,
    rr_max,
    rr_max_aggregation,
)

logger = logging_utils.module_logger(__name__)


@click.command
@click.option(
    "aggregate_rei_id",
    "--aggregate_rei_id",
    type=int,
    help="Aggregate REI ID to calculate RR max for.",
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def aggregate_rr_max(aggregate_rei_id: int, version_id: int) -> None:
    """Aggregate RR max for the given aggregate REI ID."""
    params = parameters.Parameters.read_from_cache(version_id)
    rei_metadata = params.read_rei_metadata().query(f"rei_id == {aggregate_rei_id}")
    aggregate_rei_metadata = params.read_rei_metadata(
        rei_set_id=constants.AGGREGATION_REI_SET_ID
    )

    # We only have RRmax for child risks with RR models but we also need full list
    # of child risks in order to find and drop all 100% attributable outcomes
    child_rei_ids_with_rr_max = risks.get_child_rei_ids(
        aggregate_rei_id, aggregate_rei_metadata, exclude_no_rr_rei_ids=True
    )
    all_child_rei_ids = risks.get_child_rei_ids(
        aggregate_rei_id, aggregate_rei_metadata, exclude_no_rr_rei_ids=False
    )
    logger.info(
        f"Starting RR max aggregation for '{rei_metadata['rei_name'].iat[0]}', "
        f"REI ID {aggregate_rei_id}, child REI IDs {all_child_rei_ids}"
    )

    # Aggregate child RRmaxes to create RRmax for the aggregate REI
    rr_max_df = io.read_rr_max_draws(child_rei_ids_with_rr_max, params.output_dir)

    logger.info("Drop 100% attributable outcomes.")
    rr_max_df = rr_max_aggregation.drop_one_hundred_percent_attributable(
        rr_max_df=rr_max_df,
        child_rei_ids=all_child_rei_ids,
        pafs_of_one=params.read_file("pafs_of_one"),
        cause_metadata=params.read_cause_metadata(),
        release_id=params.release_id,
    )

    logger.info("Aggregate RRmaxes.")
    rr_max_df = rr_max_aggregation.aggregate_rr_max(rr_max_df, params.draw_cols)

    # Validate RR max
    rr_max.validate_rr_max(aggregate_rei_id, rr_max_df, params)

    logger.info("RRmax aggregation complete. Saving draws.")
    io.save_rr_max_draws(rr_max_df, aggregate_rei_id, params.draw_cols, params.output_dir)


if __name__ == "__main__":
    aggregate_rr_max()
