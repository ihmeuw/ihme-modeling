import click

from ihme_cc_sev_calculator.lib import parameters, sev_aggregation


@click.command
@click.option("rei_id", "--rei_id", type=int, help="REI ID to aggregate locations for.")
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def aggregate_locations_for_sevs(rei_id: int, version_id: int) -> None:
    """Aggregate locations for SEVs for the given REI ID."""
    params = parameters.Parameters.read_from_cache(version_id)

    sev_aggregation.run_location_aggregation(rei_id=rei_id, by_cause=False, params=params)

    if params.by_cause:
        sev_aggregation.run_location_aggregation(rei_id=rei_id, by_cause=True, params=params)


if __name__ == "__main__":
    aggregate_locations_for_sevs()
