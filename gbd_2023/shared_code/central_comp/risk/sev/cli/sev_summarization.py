import click

from ihme_cc_sev_calculator.lib import parameters, sev_summarization


@click.command
@click.option(
    "location_id", "--location_id", type=int, help="Location ID to summarize SEVs for."
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def summarize_sevs_for_location(location_id: int, version_id: int) -> None:
    """Summarize SEVs for the given location ID."""
    params = parameters.Parameters.read_from_cache(version_id)

    sev_summarization.summarize_sevs_for_location(
        location_id=location_id, by_cause=False, params=params
    )

    if params.by_cause:
        sev_summarization.summarize_sevs_for_location(
            location_id=location_id, by_cause=True, params=params
        )


if __name__ == "__main__":
    summarize_sevs_for_location()
