import click

from gbd.constants import cause

from ihme_cc_sev_calculator.lib import io, parameters, temperature_etl


@click.command
@click.option(
    "location_id", "--location_id", type=int, help="Location ID to calculate SEVs for."
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def etl_temperature_sevs(location_id: int, version_id: int) -> None:
    """ETL SEVs for all temperature risks for the given location ID."""
    params = parameters.Parameters.read_from_cache(version_id)
    cause_metadata = params.read_cause_metadata()

    sev_df = io.read_temperature_sev_draws(
        location_id, params.year_ids, params.n_draws, params.release_id
    )
    temperature_etl.validate_temperature_sevs(sev_df, cause_metadata)

    # Save draws by cause if requested
    if params.by_cause:
        for rei_id in params.temperature_rei_ids:
            subset = sev_df.query(f"rei_id == {rei_id} & cause_id != {cause.ALL_CAUSE}")
            io.save_sev_draws(
                sev_df=subset,
                rei_id=rei_id,
                location_id=location_id,
                draw_cols=params.draw_cols,
                root_dir=params.output_dir,
                by_cause=True,
            )

    for rei_id in params.temperature_rei_ids:
        subset = sev_df.query(f"rei_id == {rei_id} & cause_id == {cause.ALL_CAUSE}")
        io.save_sev_draws(
            sev_df=subset,
            rei_id=rei_id,
            location_id=location_id,
            draw_cols=params.draw_cols,
            root_dir=params.output_dir,
            by_cause=False,
        )


if __name__ == "__main__":
    etl_temperature_sevs()
