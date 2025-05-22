import click

from core_maths import summarize

from ihme_cc_sev_calculator.lib import io, parameters, rr_max_summarization


@click.command()
@click.option(
    "--rei_id", required=True, type=int, help="The risk for which to summarize RRmax"
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def summarize_rrmax(rei_id: int, version_id: int) -> None:
    """Summarize RRmax draws for an REI, summarize, and save."""
    params = parameters.Parameters.read_from_cache(version_id)
    rr_max_df = io.read_rr_max_draws(rei_id, params.output_dir)

    # Summarize and clean up columns
    summaries = summarize.get_summary(rr_max_df, params.draw_cols).rename(
        columns={"mean": "val"}
    )
    summaries = rr_max_summarization.format_summaries(summaries)

    # Save summaries as they are (without NAs for age/sex restrictions) for upload.
    # We don't want to include NAs in the db
    io.save_rr_max_summaries(summaries, rei_id, params.output_dir, purpose="upload")

    # Then make square, filling in missingness with NAs and save separately. Request from FHS
    summaries = rr_max_summarization.make_square(
        summaries, params.age_group_ids, params.sex_ids
    )
    io.save_rr_max_summaries(summaries, rei_id, params.output_dir, purpose="fhs")


if __name__ == "__main__":
    summarize_rrmax()
