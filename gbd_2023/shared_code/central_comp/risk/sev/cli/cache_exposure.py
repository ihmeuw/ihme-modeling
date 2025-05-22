import click

import ihme_cc_risk_utils

from ihme_cc_sev_calculator.lib import constants, io, parameters, risk_prevalence


@click.command
@click.option("rei_id", "--rei_id", type=int, help="REI ID to cache exposure inputs for.")
@click.option(
    "location_id", "--location_id", type=int, help="Location ID to cache exposure inputs for."
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def cache_exposure_draws(rei_id: int, location_id: int, version_id: int) -> None:
    """Cache exposure and exposure SD draws for FPG/SBP for use in R edensity code.

    As of writing this, we have not rewritten the legacy ensemble density code in R.
    For the two risks in the SEV Calculator that use it (high FPG, high SBP), we pull
    exposure, exposure SD and set exposure thresholds first using code that has been
    rewritten into Python and standardized. The second step, uses the
    legacy ensemble density code to calculate risk prevalence above the outcome threshold.
    """
    params = parameters.Parameters.read_from_cache(version_id)

    if rei_id not in constants.EDENSITY_REI_IDS:
        raise RuntimeError(
            "Cache exposure task should only be run for PAFs of 1 REIs "
            f"{constants.EDENSITY_REI_IDS}, not rei_id={rei_id}"
        )

    exposure = ihme_cc_risk_utils.get_exposure_draws(
        rei_id=rei_id,
        location_id=location_id,
        release_id=params.release_id,
        year_id=params.year_ids,
        n_draws=params.n_draws,
    )
    exposure_sd = ihme_cc_risk_utils.get_exposure_sd_draws(
        rei_id=rei_id,
        location_id=location_id,
        release_id=params.release_id,
        year_id=params.year_ids,
        n_draws=params.n_draws,
    )

    exposure_df = risk_prevalence.format_exposure_for_edensity(
        rei_id=rei_id, exposure=exposure, exposure_sd=exposure_sd, draw_cols=params.draw_cols
    )
    io.cache_exposure_draws(
        exposure_df=exposure_df,
        rei_id=rei_id,
        location_id=location_id,
        root_dir=params.output_dir,
    )


if __name__ == "__main__":
    cache_exposure_draws()
