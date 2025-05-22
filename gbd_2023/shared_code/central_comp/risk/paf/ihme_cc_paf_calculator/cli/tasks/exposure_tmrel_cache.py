import pathlib

import click
import pandas as pd

import ihme_cc_risk_utils

from ihme_cc_paf_calculator.lib import constants, intervention_utils, io_utils
from ihme_cc_paf_calculator.lib.custom_pafs import (
    air_pmhap,
    averted_burden,
    drug_dependence,
    hiv,
    lbwsg,
)


@click.command
@click.option(
    "output_dir",
    "--output_dir",
    type=str,
    required=True,
    help="root directory of a specific paf calculator run",
)
@click.option(
    "location_id",
    "--location_id",
    type=int,
    required=True,
    help="location to cache exposure and tmrel for",
)
def main(output_dir: str, location_id: int) -> None:
    """Saves exposure, exposure_sd, and tmrel draws for a location."""
    _run_task(output_dir, location_id)


def _run_task(root_dir: str, location_id: int) -> None:
    """Saves exposure, exposure_sd, and tmrel draws for a location."""
    root_dir = pathlib.Path(root_dir)

    settings = io_utils.read_settings(root_dir)
    rei_metadata = io_utils.get(root_dir, constants.CacheContents.REI_METADATA)
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA)
    models = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)
    paf_calc_type = rei_metadata["rei_calculation_type"].astype(int).iat[0]

    exposure = get_exposure_draws_for_location(
        settings=settings, location_id=location_id, models=models
    )
    if paf_calc_type == constants.CalculationType.CATEGORICAL:
        if settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
            exposure = averted_burden.adjust_categorical_exposure_for_max(exposure)
        exposure = normalize_exposure(exposure, settings.n_draws)

    if paf_calc_type in [
        constants.CalculationType.CATEGORICAL,
        constants.CalculationType.CUSTOM,
        constants.CalculationType.DIRECT,
    ]:
        # Categorical, custom, and direct PAFs generally only need exposure
        exposure_sd = None
        tmrel = None
    else:
        tmrel = ihme_cc_risk_utils.get_tmrel_draws(
            rei_id=settings.rei_id,
            release_id=settings.release_id,
            n_draws=settings.n_draws,
            me_ids=models,
            rei_metadata=rei_metadata,
            index_cols=["location_id", "year_id", "age_group_id", "sex_id"],
            year_id=settings.year_id,
            location_id=location_id,
            sex_id=exposure["sex_id"].unique().tolist(),
            age_group_id=exposure["age_group_id"].unique().tolist(),
        )
        exposure_sd = ihme_cc_risk_utils.get_exposure_sd_draws(
            rei_id=settings.rei_id,
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            sex_id=exposure["sex_id"].unique().tolist(),
            n_draws=settings.n_draws,
        )

    mediators = rr_metadata[rr_metadata["source"] == "delta"]["med_id"].unique().tolist()
    all_mediator_exposure = None
    all_mediator_exposure_sd = None
    all_mediator_tmrel = None
    if mediators:
        mediator_rei_metadata = io_utils.get(
            root_dir, constants.CacheContents.MEDIATOR_REI_METADATA
        )
        all_mediator_exposure = pd.DataFrame()
        all_mediator_exposure_sd = pd.DataFrame()
        all_mediator_tmrel = pd.DataFrame()
        for med_id in mediators:
            mediator_exposure = ihme_cc_risk_utils.get_exposure_draws(
                rei_id=med_id,
                release_id=settings.release_id,
                location_id=location_id,
                year_id=settings.year_id,
                n_draws=settings.n_draws,
            ).assign(rei_id=med_id)
            mediator_tmrel = ihme_cc_risk_utils.get_tmrel_draws(
                rei_id=med_id,
                release_id=settings.release_id,
                n_draws=settings.n_draws,
                me_ids=models.query("rei_id == @med_id"),
                rei_metadata=mediator_rei_metadata.query("rei_id == @med_id"),
                index_cols=["location_id", "year_id", "age_group_id", "sex_id"],
                year_id=settings.year_id,
                location_id=location_id,
                sex_id=mediator_exposure["sex_id"].unique().tolist(),
                age_group_id=mediator_exposure["age_group_id"].unique().tolist(),
            ).assign(rei_id=med_id)
            mediator_exposure_sd = ihme_cc_risk_utils.get_exposure_sd_draws(
                rei_id=med_id,
                release_id=settings.release_id,
                location_id=location_id,
                year_id=settings.year_id,
                sex_id=mediator_exposure["sex_id"].unique().tolist(),
                n_draws=settings.n_draws,
            ).assign(rei_id=med_id)
            all_mediator_exposure = pd.concat(
                [all_mediator_exposure, mediator_exposure], ignore_index=True
            )
            all_mediator_exposure_sd = pd.concat(
                [all_mediator_exposure_sd, mediator_exposure_sd], ignore_index=True
            )
            all_mediator_tmrel = pd.concat(
                [all_mediator_tmrel, mediator_tmrel], ignore_index=True
            )

    intervention_coverage = None
    if settings.intervention_rei_id:
        intervention_coverage = intervention_utils.get_intervention_coverage_draws(
            intervention_rei_id=settings.intervention_rei_id,
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
        )

    io_utils.write_exposure_tmrel_cache(
        root_dir=root_dir,
        location_id=location_id,
        exposure=exposure,
        exposure_sd=exposure_sd,
        tmrel=tmrel,
        mediator_exposure=all_mediator_exposure,
        mediator_exposure_sd=all_mediator_exposure_sd,
        mediator_tmrel=all_mediator_tmrel,
        intervention_coverage=intervention_coverage,
    )
    return


def get_exposure_draws_for_location(
    settings: constants.PafCalculatorSettings, location_id: int, models: pd.DataFrame
) -> pd.DataFrame:
    """Get exposure draws for a given location and REI."""
    if settings.rei_id == constants.DRUG_DEPENDENCE_REI_ID:
        # Drug dependence is a custom PAF and has special handling for pulling exposure
        exposure = drug_dependence.get_exposure_draws_drug_dependence(
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
            me_ids=models,
        )
    elif settings.rei_id == constants.AIR_PMHAP_REI_ID:
        exposure = air_pmhap.get_exposure_draws(
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
        )
    elif settings.rei_id == constants.UNSAFE_SEX_REI_ID:
        exposure = hiv.get_exposure_draws(
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
        )
    elif settings.rei_id == constants.LBWSGA_REI_ID:
        exposure = lbwsg.get_exposure_draws(
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
        )
    else:
        exposure = ihme_cc_risk_utils.get_exposure_draws(
            rei_id=settings.rei_id,
            release_id=settings.release_id,
            location_id=location_id,
            year_id=settings.year_id,
            n_draws=settings.n_draws,
        )

    return exposure


def normalize_exposure(exposure: pd.DataFrame, n_draws: int) -> pd.DataFrame:
    """Rescale categorical exposure sums to 1. Raise an exception if any sums are zero.

    While get_draws calculates a residual for categorical exposures, the residual is not
    allowed to go negative, so if the sum of exposures across modeled categories exceeds 1
    (a real possibility when the categories are modeled independently of each other), the
    residual will not correct for that.

    Downsampling occurs after residual calculation in get_draws, which can also lead to
    exposure sums other than 1.
    """
    draw_cols = [f"draw_{i}" for i in range(n_draws)]

    # We rely on the index to align division below, so ensure it's unique.
    exposure = exposure.reset_index(drop=True)

    # Compute sum of exposure categories for each demographic-draw.
    sums = exposure.groupby(constants.DEMOGRAPHIC_COLS)[draw_cols].transform("sum")

    # Check for any sums of zero; raise if found.
    bad = sums[(sums == 0).any(axis=1)]
    if not bad.empty:
        raise RuntimeError(
            "Found categorical exposure sums of zero. Example:\n"
            f"{exposure.loc[bad.index[0], constants.DEMOGRAPHIC_COLS]}"
        )

    # Renormalize exposure so each demographic-draw sums to 1.
    exposure[draw_cols] = exposure[draw_cols] / sums
    return exposure


if __name__ == "__main__":
    main()
