"""Custom PAFs for HIV.

These PAFs are not directly modeled by researchers but calculated on the fly.
"""

import pathlib
from typing import List, Optional

import pandas as pd

import db_queries
from gbd.constants import measures
from get_draws import api

from ihme_cc_paf_calculator.lib import constants, io_utils

HIV_IV_DRUG_ME_ID: int = 20953  # Proportion HIV due to intravenous drug use annual exposure
HIV_SEX_ME_ID: int = 20954  # Proportion HIV due to sex annual exposure

INJECTED_DRUG_USE_REI_ID: int = 138


def get_exposure_draws(
    release_id: int, location_id: int, year_id: List[int], n_draws: int
) -> pd.DataFrame:
    """Get exposure draws for three HIV proportion MEs.

    These are treated as PAFs once normalized. We don't use
    ihme_cc_risk_utils.get_exposure_draws because it does not conserve ME ID.
    """
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    return api.get_draws(
        gbd_id_type="rei_id",
        gbd_id=constants.UNSAFE_SEX_REI_ID,
        source="exposure",
        release_id=release_id,
        location_id=location_id,
        year_id=year_id,
        n_draws=n_draws,
        downsample=True,
    )[constants.DEMOGRAPHIC_COLS + ["modelable_entity_id"] + draw_cols]


def calculate_hiv_due_to_drug_use_pafs(
    location_id: int,
    year_ids: List[int],
    release_id: int,
    n_draws: int,
    cause_metadata: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Calculate HIV due to drug use PAFs for injected drug use.

    Intended for use in the PAF Aggregator.

    Methods:
        * Pull unsafe sex exposure from three MEs: proportion of HIV due to drug use,
            proportion of HIV due to sex, and proportion of HIV due to other
        * Normalize the exposures by dividing by the sum of all three exposures,
            which is not constrained to 1
        * Copy normalized unsafe sex exposure due to IV drug use as PAFs for all
            most detailed HIV causes and for both YLLs and YLDs
        * Set the REI as injected drug use

    Args:
        location_id: ID of the location to calculate PAFs for.
        year_ids: IDs of the years to calculate PAFs for.
        release_id: ID of the release.
        n_draws: number of draws
        cause_metadata: cause metadata. If given, uses cause_metadata to determine most
            detailed HIV causes. If not, uses release_id to pull cause metadata to do so.
    """
    # Pull unsafe sex exposure
    exposure = get_exposure_draws(
        release_id=release_id, location_id=location_id, year_id=year_ids, n_draws=n_draws
    )

    pafs = _calculate_hiv_paf(
        rei_id=constants.INJECTED_DRUG_USE_REI_ID,
        exposure=exposure,
        me_id=HIV_IV_DRUG_ME_ID,
        n_draws=n_draws,
        release_id=release_id,
        cause_metadata=cause_metadata,
    )

    return pafs


def calculate_hiv_due_to_unsafe_sex_paf(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> None:
    """Calculates custom PAFs for HIV due to unsafe sex.

    Unsafe sex has direct PAFs (no relative risk model). The associated exposure draws
    for the three HIV MEs are pulled, scaled to 1, and only
    'Proportion HIV due to sex proportion' is kept.

    PAFs are duplicated for all most detailed HIV causes and for both measures. Age restricted
    to 10+.

    Intended only for use within the PAF Calculator.
    """
    cause_metadata = io_utils.get(root_dir, constants.CacheContents.CAUSE_METADATA)
    age_metadata = io_utils.get(root_dir, constants.CacheContents.AGE_METADATA)

    if settings.rei_id != constants.UNSAFE_SEX_REI_ID:
        raise RuntimeError(
            "Internal error: function can only be run for unsafe sex, REI ID "
            f"{constants.UNSAFE_SEX_REI_ID}. Got rei_id {settings.rei_id}"
        )

    exposure = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
    )

    pafs = _calculate_hiv_paf(
        rei_id=settings.rei_id,
        exposure=exposure,
        me_id=HIV_SEX_ME_ID,
        n_draws=settings.n_draws,
        cause_metadata=cause_metadata,
    )

    # Age-restrict to 10+
    age_group_ids_10_plus = age_metadata.query("age_group_years_start >= 10")[
        "age_group_id"
    ].tolist()
    pafs = pafs.query(f"age_group_id.isin({age_group_ids_10_plus})")

    # Save PAF
    io_utils.write_paf(pafs, root_dir, location_id)


def _calculate_hiv_paf(
    rei_id: int,
    exposure: pd.DataFrame,
    me_id: int,
    n_draws: int,
    release_id: Optional[int] = None,
    cause_metadata: Optional[pd.DataFrame] = None,
) -> None:
    """Calculates HIV PAFs for the given ME ID using the given exposure.

    Methods:
        * Uses given unsafe sex exposure from three MEs: proportion of HIV due to drug use,
            proportion of HIV due to sex, and proportion of HIV due to other
        * Normalize the exposures by dividing by the sum of all three exposures,
            which is not constrained to 1
        * Copy normalized unsafe sex exposure due X (where X is determined by me_id) for all
            most detailed HIV causes and for both YLLs and YLDs
        * Set the REI as given rei_id
    """
    # Confirm given exposure has results for three MEs and for the requested ME
    exposure_me_ids = exposure["modelable_entity_id"].unique().tolist()
    if len(exposure_me_ids) != 3:
        raise ValueError(
            f"Expected given HIV exposure to have three ME IDs, got {len(exposure_me_ids)}: "
            f"{exposure_me_ids}"
        )

    if me_id not in exposure_me_ids:
        raise ValueError(
            f"No exposure draws found for given me_id {me_id}. Exposure contains the "
            f"following ME IDs: {exposure_me_ids}."
        )

    draw_cols = [f"draw_{i}" for i in range(n_draws)]

    # Calculate sum of three unsafe sex exposure MEs, may not be 1
    total_unsafe_sex_exposure = exposure.groupby(constants.DEMOGRAPHIC_COLS)[draw_cols].sum()

    # Divide unsafe sex exposure for given me_id (proportion of HIV due to drug use,
    # proportion of HIV due to sex) by the sum of all exposures
    results = (
        exposure.query("modelable_entity_id == @me_id")
        .set_index(constants.DEMOGRAPHIC_COLS + ["modelable_entity_id"])
        .divide(total_unsafe_sex_exposure)
        .reset_index()
    )

    # Copy drug use PAFs for all HIV most-detailed causes and for YLLs and YLDs
    hiv_cause_ids = _get_most_detailed_hiv_causes(release_id, cause_metadata)
    results = (
        results.merge(pd.DataFrame({"cause_id": hiv_cause_ids}), how="cross")
        .merge(pd.DataFrame({"measure_id": [measures.YLD, measures.YLL]}), how="cross")
        .assign(rei_id=rei_id)
    )[["rei_id", "cause_id"] + constants.DEMOGRAPHIC_COLS + ["measure_id"] + draw_cols]

    # Confirm number of rows is the number of unsafe sex exposure draw rows divided by 3
    # (for each ME) and multiplied by the number of HIV causes and measures (2)
    expected_num_rows = (len(exposure) / 3) * len(hiv_cause_ids) * 2
    if len(results) != expected_num_rows:
        raise RuntimeError(
            f"After calculating HIV PAFs, expected {expected_num_rows} rows."
            f"Only {len(results)} rows exist."
        )

    return results


def _get_most_detailed_hiv_causes(
    release_id: Optional[int], cause_metadata: Optional[pd.DataFrame]
) -> List[int]:
    """Pulls the most detailed HIV causes.

    Uses cause_metadata if not None. Otherwise, pulls cause metadata using release_id.
    """
    if cause_metadata is None:
        cause_metadata = db_queries.get_cause_metadata(
            cause_set_id=constants.COMPUTATION_CAUSE_SET_ID, release_id=release_id
        )

    return (
        cause_metadata.query(
            "acause.str.contains('hiv_') & most_detailed == 1", engine="python"
        )
    )["cause_id"].tolist()
