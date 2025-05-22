from typing import Dict, List

import pandas as pd

import chronos
from gbd.constants import measures, metrics
from get_draws import api as get_draws_api

from ihme_cc_sev_calculator.lib import constants

# Map of rei_id -> definitional threshold
THRESHOLD_MAP: Dict[int, int] = {
    constants.HIGH_FPG_REI_ID: 7,  # FPG: diabetic > 7 mmol/L FPG
    constants.HIGH_SBP_REI_ID: 140,  # SBP: hypertensive heart disease > 140 mmHg
}


def get_iron_deficiency_prevalance(
    location_id: int,
    year_ids: List[int],
    estimation_year_ids: List[int],
    release_id: int,
    me_ids: pd.DataFrame,
    n_draws: int,
    draw_cols: List[str],
) -> pd.DataFrame:
    """Get iron deficiency prevalence.

    Defined as the prevalence of moderate or severe anemia.

    If requesting years outside of estimation years, the function interpolates
    between the closest estimation years to min(year_ids) - 5 and max(year_ids) + 5.
    """
    query_str = (
        "modelable_entity_id.isin("
        f"{[constants.MODERATE_ANEMIA_ME_ID, constants.SEVERE_ANEMIA_ME_ID]})"
    )
    prevalence_me_ids = me_ids.query(query_str)
    if len(prevalence_me_ids) != 2:
        raise RuntimeError(f"Expected exactly two anemia MEs. Received:\n{prevalence_me_ids}")

    if set(year_ids).issubset(estimation_year_ids):
        # If all the requested years are estimation years, we can pull existing results
        prevalence = get_draws_api.get_draws(
            "modelable_entity_id",
            gbd_id=prevalence_me_ids["modelable_entity_id"].tolist(),
            version_id=prevalence_me_ids["model_version_id"].tolist(),
            source="epi",
            location_id=location_id,
            year_id=year_ids,
            measure_id=measures.PREVALENCE,
            metric_id=metrics.RATE,
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
        )
    else:
        # If any of the requested years fall outside of estimation years, we interpolate
        # between the closest estimation years to min(year_ids) - 5 and max(year_ids) + 5
        start_year_id = _get_closest_value(estimation_year_ids, target=min(year_ids) - 5)
        end_year_id = _get_closest_value(estimation_year_ids, target=max(year_ids) + 5)

        moderate_prevalence = chronos.interpolate(
            gbd_id_type="modelable_entity_id",
            gbd_id=constants.MODERATE_ANEMIA_ME_ID,
            version_id=prevalence_me_ids.query(
                f"modelable_entity_id == {constants.MODERATE_ANEMIA_ME_ID}"
            )["model_version_id"].iat[0],
            source="epi",
            location_id=location_id,
            measure_id=measures.PREVALENCE,
            release_id=release_id,
            reporting_year_start=start_year_id,
            reporting_year_end=end_year_id,
            n_draws=n_draws,
            downsample=True,
        )
        severe_prevalence = chronos.interpolate(
            gbd_id_type="modelable_entity_id",
            gbd_id=constants.SEVERE_ANEMIA_ME_ID,
            version_id=prevalence_me_ids.query(
                f"modelable_entity_id == {constants.SEVERE_ANEMIA_ME_ID}"
            )["model_version_id"].iat[0],
            source="epi",
            location_id=location_id,
            measure_id=measures.PREVALENCE,
            release_id=release_id,
            reporting_year_start=start_year_id,
            reporting_year_end=end_year_id,
            n_draws=n_draws,
            downsample=True,
        )

        # Subset to only requested years and downsample
        prevalence = (
            pd.concat([moderate_prevalence, severe_prevalence])
            .query(f"year_id.isin({year_ids})")
            .reset_index(drop=True)
        )

    # Verify all requested years are present
    different_year_ids = set(year_ids).symmetric_difference(prevalence["year_id"])
    if different_year_ids:
        raise RuntimeError(
            "Unexpected symmetric difference in requested years and years found in draws: "
            f"{different_year_ids}."
        )

    # Sum moderate and severe anemia prevalences together (assuming mutual exclusivity)
    return prevalence.groupby(constants.DEMOGRAPHIC_COLS)[draw_cols].sum().reset_index()


def _get_closest_value(values: List[int], target: int) -> int:
    """Get closest value in values to target.

    Ex:
        If values=[0, 5, 10], target=4 => returns 5.
    """
    return min(values, key=lambda value: abs(value - target))


def format_exposure_for_edensity(
    rei_id: int, exposure: pd.DataFrame, exposure_sd: pd.DataFrame, draw_cols: List[str]
) -> pd.DataFrame:
    """Format exposure and exposure SD draws for use within edensity R code.

    Melts the two dataframes long, joins, and adds an exposure threshold column based on
    the REI:
        * FPG: > 7 mmol/L FPG
        * SBP: > 140 mmHg

    Returns:
        DataFrame with columns location_id, year_id, age_group_id, sex_id, draw, exp_mean,
            exp_sd, threshold
    """
    exposure_long = exposure.melt(
        id_vars=constants.DEMOGRAPHIC_COLS,
        value_vars=draw_cols,
        var_name="draw",
        value_name="exp_mean",
    )
    exposure_sd_long = exposure_sd.melt(
        id_vars=constants.DEMOGRAPHIC_COLS,
        value_vars=draw_cols,
        var_name="draw",
        value_name="exp_sd",
    )

    exposure_df = pd.merge(
        exposure_long, exposure_sd_long, on=constants.DEMOGRAPHIC_COLS + ["draw"]
    )

    # Add on exposure threshold. FPG and SBP outcomes are definitional, meaning that they
    # are defined as exposure above a threshold.
    exposure_df = exposure_df.assign(threshold=THRESHOLD_MAP[rei_id])

    return exposure_df
