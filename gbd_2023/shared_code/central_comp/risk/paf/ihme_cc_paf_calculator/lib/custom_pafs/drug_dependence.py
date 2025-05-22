"""Custom PAF calculation for drug dependence (drugs_illicit_suicide)."""

import pathlib
from typing import List

import pandas as pd

import ihme_cc_risk_utils
from gbd.constants import measures, metrics
from get_draws import api as get_draws_api

from ihme_cc_paf_calculator.lib import constants, io_utils, math

# ME mapping for each drug. Cocaine and amphetamine share an RR ME
DRUG_USE_ME_MAP = {
    "opioid": {
        "exposure_modelable_entity_id": 16437,  # Opioid use disorders cases, interpolated
        "rr_modelable_entity_id": 26260,  # Opioid use disorders Relative Risk
    },
    "cocaine": {
        "exposure_modelable_entity_id": 16438,  # Cocaine use disorders cases, interpolated
        "rr_modelable_entity_id": 26261,  # Cocaine or Amphetamine use disorders Relative Risk
    },
    "amphetamine": {
        "exposure_modelable_entity_id": 16439,  # Amphetamine use disorders cases, interpolate
        "rr_modelable_entity_id": 26261,  # Cocaine or Amphetamine use disorders Relative Risk
    },
}


def get_exposure_draws_drug_dependence(
    release_id: int, location_id: int, year_id: List[int], n_draws: int, me_ids: pd.DataFrame
) -> pd.DataFrame:
    """Get exposure draws for drug dependence.

    Pulls exposure models for three MEs: opioid use, cocaine use, amphetamine use.
    These models differ from normal exposure models in that there are saved as prevalence
    draws associated with a particular ME rather than an REI.

    Prevalence draws are converted into dichotomous exposure. The original prevalence
    is considered the exposed population ('cat1') and the residual, unexposed population
    ('cat2'), is calculated as max(0, 1 - prevalence). Exposure is then rescaled so that
    the sum across categories is 1 for each demographic, ME, draw.

    Drug-specific exposure is specified by modelable_entity_id.

    Args:
        release_id: release ID
        location_id: location ID to pull exposure draws for
        year_id: year ID(s) to pull exposure draws for
        n_draws: number of draws to pull
        me_ids: dataframe of ME IDs for drug dependency

    Returns:
        exposure draws with the following columns:
            location_id, age_group_id, sex_id, year_id, modelable_entity_id, parameter,
            draw_0, ..., draw_n-1
    """
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    index_cols = ["location_id", "age_group_id", "sex_id", "year_id", "modelable_entity_id"]
    keep_cols = index_cols + ["parameter"] + draw_cols

    exposure_me_ids = me_ids.query("draw_type == 'exposure'")
    if len(exposure_me_ids) != 3:
        raise RuntimeError(
            "Internal error: expected exactly three drug dependency exposure MEs:\n"
            f"{exposure_me_ids}"
        )

    exposure = get_draws_api.get_draws(
        source="epi",
        gbd_id_type="modelable_entity_id",
        gbd_id=exposure_me_ids["modelable_entity_id"].tolist(),
        location_id=location_id,
        year_id=year_id,
        measure_id=measures.PREVALENCE,
        metric_id=metrics.RATE,
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        version_id=exposure_me_ids["model_version_id"].tolist(),
    ).assign(parameter="cat1")

    # Create a residual category 'cat2' so we have dichotomous exposure.
    # Residual (unexposed) = max(0, 1 - exposed).
    residual_exposure = exposure.copy()
    residual_exposure[draw_cols] = (1 - exposure[draw_cols]).clip(lower=0)
    residual_exposure = residual_exposure.assign(parameter="cat2")

    total_exposure = pd.concat([exposure, residual_exposure]).reset_index(drop=True)[
        keep_cols
    ]

    # Rescale exposure sums to 1.
    total_exposure[draw_cols] = total_exposure[draw_cols] / total_exposure.groupby(
        index_cols
    )[draw_cols].transform("sum")
    return total_exposure


def calculate_drugs_illicit_suicide(
    root_dir: pathlib.Path,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
) -> None:
    """Calculates custom PAFs for drug dependence, drugs_illicit_suicide, REI ID 140.

    For each drug (opioids, cocaine, amphetamine), we calculate categorical PAFs with
    dichotomous exposure modeled as drug use prevalence. PAFs for the three drugs are then
    aggregated using the standard PAF aggregation equation.

    Intended only for use within the PAF Calculator.
    """
    if settings.rei_id != constants.DRUG_DEPENDENCE_REI_ID:
        raise RuntimeError(
            "Internal error: function can only be run for drug dependence, REI ID "
            f"{constants.DRUG_DEPENDENCE_REI_ID}. Got rei_id {settings.rei_id}"
        )

    me_ids = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA).query(
        f"cause_id == {cause_id}"
    )

    # Cached exposure contains exposure draws for all three drugs
    exposure = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
    )

    paf_list = []
    for drug_name in ["opioid", "cocaine", "amphetamine"]:
        paf_list.append(
            _calculate_drug_specific_paf(
                drug_name=drug_name,
                location_id=location_id,
                cause_id=cause_id,
                settings=settings,
                me_ids=me_ids,
                rr_metadata=rr_metadata,
                exposure=exposure,
            )
        )

    paf = pd.concat(paf_list)

    # Calculate the joint PAF using the standard PAF aggregation equation
    aggregated_paf = math.aggregate_pafs(paf_df=paf, collapse_cols=[])

    io_utils.write_paf(aggregated_paf, root_dir, location_id)


def _calculate_drug_specific_paf(
    drug_name: str,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
    me_ids: pd.DataFrame,
    rr_metadata: pd.DataFrame,
    exposure: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate drug dependency PAFs for a specific drug: opioids, cocaine, or amphetamine.

    Once exposure is filtered to a specific drug, proceed as normal for a categorical risk.
    Age restrictions are implicitly handled by inner join between existing age groups in
    rr and exposure.

    Returns:
        paf dataframe with the following columns:
            rei_id, location_id, year_id, age_group_id, sex_id, cause_id, mortality,
            morbidity, draw_0, ..., draw_n-1
    """
    exposure = exposure.query(
        f"modelable_entity_id == {DRUG_USE_ME_MAP[drug_name]['exposure_modelable_entity_id']}"
    ).drop(columns="modelable_entity_id")
    rr_model_version_id = me_ids.query(
        f"modelable_entity_id == {DRUG_USE_ME_MAP[drug_name]['rr_modelable_entity_id']}"
    )["model_version_id"].iat[0]
    rr_is_year_specific = (
        rr_metadata.query(f"model_version_id == {rr_model_version_id}")["year_specific"] == 1
    ).bool()

    rr = ihme_cc_risk_utils.get_rr_draws(
        rei_id=settings.rei_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
        cause_id=cause_id,
        year_id=settings.year_id if rr_is_year_specific else None,
        location_id=location_id,
        model_version_id=rr_model_version_id,
    ).drop(
        columns=[
            "model_version_id",
            "metric_id",
            "modelable_entity_id",
            "location_id",
            "exposure",
        ]
    )
    rr = ihme_cc_risk_utils.add_tmrel_indicator_to_categorical_data(rr)
    return math.calculate_categorical_paf(rr, exposure)
