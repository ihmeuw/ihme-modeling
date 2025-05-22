"""Utilities for calculating PAFs for interventions.

Currently assumes all interventions are drugs in the Averted Burden REI set
as validated in cli.launch_paf_calculator.
"""

from typing import List

import pandas as pd

import ihme_cc_averted_burden
import ihme_cc_risk_utils


def get_intervention_coverage_draws(
    intervention_rei_id: int,
    release_id: int,
    location_id: int,
    year_id: List[int],
    n_draws: int,
) -> pd.DataFrame:
    """Read draws of coverage for intervention and process given counterfactual scenario."""
    coverage = ihme_cc_risk_utils.get_exposure_draws(
        rei_id=intervention_rei_id,
        release_id=release_id,
        location_id=location_id,
        year_id=year_id,
        n_draws=n_draws,
    )
    # Interventions have dichotomous exposure, keep only coverage, "cat1", if averted or
    # 1 - coverage, "cat2", if avertable.
    keep_parameter = (
        "cat1"
        if intervention_rei_id
        == ihme_cc_averted_burden.get_all_rei_ids_for_drug(intervention_rei_id).averted_rei_id
        else "cat2"
    )
    return coverage.loc[coverage["parameter"] == keep_parameter].drop(columns="parameter")


def get_intervention_effect_size_draws(
    intervention_rei_id: int, rei_id: int, n_draws: int
) -> pd.DataFrame:
    """
    Read and format draws of effect size between intervention and risk factor.

    Additionally adds on column for threshold of risk exposure at or above which population
    is eligible for intervention coverage.
    """
    effect_size = (
        ihme_cc_averted_burden.get_drug_risk_effect_size_draws(
            drug_rei_id=intervention_rei_id, risk_rei_id=rei_id, n_draws=n_draws
        )
        .drop(columns=["drug_rei_id", "risk_rei_id"])
        .assign(
            exposure_threshold=ihme_cc_averted_burden.get_risk_exposure_threshold(
                rei_id=rei_id
            )
        )
    )
    # Multiply effect size by -1 if averted, and keep as-is if avertable.
    if (
        intervention_rei_id
        == ihme_cc_averted_burden.get_all_rei_ids_for_drug(intervention_rei_id).averted_rei_id
    ):
        draw_cols = [f"draw_{i}" for i in range(n_draws)]
        effect_size[draw_cols] = -1 * effect_size[draw_cols]
    return effect_size


def modify_exposure_edensity_for_intervention(
    exposure_density_vectors: pd.Series,
    intervention_coverage: pd.Series,
    intervention_effect_size: pd.DataFrame,
) -> pd.Series:
    """
    Create an intervention counterfactual version of a given risk exposure density function.

    Counterfactuals are generated for each draw in a single demographic.

    Written specifically to work across and mimic the output of ensemble_utils.get_edensity.
    If that output changes, this function must change as well.
    """
    for draw in exposure_density_vectors.index:
        counterfactual_vectors = ihme_cc_averted_burden.create_counterfactual_distribution(
            edensity_vectors=exposure_density_vectors[draw],
            exposure_threshold=float(intervention_effect_size["exposure_threshold"]),
            coverage=float(intervention_coverage[draw]),
            effect_size=float(intervention_effect_size[draw]),
            effect_size_is_absolute=bool(intervention_effect_size["is_absolute"].iat[0]),
            force_integrate_to_one=True,
        )
        counterfactual_vectors["fx"] = counterfactual_vectors["fx_hat"]

        exposure_density_vectors[draw] = {
            key: counterfactual_vectors[key] for key in exposure_density_vectors[draw]
        }

    return exposure_density_vectors
