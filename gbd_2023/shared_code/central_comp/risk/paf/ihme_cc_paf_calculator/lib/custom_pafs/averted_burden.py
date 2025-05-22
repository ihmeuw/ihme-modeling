"""Custom PAF calculation for Averted Burden.

Used for drug class interventions in the Averted Burden project.
"""

from typing import Optional

import pandas as pd

import ihme_cc_averted_burden

from ihme_cc_paf_calculator.lib import constants, data_utils, logging_utils

logger = logging_utils.module_logger(__name__)


def calculate_unavertable_paf(
    settings: constants.PafCalculatorSettings, avertable_draws: pd.DataFrame
) -> Optional[pd.DataFrame]:
    """Calculate unavertable PAFs.

    Unavertable PAFs are only generated when either the intervention REI ID or REI ID
    corresponds to an avertable drug counterfactual. If the REI ID corresponds to an averted
    counterfactual, no action is taken.

    Unavertable PAFs are calculated as 1 - Avertable PAF where Avertable PAF is floored at 0
    if there are any negative draws such that Unavertable is not > 100%.
    """
    drug_rei_id = settings.intervention_rei_id or settings.rei_id
    drug_rei_ids = ihme_cc_averted_burden.get_all_rei_ids_for_drug(drug_rei_id)
    if drug_rei_ids.avertable_rei_id == drug_rei_id:
        _, draw_cols = data_utils.get_index_draw_columns(avertable_draws)

        # First, check if avertable PAFs are greater than or equal to zero.
        # Sometimes RR draws are harmful, and these should be treated as 1.
        negative_mask = (avertable_draws[draw_cols] < 0).any()
        if negative_mask.any():
            logger.info("Setting floor of 0 for avertable PAF draws under 0.")

        # Unavertable draws are the complement of the avertable draws floored at 0.
        unavertable_draws = avertable_draws.copy()
        unavertable_draws[draw_cols] = 1 - unavertable_draws[draw_cols].clip(lower=0)
        unavertable_draws = unavertable_draws.assign(rei_id=drug_rei_ids.unavertable_rei_id)

        return unavertable_draws


def adjust_categorical_exposure_for_max(exposure: pd.DataFrame) -> pd.DataFrame:
    """Re-scale categorical exposures when the max coverage is not 100% of the population.

    Drugs where the health outcome is a cause are modeled as categorical PAFs. For some,
    not all the population is eligible for the intervention. In these cases, two
    exposure MEs are modeled rather than one. cat1 still reflects the proportion of the
    total population covered by the intervention, and cat2 will reflect the proportion
    of the total population that is eligible, true max coverage.

    Coverage is then re-calculated as 1 - (max coverage - coverage), and the residual
    exposure is also re-calculated such that together they still sum to 100%.
    """
    # Confirm there are exactly two or three categories of exposure present.
    params_in_exposure = set(exposure["parameter"])
    if len(params_in_exposure) == 2:
        return exposure
    elif len(params_in_exposure) != 3:
        raise RuntimeError(
            "Expected only two or three (in the case of max coverage not equal to 100%) "
            f"exposure categories, found: {params_in_exposure}."
        )

    # Pull out the two modeled categories, coverage and max coverage, and set index.
    _, draw_cols = data_utils.get_index_draw_columns(exposure)
    coverage = exposure.query("parameter == 'cat1'").set_index(constants.DEMOGRAPHIC_COLS)
    max_coverage = exposure.query("parameter == 'cat2'").set_index(constants.DEMOGRAPHIC_COLS)
    # Re-calculate coverage based on max coverage.
    coverage[draw_cols] = 1 - (max_coverage[draw_cols] - coverage[draw_cols])

    # Ensure that proportion covered is always less than max covered.
    rows_above_1 = coverage.loc[coverage[draw_cols].gt(1).any(axis=1)]
    if not rows_above_1.empty:
        raise RuntimeError(
            "Found demographics where max coverage is less than coverage. Example:\n"
            f"{rows_above_1.reset_index()[constants.DEMOGRAPHIC_COLS].head()}"
        )

    # Calculate residual given updated coverage.
    max_coverage[draw_cols] = 1 - coverage[draw_cols]

    return pd.concat([coverage, max_coverage]).reset_index()
