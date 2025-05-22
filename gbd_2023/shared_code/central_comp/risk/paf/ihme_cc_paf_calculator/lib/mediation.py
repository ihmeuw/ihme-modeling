import numpy as np
import pandas as pd

import ihme_cc_risk_utils
from ihme_dimensions import dfutils

from ihme_cc_paf_calculator.lib import constants, logging_utils

logger = logging_utils.module_logger(__name__)


def get_total_mediation_factor_draws(
    rei_id: int, n_draws: int, release_id: int
) -> pd.DataFrame:
    """Gets total mediation factor draws for the given rei_id.

    Mediation factor refers to what proportion of a risk - cause is mediated through
    exposure to another risk. Total mediation factor is the proportion of risk - cause
    burden mediated via exposure to any other risk. To compute total mediation factors,
    this function aggregates across all mediators per cause.

    Relatedly, the residual (1 - total mediation) is the proportion of the risk's effect that
    is directly due to exposure to the given risk after accounting for mediation, which
    is used to compute unmediated PAFs.

    Simple example:
        * Risk - cause pair: Diet low in fruits - Ischemic stroke
        * This pair is mediated by three risks: high FPG, high SBP, and high LDL cholesterol
        * All three risks have a mediation factor of 0.05
        * Total mediation factor: 1 - ((1 - 0.05) * (1 - 0.05) * (1 - 0.05)) = 0.14
        * Interpretation: 14% of diet low in fruits' effect on CKD is mediated by other risks
            and 86% (1 - 0.14) is due to diet low in fruits alone

    Returns an empty DataFrame if mediation does not apply to the risk for any causes, or if
    all causes are 100% mediated. Either way, unmediated PAFs do not need to be computed.
    Also accounts for an edge case with 2-stage mediation split between multiple mediators,
    which are 100% mediated when taken together and should be removed from returned mediation
    factors.

    Args:
        rei_id: REI ID
        n_draws: number of draws. If less than 1000, downsamples risk mediation matrix
        release_id: release ID

    Returns:
        DataFrame with cause_id and total mediation factor draws
    """
    draw_cols = [f"draw_{i}" for i in range(n_draws)]

    matrix = ihme_cc_risk_utils.get_risk_mediation_matrix(rei_id, release_id)
    if matrix.empty:
        return pd.DataFrame(columns=["cause_id"] + draw_cols)

    # Downsample if n_draws < 1000. No-op if n_draws == 1000
    matrix = dfutils.resample(matrix, n_draws=n_draws)

    # Drop rows for an edge case where 2-stage mediation is split between multiple
    # mediators. 2-stage mediation relationships don't use mediation factors directly
    for cause_id, mediators in constants.CAUSES_WITH_MULTIPLE_2_STAGE_MEDIATORS.items():
        if set(matrix.query(f"cause_id == {cause_id}")["med_id"]) == set(mediators):
            matrix = matrix[matrix["cause_id"] != cause_id]

    # Aggregate draws across mediators to get total mediation for the risk
    aggregated_matrix = (
        matrix.groupby(["cause_id"])
        .apply(lambda row: 1 - (1 - row[draw_cols]).prod())
        .reset_index()
    )

    # Take mean of mediation factors, dropping any rows where the mean is 1 (100% mediated)
    aggregated_matrix["mean"] = np.mean(aggregated_matrix[draw_cols], axis=1)
    return aggregated_matrix.query("mean != 1").reset_index(drop=True).drop(columns="mean")


def expand_delta(delta: pd.DataFrame, exposure: pd.DataFrame) -> pd.DataFrame:
    """Expand delta dataframe to match demographics in exposure.

    For all cases except salt - SBP, delta is one row. In order to use the delta
    like other input models, we expand to match the demographics in exposure.

    Args:
        delta: delta df with columns: rei_id, med_id, draw_0, ..., draw_n
        exposure: exposure df with columns: location_id, year_id, age_group_id, sex_id,
            draw_0, ..., draw_n

    Returns:
        delta with all original columns with the same number of rows as exposure
    """
    if len(delta) != 1:
        raise ValueError(f"Expected delta to contain a single row:\n{delta}")

    delta = delta.merge(exposure[constants.DEMOGRAPHIC_COLS], how="cross")

    if len(exposure) != len(delta):
        raise RuntimeError(
            f"Internal error: exposure df length ({len(exposure)}) does not match "
            f"expanded delta length ({len(delta)})."
        )

    return delta


def restrict_mediation_factors_to_rr_metadata(
    mediation_factors: pd.DataFrame, rr_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Restrict mediation_factors to causes in rr_metadata."""
    drop_causes = set(mediation_factors.cause_id) - set(rr_metadata.cause_id)
    if drop_causes:
        logger.warning(
            f"Found cause ID(s) {drop_causes} in mediation_factors but not rr_metadata. No "
            "PAFs will be computed for these causes. Dropping the following row(s) from "
            f"mediation_factors:\n{mediation_factors.query('cause_id in @drop_causes')}"
        )
        mediation_factors = mediation_factors.query("cause_id not in @drop_causes")
    return mediation_factors
