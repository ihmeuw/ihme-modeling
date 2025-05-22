from typing import Tuple

import pandas as pd

import db_queries.api.internal as db_queries_api_internal
import ihme_cc_risk_utils
from gbd import gbd_round, release
from ihme_cc_risk_utils.lib import lbwsg

from ihme_cc_sev_calculator.lib import constants, logging_utils, parameters

logger = logging_utils.module_logger(__name__)

GLOBAL_LOCATION_ID: int = 1


def calculate_rr_max_for_lbwsg(rei_id: int, params: parameters.Parameters) -> pd.DataFrame:
    """Calculate RRmax for LBW/SG and its child risks.

    RR and exposure for LBW/SG is a two dimensional grid where one dimension is low birth
    weight and the other is short gestation. For all three risks (the parent risk, LBW/SG,
    and the two child risks), the calculation uses global LBW/SG RR and exposure. The RR
    model is not year-specific. Exposure is pulled for an arbitrary year, which is set to
    the last reporting year for the LBW/SGA exposure best release.

    For a child risk, collapse RR and exposure from two dimensions to one across the relevant
    dimension. RR is rescaled, and the max exposure category is calculated. The function
    returns the collapsed (one-dimensional) RR for the max exposure category.

    For the parent risk (LBW/SG), find the max exposure category for both child risks
    (repeating process described above for each). The function returns RR for the exposure
    category that matches both max 1D exposure categories.
    """
    if rei_id not in constants.LBW_PRETERM_REI_IDS:
        raise ValueError(f"Only LBW/SG-related REI IDs are supported, not rei_id {rei_id}")

    # Expect a single RR model for LBW/SG for a single cause
    rr_metadata = params.read_rr_metadata(rei_id=constants.LBWSGA_REI_ID)
    if len(rr_metadata) != 1:
        raise RuntimeError(
            "Expected rr_metadata for LBW/SG to contain a single row. Received:\n"
            f"{rr_metadata}"
        )

    rr_metadata = rr_metadata.to_dict("records")[0]

    # Expect RR model is not year specific
    if rr_metadata["year_specific"]:
        raise RuntimeError(
            "LBW/SG RR model is marked as year-specific. Expected not year-specific."
        )

    # Pull global LBW/SG RR
    rr = ihme_cc_risk_utils.get_rr_draws(
        rei_id=rr_metadata["rei_id"],
        release_id=params.release_id,
        n_draws=params.n_draws,
        cause_id=rr_metadata["cause_id"],
        year_id=None,  # RR is already validated to not be year-specific
        model_version_id=rr_metadata["model_version_id"],
        cause_metadata=params.read_cause_metadata(constants.LBWSG_CAUSE_SET_ID),
    ).drop(columns="location_id")

    # Pull global LBW/SG exposure for a single year (the year corresponding with the
    # best release for LBW/SG exposure)
    best_release = (
        db_queries_api_internal.get_risk_factor_modelable_entities(
            rei_id=constants.LBWSGA_REI_ID, release_id=params.release_id
        )
        .query('draw_type=="exposure"')
        .release_id.unique()
        .item()
    )
    arbitrary_year_id = int(
        gbd_round.gbd_round_from_gbd_round_id(
            release.get_gbd_round_id_from_release(best_release)
        )
    )
    logger.info(
        f"Arbitrary year selected for pulling global LBW/SG exposure: {arbitrary_year_id}"
    )
    global_exposure = ihme_cc_risk_utils.get_exposure_draws(
        rei_id=constants.LBWSGA_REI_ID,
        release_id=params.release_id,
        location_id=GLOBAL_LOCATION_ID,
        year_id=arbitrary_year_id,
        n_draws=params.n_draws,
    )

    # If child risk, find max exposure category, collapse RR, and return
    # collapsed RR for that category
    if rei_id != constants.LBWSGA_REI_ID:
        collapsed_rr, exp_max_category = collapse_and_calculate_max_exposure_category(
            rei_id=rei_id, rr=rr, exposure=global_exposure, n_draws=params.n_draws
        )
        logger.info(
            f"For rei_id {rei_id}, one-dimensional exposure max category is "
            f"{exp_max_category}."
        )
        return collapsed_rr.query(f"parameter == {exp_max_category}")
    else:
        # For LBW/SG, find the max exposure category for each dimension
        # (LBW, SG) and return RR for the respective two-dimensional category
        collapsed_rr, sg_exp_max_category = collapse_and_calculate_max_exposure_category(
            rei_id=constants.SHORT_GESTATION_REI_ID,
            rr=rr,
            exposure=global_exposure,
            n_draws=params.n_draws,
        )
        _, lbw_exp_max_category = collapse_and_calculate_max_exposure_category(
            rei_id=constants.LOW_BIRTH_WEIGHT_REI_ID,
            rr=rr,
            exposure=global_exposure,
            n_draws=params.n_draws,
        )
        two_d_exp_max_category = lbwsg.PARAMETER_MAP.query(
            f"preterm == {sg_exp_max_category} & lbw == {lbw_exp_max_category}"
        )["parameter"].iat[0]

        logger.info(
            f"For LBW/SG, rei_id {rei_id}, one-dimensional exposure max categories are "
            f"{sg_exp_max_category} (short gestation) and {lbw_exp_max_category} ("
            f"low birth weight). Two-dimensional category is {two_d_exp_max_category}."
        )
        return rr.query(f"parameter == '{two_d_exp_max_category}'")[collapsed_rr.columns]


def collapse_and_calculate_max_exposure_category(
    rei_id: int, rr: pd.DataFrame, exposure: pd.DataFrame, n_draws: int
) -> Tuple[pd.DataFrame, int]:
    """Collapse RR and exposure to 1D and calculate max exposure category for child risk.

    Max exposure category in this case is different than other categorical risks. It's defined
    as the first category above the 95th percentile of mean global exposure.

    We calculate mean global exposure for each category across age group, sex and draws. Then,
    going from the best case (40 weeks for short gestation, 4000 g for low birth weight) to
    worst case, the max exposure category is the first category whose cumulative sum of
    the mean global exposure meets or surpasses 0.95.

    Returns:
        Tuple of collapsed RR dataframe with columns age_group_id, sex_id, cause_id,
            mortality, morbidity, parameter, draw_0, ..., draw_n and one-dimensional
            max exposure category
    """
    draw_cols = [f"draw_{i}" for i in range(n_draws)]

    # Collapse RR and exposure to a single dimension (either LBW or short gestation)
    rr, exposure = lbwsg.collapse_lbwsg_rr_and_exposure(
        rei_id=rei_id, rr=rr, exposure=exposure, global_exposure=exposure, n_draws=n_draws
    )

    # Take exposure mean across draws by category.
    # Mean of subsample means is the same as the mean of all observations since
    # the subsamples have the same number of data point
    exposure_mean = exposure.groupby("parameter")[draw_cols].mean().reset_index()
    exposure_mean["mean"] = exposure_mean[draw_cols].mean(axis=1)
    exposure_mean = exposure_mean[["parameter", "mean"]]

    # Sort categories by descending order since child risks are protective
    # and calculate cumulative sum from TMREL to highest risk
    exposure_mean = exposure_mean.sort_values("parameter", ascending=False)
    exposure_mean["cumulative_mean"] = exposure_mean["mean"].cumsum()

    # Grab first category at or above the 95th percentile
    exposure_mean.loc[
        exposure_mean["cumulative_mean"] >= 1 - constants.EXPOSURE_MAX_PERCENTILE,
        "above_percentile",
    ] = 1
    exp_max_category = exposure_mean.query("above_percentile == 1")["parameter"].iat[0]

    return (rr, exp_max_category)
