"""Functions related to calculating RR max."""

import re
from typing import List, Optional

import numpy as np
import pandas as pd

import ihme_cc_risk_utils

from ihme_cc_sev_calculator.lib import constants, logging_utils, parameters

logger = logging_utils.module_logger(__name__)

# RR maxes are unique by cause, age group and sex (not location or year-specific)
INDEX_COLS: List[str] = ["cause_id", "age_group_id", "sex_id"]


def calculate_rr_max(
    rei_id: int, params: parameters.Parameters, rei_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Calculate RRmax for risks with uploaded relative risks."""
    cause_metadata = params.read_cause_metadata()
    distal_rei_metadata = rei_metadata.query(f"rei_id == {rei_id}")
    me_ids = params.read_rei_me_ids(rei_id)
    rr_metadata = params.read_rr_metadata(rei_id)

    # Drop PAFs of one causes for this REI.
    rr_metadata = drop_pafs_of_one_causes(
        rr_metadata=rr_metadata, params=params, rei_id=rei_id
    )

    # For continuous risks: pull exposure max/min, TMREL, and if two-stage mediation, deltas
    if (
        rr_metadata["relative_risk_type_id"]
        .isin([constants.LOG_LINEAR_RR_ID, constants.EXPOSURE_DEPENDENT_RR_ID])
        .any()
    ):
        exp = ihme_cc_risk_utils.get_exposure(
            rei_id=rei_id,
            release_id=params.release_id,
            me_ids=me_ids,
            rei_metadata=distal_rei_metadata,
        )
        exp_max = ihme_cc_risk_utils.get_exposure_min_max(
            rei_id=rei_id,
            exposure=exp,
            rei_metadata=distal_rei_metadata,
            percentile=constants.EXPOSURE_MAX_PERCENTILE,
            return_min_or_max=True,
        )
        tmrel = ihme_cc_risk_utils.get_tmrel_draws(
            rei_id=rei_id,
            release_id=params.release_id,
            n_draws=params.n_draws,
            me_ids=me_ids,
            rei_metadata=distal_rei_metadata,
            index_cols=[col for col in INDEX_COLS if col != "cause_id"],
            year_id=params.year_ids,
            age_group_id=params.age_group_ids,
            sex_id=params.sex_ids,
        )

        # If risk uses two-stage mediation, read delta draws for all mediators
        delta = (
            ihme_cc_risk_utils.get_mediation_delta(
                rei_id=rei_id, release_id=params.release_id, n_draws=params.n_draws
            )
            if "delta" in rr_metadata["source"].tolist()
            else None
        )

    logger.info(
        f"Reading relative risk draws by cause for {len(rr_metadata)} cause(s) and "
        "finding RR at max risk exposure"
    )
    rr_max_list = []
    for _, row in rr_metadata.iterrows():
        logger.info(f"Calculating RRmax for cause {row['cause_id']}")
        rr = ihme_cc_risk_utils.get_rr_draws(
            rei_id=row["med_id"],
            release_id=params.release_id,
            n_draws=params.n_draws,
            cause_id=row["cause_id"],
            year_id=params.year_ids if row["year_specific"] else None,
            model_version_id=row["model_version_id"],
            cause_metadata=cause_metadata,
        )
        mediator_rei_metadata = rei_metadata.query(f"rei_id == {row['med_id']}")

        if row["source"] == "delta":
            # Verify we actually have estimates for delta (mostly for typechecking)
            if delta is None:
                raise RuntimeError("delta cannot be None for two-stage mediation")
            else:
                med_delta = delta.query(f"med_id == {row['med_id']}")
        else:
            med_delta = None

        if row["relative_risk_type_id"] == constants.CATEGORICAL_RR_ID:
            rr_max = calculate_categorical_rr_max(rr=rr, draw_cols=params.draw_cols)
        elif row["relative_risk_type_id"] == constants.EXPOSURE_DEPENDENT_RR_ID:
            # Pull mediator exposure max if applicable
            if row["source"] == "delta":
                logger.info(f"Preparing estimates for mediator {row['med_id']}")
                med_exp = ihme_cc_risk_utils.get_exposure(
                    rei_id=row["med_id"],
                    release_id=params.release_id,
                    me_ids=me_ids,
                    rei_metadata=mediator_rei_metadata,
                )
                # When the mediator is high systolic blood pressure and the cause is
                # hypertensive heart disease, we use this median threshold for the
                # mediator exposure max, because the absolute risk curve has regions
                # of zero risk.
                median_threshold = (
                    constants.HYPERTENSION_THRESHOLD
                    if row["med_id"] == constants.HIGH_SBP_REI_ID
                    and row["cause_id"] == constants.HYPERTENSIVE_HD_CAUSE_ID
                    else None
                )
                med_exp_max = ihme_cc_risk_utils.get_exposure_min_max(
                    rei_id=row["med_id"],
                    exposure=med_exp,
                    rei_metadata=mediator_rei_metadata,
                    percentile=constants.EXPOSURE_MAX_PERCENTILE,
                    median_threshold=median_threshold,
                    return_min_or_max=True,
                )

                # For exposure-dependent RR models, calculate the delta shift at exp max
                med_delta = calculate_delta_shift(
                    delta=med_delta,
                    exp_max=exp_max,
                    tmrel=tmrel,
                    inv_exp=bool(distal_rei_metadata["inv_exp"].iat[0]),
                    draw_cols=params.draw_cols,
                )
            else:
                med_exp_max = None

            rr_max = calculate_exposure_dependent_rr_max(
                rr=rr,
                exp_max=exp_max,
                tmrel=tmrel,
                source=row["source"],
                med_exp_max=med_exp_max,
                delta=med_delta,
                draw_cols=params.draw_cols,
            )
        elif row["relative_risk_type_id"] == constants.LOG_LINEAR_RR_ID:
            rr_max = calculate_log_linear_rr_max(
                rr=rr,
                exp_max=exp_max,
                tmrel=tmrel,
                delta=med_delta,
                draw_cols=params.draw_cols,
                inv_exp=bool(distal_rei_metadata["inv_exp"].iat[0]),
                med_inv_exp=bool(mediator_rei_metadata["inv_exp"].iat[0]),
                med_rr_scalar=float(mediator_rei_metadata["rr_scalar"].iat[0]),
            )
        else:
            raise RuntimeError("not yet implemented")

        # Take RR mean over location and year - RRmax does not vary by these dimensions
        # Most RRs do not vary by location or year so this will be a no-op
        rr_max = (
            rr_max[INDEX_COLS + params.draw_cols].groupby(INDEX_COLS).mean().reset_index()
        )
        rr_max["rei_id"] = row["rei_id"]
        rr_max["med_id"] = row["med_id"]
        rr_max_list.append(rr_max)
        # RRmax is appended twice for drugs_illicit_suicide and cocaine/amphetamine, once for
        # each drug, so that the averaging in average_rr_max_across_drug_subrisks gives each
        # of opioids, cocaine, and amphetamines equal weight.
        if (
            rei_id == constants.DRUGS_ILLICIT_SUICIDE_REI_ID
            and me_ids.query(
                f"model_version_id == {row['model_version_id']}"
            ).modelable_entity_id.item()
            == constants.COCAINE_AMPHETAMINE_RR_ME_ID
        ):
            rr_max_list.append(rr_max)

    return pd.concat(rr_max_list)


def calculate_delta_shift(
    delta: pd.DataFrame,
    exp_max: pd.DataFrame,
    tmrel: pd.DataFrame,
    inv_exp: bool,
    draw_cols: List[str],
) -> pd.DataFrame:
    """Calculate delta shift from max distal exposure in units of mediator exposure.

    Represents max distal exposure distance from distal TMREL but in units of mediator
    exposure. Formula:
        * delta * (distal exposure max - distal TMREL)

    Returns:
        Dataframe of delta shifts with columns age_group_id, sex_id, med_id, draw_0, ...
        draw_n with rows for each unique age group/sex/med_id combination.
    """
    distance = pd.merge(exp_max, tmrel, on="age_group_id")

    # Calculate distance from TMREL: distal exposure max - distal TMREL
    # Technically: -1 * (distal TMREL - distal exposure max) to work around pandas syntax
    distance[draw_cols] = -1 * distance[draw_cols].sub(distance["exposure_max"], axis="index")

    # If exp max is 'better' than the TMREL, set distance to 0 (no delta shift).
    # For normal risks,  set the lower bound of the distance to 0 (no negatives).
    # For protective risks, set the upper bound of the distance to 0 (no positives).
    if not inv_exp:
        distance[draw_cols] = distance[draw_cols].clip(lower=0)
    else:
        distance[draw_cols] = distance[draw_cols].clip(upper=0)

    # Expand distance (unique by age group, sex) and delta (unique by med_id)
    # so they have matching indexes in order to calculate delta shift
    index = pd.merge(distance[["age_group_id", "sex_id"]], delta["med_id"], how="cross")
    delta = delta.merge(index, on="med_id")
    distance = distance.merge(index, on=["age_group_id", "sex_id"]).drop(
        columns="exposure_max"
    )

    index_cols = ["med_id", "age_group_id", "sex_id"]
    return (
        delta.set_index(index_cols)[draw_cols] * distance.set_index(index_cols)[draw_cols]
    ).reset_index()


def calculate_categorical_rr_max(rr: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Calculate RRmax for a categorical risk.

    For categorical risks, keep exposure category with highest RR for any
    demographic (max exposure). This is same as first exposure category except
    for risks where categories are not ordered, like back pain and asthmagens.

    Ties are broken by lower category number, ie. 'cat1' over 'cat3'
    """
    rr["mean"] = rr[draw_cols].mean(axis=1)
    max_category = (
        rr.query("mean == mean.max()")["parameter"]
        .sort_values(key=_mixed_string_value)
        .iat[0]
    )
    return rr.query(f"parameter == '{max_category}'").drop(columns="mean")


def _mixed_string_value(s: pd.Series) -> pd.Series:
    """Returns sorting values for an input Series of strings with embedded numbers.

    Ex:
        'cat10', 'cat1', 'cat5' -> 10, 1, 5
    """
    return s.apply(lambda x: re.findall(r"\d+", x)[0]).astype(int)


def calculate_exposure_dependent_rr_max(
    rr: pd.DataFrame,
    exp_max: pd.DataFrame,
    tmrel: pd.DataFrame,
    source: str,
    med_exp_max: Optional[pd.DataFrame],
    delta: Optional[pd.DataFrame],
    draw_cols: List[str],
) -> pd.DataFrame:
    """Calculate RRmax for a continuous risk with an exposure-dependent RR curve.

    A risk curve with exposure on the x axis and relative risk on the y axis is created
    from input draws and connected via linear interpolation.

    For distal risks (no mediation), RRmax is calculated as the relative risk at the maximum
    exposure. TMREL is used to 'normalize' the risk curve such that relative risk at the
    TMREL is 1:
        * RRmax = RR(exposure max) / RR(TMREL)

    For mediator risks (two-stage mediation), RRmax is calculated as the relative risk
    at the maximum mediator exposure + the delta shift divided by the relative risk
    at the maximum mediator exposure:
        * RRmax = RR(exposure max + delta shift) / RR(exposure max)

    Returns:
        Dataframe with RRmax data and columns: cause_id, location_id, sex_id, age_group_id,
            draw_0, ..., draw_n
    """
    # Exposure-dependent RRs: approximate RR at the exposure max
    if source == "delta":
        draws = pd.merge(
            rr, med_exp_max[["age_group_id", "exposure_max"]], on="age_group_id"
        ).merge(delta, on=["sex_id", "age_group_id"], suffixes=("", "_delta"))
        interpolation_function = _interpolate_mediated_relative_risk
    else:
        draws = pd.merge(
            rr, exp_max[["age_group_id", "exposure_max"]], on="age_group_id"
        ).merge(tmrel, on=["sex_id", "age_group_id"], suffixes=("", "_tmrel"))
        interpolation_function = _interpolate_relative_risk

    # RR may or may not be year-specific
    rr_index_cols = ["cause_id", "location_id", "sex_id", "age_group_id"]
    if "year_id" in draws.columns:
        rr_index_cols += ["year_id"]

    # Calculate RRmax as the relative risk at the max exposure for each demographic
    # grouping and draw col
    draws_list = []
    for col in draw_cols:
        draws_list.append(
            draws.groupby(rr_index_cols).apply(interpolation_function, draw_col=col)
        )

    # Append all draw columns together and rename columns back to expected names
    return (
        pd.concat(draws_list, axis=1)
        .reset_index()
        .rename(columns={i: draw_cols[i] for i in range(len(draw_cols))})
    )


def _interpolate_relative_risk(group: pd.DataFrame, draw_col: str) -> float:
    """Interpolate relative risk curve, returning RR at exposure max.

    Linear interpolation of risk curve where exposure is on the x axis and
    relative risk for a particular draw is on the y axis. In this context,
    linear interpolation means a straight line is drawn between exposure,
    relative risk points.

    TMREL (matched with RR by draw number) is used to 'normalize' the risk curve,
    setting relative risk to 1 at the TMREL.

    `group` is assumed to have a single age group - sex combination and exposure
    max and TMREL draws are expected to have a single value per age group - sex.
    """
    # np.interp expects x-coordinate values to be sorted in increasing order
    group = group.sort_values("exposure", ignore_index=True)

    rr_at_exp_max, rr_at_tmrel = np.interp(
        [group["exposure_max"].iat[0], group[draw_col + "_tmrel"].iat[0]],
        xp=group["exposure"],
        fp=group[draw_col],
    )
    return rr_at_exp_max / rr_at_tmrel


def _interpolate_mediated_relative_risk(group: pd.DataFrame, draw_col: str) -> float:
    """Interpolate mediated relative risk curve.

    Linear interpolation of risk curve where exposure is on the x axis and
    relative risk for a particular draw is on the y axis. In this context,
    linear interpolation means a straight line is drawn between exposure,
    relative risk points.

    `group` is assumed to have a single age group - sex combination and exposure
    max and delta draws are expected to have a single value per age group - sex.
    """
    group = group.sort_values("exposure", ignore_index=True)

    rr_at_med_exp_max, rr_at_shifted_med_exp_max = np.interp(
        [
            group["exposure_max"].iat[0],
            group["exposure_max"].iat[0] + group[draw_col + "_delta"].iat[0],
        ],
        xp=group["exposure"],
        fp=group[draw_col],
    )

    if rr_at_med_exp_max == 0:
        raise RuntimeError(
            f"Relative risk at mediator exposure max is 0 for '{draw_col}. Resulting RRmax "
            "would be infinite."
        )

    return rr_at_shifted_med_exp_max / rr_at_med_exp_max


def adjust_rrmax_for_multiple_mediators(
    rr_max_df: pd.DataFrame, draw_cols: List[str]
) -> pd.DataFrame:
    """Adjust RRmax for multiple mediators.

    No-op if there are not multiple mediators for a cause/age group/sex. If there are,
    adjust RRmax as follows:
        * RRmax = SUM(RRmax) - 1

    Returns:
        RRmax dataframe with columns: cause_id, age_group_id, sex_id, draw_0, ..., draw_n
    """
    draws_list = []
    for col in draw_cols:
        draws_list.append(_collapse_mediators(rr_max_df, col))

    # Append all draw columns together and rename columns back to expected names
    return (
        pd.concat(draws_list, axis=1)
        .reset_index()
        .rename(columns={i: draw_cols[i] for i in range(len(draw_cols))})
    )


def _collapse_mediators(rr_max_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Collapse mediators in rr_max_df for draw column col.

    Having _collapse_mediators as the middle man between applying _collapse_mediators_discrete
    and looping through all draw cols avoids late binding the 'col' variable. Otherwise, it's
    possible that all iterations of the closures defined by lambda use the same (final) value
    for col, draw_n.
    """
    return (
        rr_max_df.groupby(INDEX_COLS)[[col]]
        .agg(**{col: (col, "sum"), "n_mediators": (col, "count")})
        .apply(lambda row: _collapse_mediators_discrete(row[col], row["n_mediators"]), axis=1)
    )


def _collapse_mediators_discrete(rr_max_sum: float, n_mediators: int) -> float:
    """Collapse RRmax for multiple mediators of a discrete unit.

    If any cause/age group/sex has multiple mediators, RRmax across mediators is:
        * SUM(RRmax) - 1

    If there is only one mediator, RRmax is unchanged.
    """
    if n_mediators < 1:
        raise RuntimeError(
            "When adjusting RRmax for muliple mediators, n_mediators had an unexpected "
            f"value: {n_mediators}"
        )
    if n_mediators == 1:
        return rr_max_sum
    else:
        return rr_max_sum - 1


def validate_rr_max(
    rei_id: int, rr_max_df: pd.DataFrame, params: parameters.Parameters
) -> None:
    """Validates RR max.

    Validations:
        * No duplicates by cause/age group/sex
        * No negative or NA values
        * No causes with PAFs of 1
    """
    duplicates = rr_max_df[rr_max_df[INDEX_COLS].duplicated()]
    if not duplicates.empty:
        raise RuntimeError(
            f"Found {len(duplicates)} duplicate(s) after calculating RRmax:\n{duplicates}"
        )

    negatives_or_nas = rr_max_df[
        (rr_max_df[params.draw_cols] < 0).any(axis=1)
        | (rr_max_df[params.draw_cols].isna()).any(axis=1)
    ]
    if not negatives_or_nas.empty:
        raise RuntimeError(
            f"Found {len(negatives_or_nas)} negatives or NA(s) after calculating "
            f"RRmax:\n{negatives_or_nas}"
        )

    # Filter to PAFs of 1 for given risk. Return early if there are none
    pafs_of_one = params.read_file("pafs_of_one").query(f"rei_id == {rei_id}")
    if pafs_of_one.empty:
        return

    pafs_of_one = rr_max_df.merge(pafs_of_one[["rei_id", "cause_id"]], on="cause_id")
    if not pafs_of_one.empty:
        raise RuntimeError(
            f"Found {len(pafs_of_one[['rei_id', 'cause_id']].drop_duplicates())} causes with "
            f"PAFs of one after calculating RRmax:\n{pafs_of_one}"
        )


def _raise_log_linear_two_stage_mediator_exception() -> None:
    """Internal helper (and convenient patch target) raising an exception if two-stage
    mediation uses a log-linear mediator RR curve.
    """
    raise RuntimeError(
        "Two-stage mediation is implemented for log-linear mediators as described in the "
        "docs, but exercise of that code is currently blocked by this exception. If/when "
        "actual use cases arise, the correctness of this calculation (in particular, the use "
        "of mediator, rather than distal, metadata values in the final formula for "
        "RR_{max, distal}) should be checked, taking into account how the relevant deltas "
        "were computed."
    )


def calculate_log_linear_rr_max(
    rr: pd.DataFrame,
    exp_max: pd.DataFrame,
    tmrel: pd.DataFrame,
    delta: Optional[pd.DataFrame],
    draw_cols: List[str],
    inv_exp: bool,
    med_inv_exp: bool,
    med_rr_scalar: float,
) -> pd.DataFrame:
    """Calculate rr_max for log-linear rr."""
    if delta is not None:
        # We do not currently allow access to the log-linear two-stage mediation calculation.
        _raise_log_linear_two_stage_mediator_exception()

    # RR may or may not be year-specific.
    rr_index_cols = ["cause_id", "location_id", "sex_id", "age_group_id"]
    if "year_id" in rr.columns:
        rr_index_cols += ["year_id"]

    # Merge rr, exp_max, and tmrel, and set index.
    merged_data = (
        pd.merge(rr, exp_max[["age_group_id", "exposure_max"]], on="age_group_id")
        .merge(tmrel, on=["sex_id", "age_group_id"], suffixes=("", "_tmrel"))
        .set_index(rr_index_cols)
    )

    # Extract tmrel data with original draw column names.
    tmrel_col_dict = {draw_col + "_tmrel": draw_col for draw_col in draw_cols}
    tmrel = merged_data[tmrel_col_dict.keys()].rename(tmrel_col_dict, axis="columns")

    # Compute normalized differences between exp_max and tmrel,
    # truncating negative values to zero.
    normalized_exposure_differences = (
        (1 if inv_exp else -1)
        * tmrel.sub(merged_data["exposure_max"], axis=0)
        / med_rr_scalar
    ).clip(lower=0)

    if delta is not None:
        # We are doing two-stage mediation, so multiply by delta (a one-row dataframe).
        normalized_exposure_differences *= delta[draw_cols].iloc[0]
        # Adjust sign as needed for distal and mediator inv_exps.
        normalized_exposure_differences *= (-1 if inv_exp else 1) * (-1 if med_inv_exp else 1)

    # Calculate rr_max by exponentiation, reset index, and return result.
    return (merged_data[draw_cols] ** normalized_exposure_differences).reset_index()


def average_rr_max_across_drug_subrisks(rei_id: int, rr_max: pd.DataFrame) -> pd.DataFrame:
    """Average across opioid, cocaine, and amphetamine to get the RRmax for all drug use."""
    if rei_id == constants.DRUGS_ILLICIT_SUICIDE_REI_ID:
        rr_max = rr_max.groupby(INDEX_COLS + ["rei_id", "med_id"]).mean().reset_index()
    return rr_max


def drop_pafs_of_one_causes(
    rr_metadata: pd.DataFrame, params: parameters.Parameters, rei_id: int
) -> pd.DataFrame:
    """Helper function to drop PAFs of one causes for the given REI. We do this to avoid
    unnecessary computation, since SEVs are undefined (division by zero) for PAFs of 1.
    """
    excluded_causes = (  # noqa: F841
        params.read_file("pafs_of_one").query("rei_id == @rei_id").cause_id.tolist()
    )
    return rr_metadata.query("cause_id not in @excluded_causes")
