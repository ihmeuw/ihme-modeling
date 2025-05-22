import warnings
from typing import List, Optional

import numpy as np
import pandas as pd

from gbd.constants import measures
from get_draws import api as get_draws_api

from ihme_cc_sev_calculator.lib import constants, io, risk_prevalence, risks, rr_max

PAF_INDEX_COLS: List[str] = constants.DEMOGRAPHIC_COLS + ["cause_id"]


def paf_and_rr_max_indexes_must_match(
    rei_id: int, aggregate_rei_ids: List[int], rei_metadata: pd.DataFrame
) -> bool:
    """Returns True if indexes for input PAFs and RRmaxes must exactly match.

    False otherwise. Current rules for determining if indexes must match:
        * Risk is an aggregate risk.
        * Risk has a most detailed child risk that does not model RR and therefore
            we do not have RRmax for.

    Under these conditions, the PAFs can have a wider set of indexes (primarily age groups)
    if we have PAFs for the aggregate risk that include the child risk without an
    RR model AND that child risk has dimensions that the other child risks don't have.

    There are also exceptions for risks that we know have PAFs with more indexes.

    Example:
        Other environment risks (89) has 3 most detailed child risks: residential radon (90),
        blood lead exposure (242), and bone lead exposure (243). Blood lead does not have an
        RR model. It has custom PAFs with younger age groups than PAFs for radon and
        bone lead, which lead to PAFs for the parent risk having the extra younger age groups.
        But RRmax for other environmental risks does not have those younger age groups.
    """
    child_rei_ids = risks.get_child_rei_ids(rei_id, rei_metadata, exclude_no_rr_rei_ids=False)

    is_aggregate_risk = rei_id in aggregate_rei_ids
    has_child_risks_without_rr = (
        len(set(child_rei_ids).intersection(constants.NO_RR_REI_IDS)) > 0
    )

    # TODO, TEMPORARY: There are some risks where we know the PAF models have extra dimensions
    # that the RR models do not in GBD 2021. Should this continue to be allowed?
    known_rei_ids_with_extra_paf_dimensions = [
        # Air pollution: PAFs have additional cause: cataracts (671).
        # This cause is not a PAF of 1 for any risk.
        85,
        # Household air pollution from solid fuels: PAFs have additional cause: cataracts.
        # This cause is not a PAF of 1 for any risk.
        87,
        # Tobacco: aggregate risk that includes smoking, so extra causes in smoking PAFs
        # affect tobacco.
        98,
        # Smoking: PAFs have converted injury causes from PAF Aggregator but custom RRmax
        # still has hip/no hip fractures. CC almost definitely should address this case.
        # These injury causes are being dropped.
        99,
        # Metabolic risks (aggregate risk): PAFs have post-conversion injury causes
        # (690, 691, 692, 693, 694, 695, 697, 707, 711, 727). Probably from smoking.
        104,
        # Occupational carcinogens: PAFs have additional cause: mesothelioma (483).
        # Carcinogens - mesothelioma is a direct PAF. There are no RRs.
        127,
        # Occupational exposure to asbestos: PAFs have an additional cause: mesothelioma (483)
        # Carcinogens - mesothelioma is a direct PAF. There are no RRs.
        150,
        # Particulate matter pollution: PAFs have additional cause: cataracts (671).
        # This cause is not a PAF of 1 for any risk.
        380,
    ]
    pafs_have_extra_dimensions = rei_id in known_rei_ids_with_extra_paf_dimensions

    return not (
        (is_aggregate_risk and has_child_risks_without_rr) or pafs_have_extra_dimensions
    )


def calculate_sevs(
    paf_df: pd.DataFrame,
    rr_max_df: pd.DataFrame,
    draw_cols: List[str],
    indexes_must_match: bool,
) -> pd.DataFrame:
    """Calculate SEVs.

    Essentially a relative risk-weighted prevalence of exposure.

    Formula:
        SEV = (PAF / (1 - PAF)) / (RRmax - 1)

    Bounds input PAFs to >= 0. Bounds resulting SEVs to [0, 1]. Infinite SEVs or missing
    SEVs will cause and error.

    Creates SEVs for all demographics in paf_df. Excess RRmax demographics are ignored.
    """
    # Expand RRmax to match PAF demographics, particularly for location and year.
    # RRmax demographics that don't match any PAF estimates will be dropped and that's ok
    expanded_rr_max_df = pd.merge(
        paf_df[PAF_INDEX_COLS], rr_max_df.drop(columns="rei_id"), on=rr_max.INDEX_COLS
    )

    paf_df = paf_df[PAF_INDEX_COLS + draw_cols].set_index(PAF_INDEX_COLS).sort_index()
    expanded_rr_max_df = (
        expanded_rr_max_df[PAF_INDEX_COLS + draw_cols].set_index(PAF_INDEX_COLS).sort_index()
    )

    # Invariant check: PAFs and RRmax should have matching demographics at this point UNLESS
    # explicitly allowed (see paf_and_rr_max_indexes_must_match)
    if not pd.Index.equals(paf_df.index, expanded_rr_max_df.index):
        msg = (
            "Indexes for input PAFs and RRmax are not equal. There are additional PAF "
            "dimensions that do not exist in RRmax."
        )
        if indexes_must_match:
            mismatched_indexes = _get_mismatched_indexes(paf_df, expanded_rr_max_df)
            raise RuntimeError(msg + f"\n{mismatched_indexes}")
        else:
            # If indexes are allowed to mismatch, warn and drop indexes outside intersection
            warnings.warn(msg + " Keeping only indexes that are shared.")
            shared_indexes = paf_df.index.intersection(expanded_rr_max_df.index)
            paf_df = paf_df.loc[shared_indexes]
            expanded_rr_max_df = expanded_rr_max_df.loc[shared_indexes]

    # Set any negative PAFs to 0
    paf_df = paf_df.clip(lower=0.0)

    # Cap PAFs at 0.9999; none of these PAFs should be 1.
    paf_df = paf_df.clip(upper=constants.PAF_DRAW_CAP)

    # Calculate SEVs
    sev_df = (paf_df / (1 - paf_df)) / (expanded_rr_max_df - 1)

    # If RR <= 1, set SEV to 0
    sev_df[expanded_rr_max_df <= 1] = 0

    # Check for infinites, which are most likely a sign of unaccounted for PAFs of 1.
    # Could also be RRmax = 1
    infinite_sevs = sev_df[sev_df.isin([np.inf, -np.inf]).any(axis=1)]
    if not infinite_sevs.empty:
        raise RuntimeError(
            f"Infinite SEVs calculated for {len(infinite_sevs)} row(s). Are there PAFs of 1?"
            f"\n{infinite_sevs}"
        )

    # Set any value > 1 to 1
    sev_df = sev_df.clip(upper=1.0)

    # If there's any NAs calculated, try to set them to the mean of the draws for the
    # demographic. NAs can occur if RRmax = 1 and PAF = 0, or if any of the inputs already
    # have NAs. This won't work if all draws for a demographic are NA
    if not sev_df[sev_df.isna().any(axis=1)].empty:
        sev_df["draw_mean"] = sev_df[draw_cols].mean(axis=1)
        for draw_col in draw_cols:
            sev_df[draw_col] = sev_df[draw_col].fillna(sev_df["draw_mean"])

        sev_df = sev_df.drop(columns="draw_mean")

        # If there's still NAs, fail
        nas = sev_df[sev_df.isna().any(axis=1)]
        if not nas.empty:
            raise RuntimeError(f"{len(nas)} row(s) have NAs:\n{nas}")

    return sev_df.reset_index()


def _get_mismatched_indexes(
    paf_df: pd.DataFrame, expanded_rr_max_df: pd.DataFrame
) -> pd.DataFrame:
    """Return mismatched indexes between PAFs and expanded RRmax.

    At this point, extra dimensions that might exist in RRmax have already been dropped.
    Only extra dimensions in PAFs will show up.
    """
    return (
        paf_df.reset_index()[PAF_INDEX_COLS]
        .assign(in_pafs=1)
        .merge(
            expanded_rr_max_df.reset_index()[PAF_INDEX_COLS].assign(in_rr_max=1), how="outer"
        )
        .query("in_rr_max != 1")
    )


def aggregate_paf_to_parent_cause(
    paf_df: pd.DataFrame,
    rr_max_df: pd.DataFrame,
    parent_id: int,
    location_id: int,
    year_id: int,
    release_id: int,
    n_draws: int,
    draw_cols: List[str],
    codcorrect_version_id: int,
    como_version_id: Optional[int],
    population: Optional[pd.DataFrame],
    cause_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregates child cause PAFs to parent cause for specific parent causes.

    Implemented for LBW/SG and CKD-related PAFs. Aggregates child cause PAFs
    to the parent cause as the cause burden-weighted sum of child cause PAFs. Method
    is the same as the Burdenator.

    If como_version_id is not given (LBW/SG), uses YLLs from CodCorrect as the cause burden.
    If como_version_id IS given (CKD-related causes), uses YLLs from CodCorrect and YLDs
    from COMO to calculate DALYs (YLLs + YLDs = DALYs) as the cause burden. Aggregates as:

        * Parent cause PAF = sum(child cause PAF * child cause burden) / total cause burden

    Total cause burden should be equivalent to parent cause burden but is more resiliant to
    variation caused by downsampling. It also covers the case where not all child causes
    are included in the input PAFs.

    Returns PAFs for unrelated causes unaltered. Does not return original child cause PAFs
    UNLESS there are also RRmax estimates for any of the child causes.

    Returns:
        Dataframe of PAFs with columns: location_id, year_id, age_group_id, sex_id,
            cause_id, draw_0, ..., draw_n
    """
    child_ids = cause_metadata.query(f"parent_id == {parent_id} & most_detailed == 1")[
        "cause_id"
    ].tolist()

    # Check that we have at least one child cause in affected_paf_df.
    # Some parent causes won't have all child causes and that's allowed but we can't do
    # any aggregation with 0 child causes
    affected_paf_df = paf_df.query(f"cause_id.isin({child_ids})")
    if affected_paf_df.empty:
        raise RuntimeError(
            "Expected at least one child cause to be present in input child cause PAFs "
            f"in order to aggregate to parent cause {parent_id}. Child cause IDs: {child_ids}"
        )

    affected_paf_df = _drop_excess_ages_and_sexes_in_pafs(
        affected_paf_df, rr_max_df, parent_id
    )
    unaffected_paf_df = _filter_unaffected_pafs(paf_df, rr_max_df, child_ids)

    # Pull cause burden and drop year_id as its no longer relevant
    cause_burden = _pull_cause_burden(
        cause_ids=child_ids,
        location_id=location_id,
        year_id=year_id,
        release_id=release_id,
        n_draws=n_draws,
        draw_cols=draw_cols,
        codcorrect_version_id=codcorrect_version_id,
        como_version_id=como_version_id,
        population=population,
    ).drop(columns="year_id")

    # Reshape pafs and cause burden long
    paf_df_long = affected_paf_df.melt(
        id_vars=PAF_INDEX_COLS, value_vars=draw_cols, var_name="draw", value_name="paf"
    )
    cause_burden_long = cause_burden.melt(
        id_vars=["location_id", "age_group_id", "sex_id", "cause_id"],
        value_vars=draw_cols,
        var_name="draw",
        value_name="burden",
    )
    cause_burden_long["total_burden"] = cause_burden_long.groupby(
        ["location_id", "age_group_id", "sex_id", "draw"]
    )["burden"].transform("sum")
    cause_burden_long["weighted_burden"] = (
        cause_burden_long["burden"] / cause_burden_long["total_burden"]
    )

    # Aggregate PAFs to parent using ratio of child cause burden / sum of child burden
    parent_paf_df_long = (
        pd.merge(
            paf_df_long,
            cause_burden_long,
            on=["location_id", "age_group_id", "sex_id", "cause_id", "draw"],
            how="left",  # keep all PAF rows
        )
        # Missingness in get_draws indicates 'implied 0s' from age/sex restrictions
        .fillna(0)
        .groupby(constants.DEMOGRAPHIC_COLS + ["draw"], group_keys=False)
        .apply(lambda row: (row["paf"] * row["weighted_burden"]).sum())
        .reset_index()
        .rename(columns={0: "paf"})  # rename resulting unnamed column
        .assign(cause_id=parent_id)
    )

    # Reshape wide, add to unaffected PAFs and return
    parent_paf_df = pd.pivot(
        parent_paf_df_long, index=PAF_INDEX_COLS, columns="draw", values="paf"
    ).reset_index()
    return pd.concat([unaffected_paf_df, parent_paf_df]).reset_index(drop=True)


def _filter_unaffected_pafs(
    paf_df: pd.DataFrame, rr_max_df: pd.DataFrame, child_ids: List[int]
) -> pd.DataFrame:
    """Filter to PAFs unaffected by parent cause PAF aggregation.

    These PAFs do not go into parent cause PAF aggregation. They're kept for later SEV
    calculation.

    Rules for what PAF rows are returned:
        1) Cause is not a child cause (child_ids), OR
        2) PAF row has a match in RRmax estimates (cause/age group/sex)

    In general, we want to keep all PAFs for uninvolved causes and drop all PAFs for
    involved causes (case 1).

    However, in some cases, a child cause is used for parent cause PAF aggregation AND
    ALSO has its own RRmax estimates (case 2). Ex: for air pollution, LRI is a
    child cause for LBW/SG and also has RRmax estimates.

    We also allow for the case where child cause PAFs have different age/sex demographics
    used in parent cause PAF aggregation vs. the matching RRmax estimates (also case 2).
    Ex: for child and maternal malnutrition, LRI is a child cause for LBW/SG (using age group
    2, early neonatal). LRI has separate RRmax estimates that DON'T use age group 2.

    The dropped PAFs allow the PAF and RRmax indexes to match later on.
    """
    paf_df = paf_df.merge(
        rr_max_df[rr_max.INDEX_COLS], on=rr_max.INDEX_COLS, how="left", indicator=True
    )

    dropped_paf_df = paf_df.query(f"cause_id.isin({child_ids}) & _merge == 'left_only'")
    if not dropped_paf_df.empty:
        warnings.warn(
            "When filtering to PAFs unaffected by parent cause PAF aggregation, dropped "
            f"{len(dropped_paf_df)} PAF row(s) for child causes/age groups/sexes that do not "
            f"exist in RRmax estimates:\n{dropped_paf_df}"
        )

    return paf_df.query(f"~cause_id.isin({child_ids}) | _merge == 'both'").drop(
        columns="_merge"
    )


def _drop_excess_ages_and_sexes_in_pafs(
    paf_df: pd.DataFrame, rr_max_df: pd.DataFrame, parent_id: int
) -> pd.DataFrame:
    """Drop child cause PAF rows for excess age groups and sexes.

    We do this if child cause PAFs have more age groups/sexes than exist in parent cause
    RRmax.

    The dropped PAFs force the resulting parent cause PAF to have the same age groups and
    sexes that the RRmax estimates have. Thus, the PAF and RRmax indexes to match later on.
    """
    paf_df = rr_max_df.query(f"cause_id == {parent_id}")[["age_group_id", "sex_id"]].merge(
        paf_df, how="right", indicator=True
    )

    dropped_paf_df = paf_df.query("_merge == 'right_only'")
    if not dropped_paf_df.empty:
        warnings.warn(
            f"Dropping {len(dropped_paf_df)} PAF row(s) where there are child cause PAFs "
            f"for age groups/sexes not present in the parent cause's RRmax:\n{dropped_paf_df}"
        )

    return paf_df.query("_merge == 'both'").drop(columns="_merge")


def _pull_cause_burden(
    cause_ids: List[int],
    location_id: int,
    year_id: int,
    release_id: int,
    n_draws: int,
    draw_cols: List[str],
    codcorrect_version_id: int,
    como_version_id: Optional[int],
    population: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Pull cause burden for the given causes either as YLLs or DALYs.

    If como_version_id is None, returns YLLs. If como_version_id is given,
    returns DALYs as the sum of YLLs + YLDs.
    """
    # Pull YLLs from CodCorrect
    ylls = get_draws_api.get_draws(
        gbd_id_type="cause_id",
        gbd_id=cause_ids,
        source="codcorrect",
        location_id=location_id,
        year_id=year_id,
        measure_id=measures.YLL,
        version_id=codcorrect_version_id,
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
    )[PAF_INDEX_COLS + draw_cols]

    # If requested, pull YLDs from COMO converting to count-space and then calculate DALYs
    if como_version_id is not None:
        ylds = (
            get_draws_api.get_draws(
                gbd_id_type="cause_id",
                gbd_id=cause_ids,
                source="como",
                location_id=location_id,
                year_id=year_id,
                measure_id=measures.YLD,
                version_id=como_version_id,
                release_id=release_id,
                n_draws=n_draws,
                downsample=True,
            )[PAF_INDEX_COLS + draw_cols]
            .merge(
                population[constants.DEMOGRAPHIC_COLS + ["population"]],
                on=constants.DEMOGRAPHIC_COLS,
                how="left",
            )
            .set_index(PAF_INDEX_COLS)
        )
        ylds = ylds[draw_cols].multiply(ylds["population"], axis=0)

        # Check for NAs potentially introduced from population missing demographics
        if ylds.isna().any().any():
            raise RuntimeError(
                "NAs found in YLDs pulled from como_version_id="
                f"{como_version_id} and multiplied by population. Is population "
                "missing expected demographics?"
            )

        # DALYs = YLLs + YLDs
        # Fill any demographics missing from one side with 0s
        cause_burden = ylds.add(ylls.set_index(PAF_INDEX_COLS), fill_value=0.0).reset_index()
    else:
        cause_burden = ylls

    return cause_burden


def average_sevs_across_causes(sev_df: pd.DataFrame) -> pd.DataFrame:
    """Average SEVs across causes to get risk-specific SEV."""
    return (
        sev_df.drop(columns="cause_id")
        .groupby(constants.DEMOGRAPHIC_COLS)
        .mean()
        .reset_index()
    )


def calculate_sevs_for_pafs_of_one(
    rei_id: int,
    paf_of_one_df: pd.DataFrame,
    pafs_of_one: pd.DataFrame,
    location_id: int,
    year_ids: List[int],
    estimation_year_ids: List[int],
    release_id: int,
    me_ids: pd.DataFrame,
    n_draws: int,
    draw_cols: List[str],
    root_dir: str,
) -> pd.DataFrame:
    """Calculate SEVs for a small set of risk - causes with PAFs of 1 using risk prevalence.

    For iron deficiency, SEV is prevalence of moderate or severe anemia. For FPG and
    SBP, SEV is risk prevalence.

    Returns:
        DataFrame of SEVs with columns location_id, year_id, age_group_id, sex_id,
            cause_id, draw_0, ..., draw_n
    """
    if rei_id == constants.IRON_DEFICIENCY_REI_ID:
        prevalence = risk_prevalence.get_iron_deficiency_prevalance(
            location_id=location_id,
            year_ids=year_ids,
            estimation_year_ids=estimation_year_ids,
            release_id=release_id,
            me_ids=me_ids,
            n_draws=n_draws,
            draw_cols=draw_cols,
        )
    else:
        # FPG and SBP risk prevalence is calculated beforehand via edensity R code
        prevalence = io.read_risk_prevalence_draws(rei_id, location_id, root_dir)

    # Assumption check: if this function is called, the risk should have 1+ PAFs of 1 cause
    pafs_of_one_cause_ids = pafs_of_one.query(f"rei_id == {rei_id}")["cause_id"].tolist()
    if not pafs_of_one_cause_ids:
        raise RuntimeError(f"Expected at least one cause with PAFs of 1 for rei_id {rei_id}")

    return paf_of_one_df[PAF_INDEX_COLS].merge(prevalence, on=constants.DEMOGRAPHIC_COLS)
