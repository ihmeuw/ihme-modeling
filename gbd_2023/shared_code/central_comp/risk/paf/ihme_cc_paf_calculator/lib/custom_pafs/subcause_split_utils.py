from typing import List, Optional

import numpy as np
import pandas as pd

import db_queries
import ihme_dimensions
from gbd.constants import measures, release
from get_draws.api import get_draws

from ihme_cc_paf_calculator.lib import constants, logging_utils, machinery, math

logger = logging_utils.module_logger(__name__)


def get_subcause_burden(
    cause_id: List[int],
    location_id: int,
    codcorrect_version_id: Optional[int],
    como_version_id: Optional[int],
    n_draws: int,
    release_id: int,
    fatal_only: bool = False,
) -> pd.DataFrame:
    """
    Retrieve YLLs and optionally DALYs for a particular cause or set of causes. This will
    be used for estimating PAFs for CKD subcauses (and other custom risks).

    Pulls burden and for a single, arbitrary year as ratio of subcause burden is assumed not
    to vary much over time. Pulls population for the same year.

    Arguments:
        cause_id: list of causes to retrieve results for
        location_id: location to retrieve results for
        codcorrect_version_id: internal CodCorrect version to pull burden for.
            Expected to be None for USRE. Otherwise, must be provided.
        como_version_id: internal COMO version to pull burden for.
            Expected if fatal_only=False.
        n_draws: how many draws to downsample to
        release_id: release id used to pull population estimates and
            YLL/YLD data. Note that if release_id is USRE, we use a flat file
            rather than get_draws.
        fatal_only: if True, only pull fatal burden. If False, pull fatal and total burden

    Returns:
        dataframe of YLL and (optionally) DALY counts. Data are reshaped long and
        missing year, so columns are cause_id, location_id, sex_id,
        age_group_id, draw, measure_id, subcause_burden
    """
    arbitrary_year_id = machinery.get_arbitrary_year_id_for_subcause_splitting(release_id)

    # USRE has custom burden: they do not use central machinery
    if release_id == release.USRE:
        df = _get_subcause_burden_usre(
            cause_id=cause_id, n_draws=n_draws, fatal_only=fatal_only
        ).query("location_id == @location_id")
    else:
        logger.info(f"Pulling subcause burden for arbitrary year: {arbitrary_year_id}")
        df = _get_subcause_burden_get_draws(
            cause_id=cause_id,
            location_id=location_id,
            arbitrary_year_id=arbitrary_year_id,
            codcorrect_version_id=codcorrect_version_id,
            como_version_id=como_version_id,
            release_id=release_id,
            n_draws=n_draws,
            fatal_only=fatal_only,
        )

    logger.info(f"Pulling population for arbitrary year: {arbitrary_year_id}")
    pop_df = db_queries.get_population(
        location_id=location_id,
        year_id=arbitrary_year_id,
        sex_id="all",
        age_group_id="all",
        release_id=release_id,
    )

    return _compute_dalys(df, pop_df=pop_df, n_draws=n_draws, fatal_only=fatal_only)


def _get_subcause_burden_usre(
    cause_id: List[int], n_draws: int, fatal_only: bool = False
) -> pd.DataFrame:
    """
    When calculating PAFs for USRE team, we can't use codcorrect or como because we
    need special USRE locations. So instead we pull from a flat file that USRE team
    has prepared.
    """
    fpath = 
    df = pd.read_csv(fpath)
    df = df[df["cause_id"].isin(cause_id)]
    missing_causes = set(cause_id).difference(df["cause_id"].tolist())
    if missing_causes:
        raise RuntimeError(
            f"Asked for causes {cause_id} from USRE but missing {missing_causes}"
        )

    if fatal_only:
        df = df[df["measure_id"] == measures.YLL]

    draw_cols = [c for c in df if "draw" in c]
    if len(draw_cols) != n_draws:
        df = ihme_dimensions.dfutils.resample(df, n_draws=n_draws)
    return df


def _get_subcause_burden_get_draws(
    cause_id: List[int],
    location_id: int,
    arbitrary_year_id: int,
    codcorrect_version_id: int,
    como_version_id: Optional[int],
    release_id: int,
    n_draws: int,
    fatal_only: bool = False,
) -> pd.DataFrame:
    """Get YLLs and optionally YLDs for subcause burden for a single year.

    CodCorrect and COMO versions are decided upon launch. codcorrect_version_id cannot be
    None. If fatal_only=True, como_version_id must be None. If fatal_only=False,
    como_version_id cannot be None.
    """
    _validate_machinery_versions(codcorrect_version_id, como_version_id, fatal_only)

    ylls = get_draws(
        source="codcorrect",
        gbd_id_type="cause_id",
        gbd_id=cause_id,
        location_id=location_id,
        year_id=arbitrary_year_id,
        measure_id=measures.YLL,
        release_id=release_id,
        version_id=codcorrect_version_id,
        n_draws=n_draws,
        downsample=True,
    )
    if fatal_only:
        return ylls
    else:
        ylds = get_draws(
            source="como",
            gbd_id_type="cause_id",
            gbd_id=cause_id,
            location_id=location_id,
            year_id=arbitrary_year_id,
            measure_id=measures.YLD,
            release_id=release_id,
            version_id=como_version_id,
            n_draws=n_draws,
            downsample=True,
        )
        df = pd.concat([ylls, ylds]).drop(columns="version_id")
        return df


def _validate_machinery_versions(
    codcorrect_version_id: int, como_version_id: int, fatal_only: bool
) -> None:
    """Validate given machinery versions as an invariant check."""
    if codcorrect_version_id is None:
        raise RuntimeError(
            "Internal error: codcorrect_version_id should not be None at this point."
        )

    if fatal_only:
        if como_version_id is not None:
            raise RuntimeError(
                "Internal error: if pulling fatal-only burden, como_version_id must be None. "
                f"Received como_version_id={como_version_id}"
            )
    else:
        if como_version_id is None:
            raise RuntimeError(
                "Internal error: como_version_id should not be None at this point."
            )


def _compute_dalys(
    df: pd.DataFrame, pop_df: pd.DataFrame, n_draws: int, fatal_only: bool
) -> pd.DataFrame:
    """
    Given a dataframe of YLLs and YLDs, compute and append DALYs. Return
    YLLs and DALYs for use in ckd subcause splitting (and other custom risks).

    YLDs are assumed to be in rate space and YLLs in count space. Result is in
    count space.

    Note that there is no requirement for the year_id in pop_df to match
    the years in df. Returned data is not year specific. This isn't
    ideal because we would like the conversion to count space to be
    identical to what COMO computes. However, USRE data is not guaranteed
    to contain the same year as what COMO uses, so we have allow for the
    possibility that the years don't match.

    Arguments:
        df: dataframe of codcorrect YLLs and COMO YLDs. Note that YLDs are in
            rate space and will be converted to count space prior to DALY
            computation. Columns are age_group_id, sex_id, year_id, location_id,
            cause_id, metric_id, measure_id, draw_*
        pop_df: dataframe of populations used to convert YLDs to count space.
            Columns are age_group_id, sex_id, year_id, location_id, population
        n_draws: how many draws there are in df
        fatal_only: If True, that means df only contain YLLs (fatal burden) and no
            dalys should be calculated. Only reshaping occurs. If False, dalys are
            calculated and appended.

    Returns:
        dataframe of YLL and DALY counts. Data are reshaped long and
        missing year, so columns are cause_id, location_id, sex_id,
        age_group_id, draw, measure_id, subcause_burden

    """
    draw_map = {f"draw_{i}": i for i in range(n_draws)}
    df = df.rename(columns=draw_map)
    df = df.melt(
        id_vars=["cause_id", "measure_id", "location_id", "age_group_id", "sex_id"],
        value_vars=list(draw_map.values()),
        var_name="draw",
        value_name="subcause_burden",
    )
    df["draw"] = df["draw"].astype(int)

    if fatal_only:
        return df

    df = pd.merge(
        df,
        pop_df[["location_id", "sex_id", "age_group_id", "population"]],
        on=["location_id", "sex_id", "age_group_id"],
        how="left",
    )
    df.loc[df["measure_id"] == measures.YLD, "subcause_burden"] *= df["population"]
    df = df.drop(columns="population")

    df_dalys = (
        df.groupby(["cause_id", "location_id", "sex_id", "age_group_id", "draw"])
        .subcause_burden.sum()
        .reset_index()
        .assign(measure_id=measures.DALY)
    )
    df = pd.concat(
        [df_dalys, df[df["measure_id"] == measures.YLL]], axis=0, ignore_index=True
    )
    return df


def get_mediator_subcause_proportions(
    med_id: int,
    parent_cause_id: int,
    subcause_id: List[int],
    location_id: int,
    year_id: List[int],
    n_draws: int,
    release_id: int,
    cause_burden: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate subcause proportions for a mediator risk.

    We pull PAFs for all subcauses and the parent cause, then use subcause
    burden to calculate attributable burden and compute subcause proportions.

    Arguments:
        med_id: mediator risk id
        parent_cause_id: parent cause id
        subcause_id: list of subcause ids of parent cause
        location_id: location id
        year_id: year id
        n_draws: number of draws
        release_id: release id
        cause_burden: dataframe of subcause burden for all subcauses. Columns are
            location_id, age_group_id, sex_id, cause_id, draw, subcause_burden,
            parent_burden. Note that there is no measure_id column. Depending
            on parent cause, cause_burden is either DALYs or YLLs.

    Returns:
        dataframe with the following columns:
            location_id, age_group_id, sex_id, year_id, cause_id, morbidity, mortality,
            draw, med_prop
    """
    mediator_df = _get_mediator_pafs(
        med_id=med_id,
        parent_cause_id=parent_cause_id,
        subcause_id=subcause_id,
        location_id=location_id,
        year_id=year_id,
        n_draws=n_draws,
        release_id=release_id,
    )
    result_df = _compute_mediator_subcause_proportions(
        mediator_df=mediator_df, cause_burden=cause_burden, parent_cause_id=parent_cause_id
    )
    return result_df


def _compute_mediator_subcause_proportions(
    mediator_df: pd.DataFrame, cause_burden: pd.DataFrame, parent_cause_id: int
) -> pd.DataFrame:
    """
    Given PAFs and cause burden, compute subcause proportions (aka the fraction
    of attributable burden for each subcause over the parent cause attributable
    burden).

    Arguments:
        mediator_df: PAFs with columns cause_id, mortality, morbidity,
           location_id, year_id, age_group_id, sex_id, draw, med_paf
        cause_burden: dataframe of subcause burden for all subcauses. Columns are
            location_id, age_group_id, sex_id, cause_id, draw, subcause_burden,
            parent_burden. Note that there is no measure_id column. Depending
            on parent cause, cause_burden is either DALYs or YLLs.
        parent_cause_id: which cause id in cause_burden is the parent?

    Returns:
        dataframe with the following columns:
            location_id, age_group_id, sex_id, year_id, cause_id, morbidity,
            mortality, draw, med_prop
    """
    # first merge on cause burden for parent cause to pafs
    df = pd.merge(
        mediator_df[mediator_df["cause_id"] == parent_cause_id].drop(columns="cause_id"),
        cause_burden,
        on=["location_id", "age_group_id", "sex_id", "draw"],
        how="inner",
    )
    # calculate risk attributable burden for parent cause
    df = df.assign(med_parent_risk_attr=df["med_paf"] * df["parent_burden"]).drop(
        columns="med_paf"
    )
    id_cols = [
        "cause_id",
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "draw",
        "mortality",
        "morbidity",
    ]
    # merge on cause burden for subcauses
    df = df.merge(mediator_df, on=id_cols, how="inner")
    # calculate proportion of attributable burden split to each subcause
    df["med_prop"] = df["med_paf"] * df["subcause_burden"] / df["med_parent_risk_attr"]
    return df[id_cols + ["med_prop"]]


def _get_mediator_pafs(
    med_id: int,
    parent_cause_id: int,
    subcause_id: List[int],
    location_id: int,
    year_id: List[int],
    n_draws: int,
    release_id: int,
) -> pd.DataFrame:
    """
    Pull PAFs for a mediator risk and parent cause plus all subcauses. Reshape long
    and replace measure with mortality/morbidity.

    Returns:
        dataframe with the following columns:
            cause_id, mortality, morbidity, location_id, year_id, age_group_id,
            sex_id, draw, med_paf

    """
    df = get_draws(
        gbd_id_type=["rei_id"] + (["cause_id"] * (len(subcause_id) + 1)),
        gbd_id=[med_id, parent_cause_id] + subcause_id,
        location_id=location_id,
        year_id=year_id,
        n_draws=n_draws,
        downsample=True,
        release_id=release_id,
        source="paf",
    )
    is_nonfatal = df["measure_id"] == measures.YLD
    df["mortality"] = np.where(is_nonfatal, 0, 1)
    df["morbidity"] = np.where(is_nonfatal, 1, 0)
    df = df.drop("measure_id", axis=1)
    draw_map = {f"draw_{i}": i for i in range(n_draws)}
    df = df.rename(columns=draw_map)
    df = df.melt(
        id_vars=[
            "cause_id",
            "mortality",
            "morbidity",
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
        ],
        value_vars=list(draw_map.values()),
        var_name="draw",
        value_name="med_paf",
    )
    return df


def get_subcauses(
    cause_metadata: pd.DataFrame, parent_cause_id: int, rei_id: int
) -> List[int]:
    """Find most-detailed causes under parent, with special logic for CKD."""
    subcauses = cause_metadata[
        (
            cause_metadata["path_to_top_parent"].str.contains(f",{parent_cause_id},")
            | (cause_metadata["parent_id"] == parent_cause_id)
        )
        & (cause_metadata["most_detailed"] == 1)
    ].cause_id.tolist()

    # Drop excluded subcauses, if any.
    if parent_cause_id in constants.CAUSES_WITH_SUBCAUSE_EXCLUSIONS:
        subcauses = [
            subcause
            for subcause in subcauses
            if subcause
            not in constants.CAUSES_WITH_SUBCAUSE_EXCLUSIONS[parent_cause_id][rei_id]
        ]

    return subcauses


def get_cause_and_subcause_burden(
    parent_cause_id: int,
    subcause_id: List[int],
    codcorrect_version_id: Optional[int],
    como_version_id: Optional[int],
    release_id: int,
    location_id: int,
    n_draws: int,
) -> pd.DataFrame:
    """
    Given a parent cause and list of subcauses, pull subcause-specific burden,
    as well as parent cause burden.

    This function will either use DALYs or YLLs depending on parent cause. For
    LBWSG we use YLLs with all-cause as our parent cause, otherwise we use
    DALYs with our parent cause burden as sum of subcause burden.

    Returns:
        dataframe with the following columns:
            cause_id, location_id, sex_id, age_group_id, draw, subcause_burden, parent_burden
    """
    if parent_cause_id == constants.LBWSG_CAUSE_ID:
        # pull subcause YLLs from CoDCorrect for a single year and
        # all cause burden as parent cause burden
        cause_burden = get_subcause_burden(
            cause_id=[constants.ALL_CAUSE_ID] + subcause_id,
            location_id=location_id,
            codcorrect_version_id=codcorrect_version_id,
            como_version_id=como_version_id,
            n_draws=n_draws,
            release_id=release_id,
            fatal_only=True,
        )
        subcause_burden = cause_burden[cause_burden["cause_id"] != constants.ALL_CAUSE_ID]
        all_cause_burden = (
            cause_burden[cause_burden["cause_id"] == constants.ALL_CAUSE_ID]
            .rename(columns={"subcause_burden": "parent_burden"})
            .drop(columns="cause_id")
        )
        df = pd.merge(subcause_burden, all_cause_burden, how="left").drop(
            columns="measure_id"
        )
    else:
        # pull subcause DALYs for a single year and
        # calculate parent cause burden via sum of subcauses
        cause_burden = get_subcause_burden(
            cause_id=subcause_id,
            location_id=location_id,
            codcorrect_version_id=codcorrect_version_id,
            como_version_id=como_version_id,
            n_draws=n_draws,
            release_id=release_id,
        )
        cause_burden = cause_burden[cause_burden["measure_id"] == measures.DALY]
        cause_burden["parent_burden"] = cause_burden.groupby(
            ["location_id", "sex_id", "age_group_id", "draw"]
        )["subcause_burden"].transform("sum")
        df = cause_burden.drop(columns="measure_id")
    return df


def _calculate_parent_risk_attributable_burden(
    cause_burden: pd.DataFrame, parent_cause_pafs: pd.DataFrame
) -> pd.DataFrame:
    """
    This function calculates total parent cause risk-attributable burden.
    Because we're joining year-agnostic cause_burden to year-specific
    parent cause PAFs, the output data becomes year specific.

    Arguments:
        cause_burden: a dataframe with columns
            cause_id, location_id, sex_id, age_group_id, draw, subcause_burden,
            parent_burden
        parent_cause_pafs: a dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id

    Returns:
        A dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id, cause_id, subcause_burden,
            parent_burden, parent_risk_attr
    """
    df = pd.merge(
        parent_cause_pafs,
        cause_burden,
        on=["location_id", "age_group_id", "sex_id", "draw"],
        how="inner",
    )
    df["parent_risk_attr"] = df["parent_paf"] * df["parent_burden"]
    return df


def _split_parent_paf_using_subcause_proportions_from_mediator(
    cause_burden: pd.DataFrame, med_props: pd.DataFrame
) -> pd.DataFrame:
    """
    For risks with mediators, we split parent burden by using the mediating
    risk subcause proportions.

    We first compute each subcause's risk attributable burden by multiplying
    the proportion by the parent risk attributable burden. Then we calculate
    the subcause paf by dividing the subcause risk attributable burden by
    subcause burden.

        subcause_risk_attr = med_prop * parent_risk_attr
        paf = subcause_risk_attr / subcause_burden

    Arguments:
        cause_burden: dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id, cause_id, subcause_burden,
            parent_burden, parent_risk_attr
        med_props: dataframe with columns
            location_id, age_group_id, sex_id, year_id, cause_id, morbidity,
            mortality, draw, med_prop

    Returns:
        dataframe with columns
            cause_id, location_id, year_id, age_group_id, sex_id, draw,
            mortality, morbidity, paf

    NOTE: It's possible for this function to return pafs > 1. This
    shouldn't happen in practice, but there is no mechanism to prevent it.
    We are relying on the input data to be consistent enough to prevent this.
    If data_utils.enforce_paf_in_range warns about pafs > 1, we should look here.
    """
    # expand cause_burden mortality/morbidity columns to match med_props
    expanded_cause_burden = math.expand_morbidity_mortality(cause_burden)
    df = pd.merge(
        expanded_cause_burden,
        med_props,
        on=[
            "med_id",
            "cause_id",
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "draw",
            "mortality",
            "morbidity",
        ],
        how="inner",
    )
    df = df.eval(
        """
        subcause_risk_attr = med_prop * parent_risk_attr
        paf = subcause_risk_attr / subcause_burden
        """
    )
    return df[
        [
            "cause_id",
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "draw",
            "mortality",
            "morbidity",
            "paf",
        ]
    ]


def _calculate_pafs_for_fully_attributable_subcauses(
    cause_burden: pd.DataFrame, parent_cause_id: int, rei_id: int
) -> pd.DataFrame:
    """
    Some risk/cause pairs are 100% attributable. We must allocate burden to
    those pairs first, before allocating the rest to <100% attributable
    risk/cause pairs.

    If parent attributable burden is less than the sum of fully attributable
    subcauses, we allocate as close as 100% as we can and allocate 0% to any
    remaining partially attributable subcauses.

    Besides demographic columns, the important columns in cause_burden are:
        subcause_burden: burden of each subcause
        parent_burden: burden of parent cause
        parent_risk_attr: risk attributable burden of parent cause

    The returned dataframe has the same columns as cause_burden, plus the
    following:
        full_parent_burden: parent cause's burden, but only using subcauses
            that are fully attributable to the risk. AKA the sum of the
            of subcause burden for each demographic, excluding partially
            attributable causes.
        subcause_risk_attr: risk attributable burden of each fully-attributable
            subcause
        subcause_paf: paf of each fully-attributable subcause

    The number of rows will be smaller than subcause_burden, because any
    partially attributable causes have been removed.
    """
    fully_attributable_subcauses = constants.FULLY_ATTRIBUTABLE_SUBCAUSES[
        (parent_cause_id, rei_id)
    ]

    # keep subcauses that are directly correlated with risk exposure
    fully_attrib = cause_burden[cause_burden["cause_id"].isin(fully_attributable_subcauses)]

    # sum cause burden across included subcauses
    fully_attrib["full_parent_burden"] = fully_attrib.groupby(
        ["location_id", "year_id", "sex_id", "age_group_id", "draw", "mortality", "morbidity"]
    )["subcause_burden"].transform("sum")

    # attribute as close to 100% of the subcause burden as present. if there's not
    # enough, proportionally split across subcause(s) if multiple.
    non_negative_burden = fully_attrib["parent_risk_attr"] >= 0
    full_burden_exceeds_envelope = (
        fully_attrib["full_parent_burden"] > fully_attrib["parent_risk_attr"]
    )

    # Proportionally allocate subcause burden for rows without enough parent risk
    # attributable burden to allocate everything
    must_proportionally_allocate = non_negative_burden & full_burden_exceeds_envelope
    fully_attrib.loc[must_proportionally_allocate, "subcause_risk_attr"] = (
        fully_attrib["subcause_burden"] / fully_attrib["full_parent_burden"]
    ) * fully_attrib["parent_risk_attr"]

    # Attribute 100% subcause burden for rows with enough parent risk attributable burden
    can_allocate_all = non_negative_burden & ~full_burden_exceeds_envelope
    fully_attrib.loc[can_allocate_all, "subcause_risk_attr"] = fully_attrib["subcause_burden"]

    # if parent PAF is negative, don't allow these PAFs to go below zero
    fully_attrib.loc[~non_negative_burden, "subcause_risk_attr"] = 0

    # back-calculate sub-cause PAF
    fully_attrib["subcause_paf"] = (
        fully_attrib["subcause_risk_attr"] / fully_attrib["subcause_burden"]
    )
    return fully_attrib


def _calculate_pafs_for_partially_attributable_subcauses(
    cause_burden: pd.DataFrame, fully_attrib: pd.DataFrame
) -> pd.DataFrame:
    """
    This function calculates pafs for partially attributable subcauses. E.g.,
    after fully-attributable subcause burden has been allocated.

    Arguments:
        cause_burden: dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id, cause_id, subcause_burden,
            parent_burden, parent_risk_attr
        fully_attrib: dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id, cause_id, subcause_burden,
            parent_burden, full_parent_burden

    Returns:
        dataframe with columns
            location_id, year_id, age_group_id, sex_id, draw, mortality,
            morbidity, parent_paf, med_id, cause_id, subcause_burden,
            parent_burden, parent_risk_attr, full_parent_burden,
            full_risk_attr, subcause_risk_attr, subcause_paf

            The new columns are:
                full_risk_attr: sum of attributable burden for fully
                    attributable subcauses
                subcause_risk_attr: risk attributable burden of each
                    partially attributable subcause, after fully attributable
                    subcause burden has been allocated.
                subcause_paf: paf of each partially attributable subcause
    """
    partially_attrib = cause_burden[
        ~cause_burden["cause_id"].isin(fully_attrib["cause_id"].unique().tolist())
    ].copy()

    # sum subcause_risk_attr across included subcauses and merge with cause burden
    fully_attrib_sum = (
        fully_attrib.groupby(
            [
                "location_id",
                "year_id",
                "sex_id",
                "age_group_id",
                "draw",
                "mortality",
                "morbidity",
                "full_parent_burden",  # this is constant, we just want to keep it
            ]
        )["subcause_risk_attr"]
        .sum()
        .reset_index()
        .rename(columns={"subcause_risk_attr": "full_risk_attr"})
    )
    partially_attrib = pd.merge(
        partially_attrib,
        fully_attrib_sum,
        on=[
            "location_id",
            "year_id",
            "sex_id",
            "age_group_id",
            "draw",
            "mortality",
            "morbidity",
        ],
        how="left",
    )

    null_full_parent_burden = partially_attrib["full_parent_burden"].isna()
    partially_attrib.loc[
        null_full_parent_burden, ["full_parent_burden", "full_risk_attr"]
    ] = 0

    # subtract portion of attributable cause burden we've already assigned
    partially_attrib["parent_risk_attr"] = partially_attrib["parent_risk_attr"].round(
        12
    ) - partially_attrib["full_risk_attr"].round(12)

    # calculate remaining subcause PAFs from remaining burden
    partially_attrib.eval(
        """
        subcause_risk_attr = (subcause_burden / (parent_burden - full_parent_burden)) * parent_risk_attr
        subcause_paf = subcause_risk_attr / subcause_burden
        """,  # noqa
        inplace=True,
    )

    return partially_attrib


def split_parent_cause(
    rei_id: int,
    med_id: List[int],
    parent_cause_id: int,
    parent_paf_df: pd.DataFrame,
    cause_burden: pd.DataFrame,
    mediator_proportions: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Given PAF draws for a risk and aggregate cause, split the PAFs to some
    subset of subcauses such that they still aggregate to the parent cause and
    respect any risk - cause relationships that may be 100% attributable.

    This function is called in 3 scenarios:
         1) any risk with CKD as a cause
           In this case, either
              1) the cause is not mediated (rei_id == [med_id]) as with reis
                FPG and SBP (105, 107)
              2) the cause is mediated through another risk(s) as with
                rei_id 370 (high bmi) and med_id = [105, 107]
        2) nutrition_lbw_preterm. This is called 3 times, 1 for each type of
            parent paf (preterm, lbw, joint preterm_lbw). In all 3 cases parent
            cause is 1061 (lbwsga cause)
        3) custom air_pmhap. In this case the parent cause is 1061 (lbwsga cause)
            and it is mediated through nutrition_lbw_preterm (med_id = [339])

    Args:
        rei_id: rei ID of the risk to split PAFs.
        med_id: list of rei IDs of the risk that the cause is mediated through.
            May be the same as rei_id (in that case there is no mediation).
        parent_cause_id: cause ID of the parent cause present in PAFs.
        parent_paf_df: DataFrame of PAF draws for the risk and parent cause.
            Columns are cause_id, location_id, year_id, age_group_id, sex_id,
            draw, morbidty, mortality, paf, and (optional) med_id.
        cause_burden: dataframe with the following columns
            cause_id, location_id, sex_id, age_group_id, draw, subcause_burden, parent_burden
        mediator_proportions: dataframe with the following columns
            location_id, age_group_id, sex_id, year_id, cause_id, morbidity,
            mortality, draw, med_prop

    Returns:
        DataFrame with columns location_id, year_id, age_group_id, sex_id,
            cause_id, draw, mortality, morbidity, paf
    """
    # The parent PAF might not have any mediators, which means parent_paf_df is
    # missing med_id column. In that case we assume/require med_id is a list of
    # length 1
    if "med_id" not in parent_paf_df.columns:
        if len(med_id) != 1:
            raise ValueError(
                "Parent PAFs are missing med_id but have more than one med_id "
                f"to assign to data: {med_id}"
            )
        parent_paf_df["med_id"] = med_id[0]

    parent_paf_df = parent_paf_df.rename(columns={"paf": "parent_paf"}).drop(
        columns="cause_id"
    )
    mediation_applies = mediator_proportions is not None

    cause_burden = _calculate_parent_risk_attributable_burden(cause_burden, parent_paf_df)

    if mediation_applies:
        return _split_parent_paf_using_subcause_proportions_from_mediator(
            cause_burden, mediator_proportions
        )

    # if mediation doesn't apply, then first we assign parent cause burden
    # to fully attributable risk/cause pairs. The leftover burden is then
    # proportionally assigned to risk/cause pairs without 100% paf.
    fully_attrib = _calculate_pafs_for_fully_attributable_subcauses(
        cause_burden, parent_cause_id, rei_id
    )
    partially_attrib = _calculate_pafs_for_partially_attributable_subcauses(
        cause_burden, fully_attrib
    )

    results = pd.concat([fully_attrib, partially_attrib], axis=0, ignore_index=True)

    _sanity_check(rei_id, results, cause_burden)

    results = results[
        [
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "cause_id",
            "draw",
            "mortality",
            "morbidity",
            "subcause_paf",
        ]
    ].rename(columns={"subcause_paf": "paf"})
    return results


def _sanity_check(rei_id: int, results: pd.DataFrame, cause_burden: pd.DataFrame) -> None:
    """
    subcause risk attributable burden should sum to parent cause risk
    attributable burden except in the case of LBW/SG where the parent burden is actually
    all-cause.

    NOTE: child and parent burden are not guaranteed to be consistent if draws are
    downsampled.
    """
    subcause_sums = (
        results.groupby(
            [
                "location_id",
                "year_id",
                "age_group_id",
                "sex_id",
                "draw",
                "mortality",
                "morbidity",
            ]
        )["subcause_risk_attr"]
        .sum()
        .reset_index()
    )
    subcause_sums = subcause_sums.merge(
        cause_burden[
            [
                "location_id",
                "year_id",
                "age_group_id",
                "sex_id",
                "draw",
                "mortality",
                "morbidity",
                "parent_risk_attr",
            ]
            # merge on parent risk attr burden (these are identical by subcause
            # so we drop duplicates)
        ].drop_duplicates()
    )

    error_msg = (
        "\n\nThis could be due to issues in central machinery results or not running "
        "with enough draws. Try increasing n_draws to 1000. Otherwise, submit a help "
        "desk ticket."
    )
    if rei_id == constants.LBWSGA_REI_ID:
        bad = subcause_sums.query("subcause_risk_attr > parent_risk_attr")
        if not bad.empty:
            raise RuntimeError(
                "Splitting parent cause PAFs failed sanity check -- subcause risk "
                "attributable burden is greater than all-cause risk attributable "
                "burden." + error_msg
            )
    else:
        bad = subcause_sums.query("abs(subcause_risk_attr - parent_risk_attr) > 0.0001")
        if not bad.empty:
            raise RuntimeError(
                "Splitting parent cause PAFs failed sanity check -- subcause risk "
                "attributable burden does not sum to parent cause risk attributable "
                "burden." + error_msg
            )


def split_and_append_subcauses(
    settings: constants.PafCalculatorSettings,
    paf_df: pd.DataFrame,
    mediator_rei_id: int,
    cause_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Given a dataframe of PAFs for a parent cause, produce and append PAFs for the
    subcauses. This is a convenience function that puts together all the neccesary
    functions for subcause splitting along with some dataframe formatting.

    Args:
        settings: global parameters for this PAF calculator run
        paf_df: dataframe of PAFs for a parent cause and single mediator (if 2-stage)
            with columns
            rei_id/cause_id/location_id/year_id/age_group_id/sex_id/morbidity
            /mortality/Optional[med_id]/draw_*
        mediator_rei_id: the mediator risk (if 2-stage) or the distal risk otherwise
        cause_metadata: the computation cause hierarchy from db_queries, used for

    Returns a dataframe containing all the original PAFs for the parent cause plus
        the PAFs for the subcauses. Contains columns
        rei_id/cause_id/location_id/year_id/age_group_id/sex_id/morbidity
        /mortality/Optional[med_id]/draw_*

    """
    # Get DALY burden for the parent cause and subcauses
    cause_id = paf_df.loc[0, "cause_id"]
    location_id = paf_df.loc[0, "location_id"]
    subcause_id = get_subcauses(cause_metadata, cause_id, settings.rei_id)
    cause_burden = get_cause_and_subcause_burden(
        parent_cause_id=cause_id,
        subcause_id=subcause_id,
        codcorrect_version_id=settings.codcorrect_version_id,
        como_version_id=settings.como_version_id,
        release_id=settings.release_id,
        location_id=location_id,
        n_draws=settings.n_draws,
    )

    # if risk-cause is mediated, find the subcause proportions from the mediator risk
    mediator_proportions = None
    if mediator_rei_id != settings.rei_id:
        mediator_proportions = get_mediator_subcause_proportions(
            med_id=mediator_rei_id,
            parent_cause_id=cause_id,
            subcause_id=subcause_id,
            location_id=location_id,
            year_id=paf_df["year_id"].unique().tolist(),
            n_draws=settings.n_draws,
            release_id=settings.release_id,
            cause_burden=cause_burden,
        ).assign(med_id=mediator_rei_id)

    # Transform long by draw. We don't need the rei_id column
    paf_index_cols = [
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "cause_id",
        "morbidity",
        "mortality",
    ]
    draw_map = {f"draw_{i}": i for i in range(settings.n_draws)}
    paf_df_long = paf_df.rename(columns=draw_map).melt(
        id_vars=paf_index_cols,
        value_vars=list(draw_map.values()),
        var_name="draw",
        value_name="paf",
    )

    # Split parent cause to subcauses
    subcause_df = split_parent_cause(
        rei_id=settings.rei_id,
        med_id=[mediator_rei_id],
        parent_cause_id=cause_id,
        parent_paf_df=paf_df_long,
        cause_burden=cause_burden,
        mediator_proportions=mediator_proportions,
    )

    # Pivot back to wide by draw
    subcause_df = (
        subcause_df.pivot(index=paf_index_cols, columns="draw", values="paf")
        .reset_index()
        .rename(columns={n: f"draw_{n}" for n in range(settings.n_draws)})
        .assign(rei_id=settings.rei_id)
    )

    # Return PAFs for both the parent cause and the subcauses
    return pd.concat([paf_df, subcause_df])
