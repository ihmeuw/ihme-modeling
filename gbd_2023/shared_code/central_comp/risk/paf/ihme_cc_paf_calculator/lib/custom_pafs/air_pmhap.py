import pathlib
from typing import List, Optional

import pandas as pd

import ihme_cc_risk_utils

from ihme_cc_paf_calculator.lib import constants, io_utils, math
from ihme_cc_paf_calculator.lib.custom_pafs import lbwsg, subcause_split_utils


def _calculate_PAF_for_aggregate_cause(
    raw_df: pd.DataFrame, rr_metadata: pd.DataFrame, full_term: List[str]
) -> pd.DataFrame:
    """
    Calculate air_pmhap pafs with aggregate LBWSGA outcome for a given input dataset.

    The input data is a combination of the following datasets:
        LBWSGA categorical exposures (column lbwsga)
        shifted LBWSGA categorical exposures (column lbwsga_shift)
        Relative Risks for LBWSGA (column rr)
        Week of birth and weight in grams at birth
            column parameter corresponds to unique combinations of preterm
            (week of birth) and lbw (weight in grams at birth)

    Prior to calculating PAFs, the raw data undergoes the following transformations:
        1. scale exposures to sum to <=1
        2. Add residual category as TMREL

    For PAF calculation, we use standard categorical RR weighting formula on
    both sets of exposures, and then compute the air_pmhap PAF as the scaled
    difference between those 2 PAFs.

    PTB morbidity PAFs are calculated separately and appended to the output.

    Arguments:
        raw_df: a dataframe of input data with the following columns:
            age_group_id,sex_id,parameter,draw,location_id,year_id,lbwsga,lbwsga_shift,
            cause_id,mortality,morbidity,rr,preterm,lbw
        rr_metadata: a dataframe of info about RR models
        full_term: list of categories that correspond to full term birth. Used
            in calculating PTB morbidity PAFs.
    """
    df = raw_df.copy()
    # we drop residual category because we're going to recalculate it after rescaling exposures
    residual_categ = lbwsg.PARAMETER_MAP.query(
        f"preterm == {lbwsg.LBWSGA_SG_TMREL} & lbw == {lbwsg.LBWSGA_LBW_TMREL}"
    )["parameter"].unique()
    df = df[~df["parameter"].isin(residual_categ)]

    # find exposure sums so we can scale exposures to sum to <=1
    group_cols = [
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "cause_id",
        "mortality",
        "morbidity",
        "draw",
    ]
    total = df.groupby(group_cols).agg({"lbwsga": "sum", "lbwsga_shift": "sum"}).reset_index()
    df = pd.merge(df, total, on=group_cols, suffixes=("", "_total"))
    df.loc[df["lbwsga_shift_total"] > 1, "lbwsga_shift"] = (
        df["lbwsga_shift"] / df["lbwsga_shift_total"]
    )
    df.loc[df["lbwsga_total"] > 1, "lbwsga"] = df["lbwsga"] / df["lbwsga_total"]
    # NOTE: this extra scaling seems incorrect. Leaving it in to match the
    # original R code, but we should raise this with research team to see if
    # its truly necessary/correct.
    df.loc[df["lbwsga_shift_total"] > 1, "lbwsga_shift"] = (
        df["lbwsga_shift"] / df["lbwsga_shift_total"]
    )
    df.drop(["lbwsga_total", "lbwsga_shift_total"], axis=1, inplace=True)

    # Append rows for residual category
    residual_rows = (
        df.groupby(group_cols)
        .apply(
            lambda x: pd.Series(
                {
                    "rr": 1,
                    "lbwsga": 1 - x["lbwsga"].sum(),
                    "lbwsga_shift": 1 - x["lbwsga_shift"].sum(),
                    "parameter": residual_categ[0],
                }
            )
        )
        .reset_index()
    )
    df = pd.concat([df, residual_rows], ignore_index=True)
    df["tmrel"] = (df["parameter"] == residual_categ[0]).astype(int)

    df = df[group_cols + ["tmrel", "parameter", "rr", "lbwsga", "lbwsga_shift"]]

    # Now we calculate the PAF

    # First, calculate morbidity Neonatal preterm birth PAF (We'll append later)
    ptb = _calculate_ptb(df, rr_metadata, full_term)

    # Then calculate mortality PAF for aggregate outcome
    # mult exp * RR across BW/GA categories for normal and shifted
    df = (
        df.groupby(group_cols)
        .apply(
            lambda x: pd.Series(
                {
                    "lbwsga": (x["lbwsga"] * x["rr"]).sum(),
                    "lbwsga_shift": (x["lbwsga_shift"] * x["rr"]).sum(),
                }
            )
        )
        .reset_index()
    )
    df["paf"] = (df["lbwsga"] - df["lbwsga_shift"]) / df["lbwsga"]

    return pd.concat([df, ptb], ignore_index=True)


def _split_and_append_lbwsgs_child_pafs(
    df: pd.DataFrame,
    rei_id: int,
    codcorrect_version_id: int,
    como_version_id: Optional[int],
    release_id: int,
    cause_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """
    After computing PAFs for LBWSGA aggregate outcomes, we compute subcause
    PAFs using subcause splitting method.
    """
    subcause_id = subcause_split_utils.get_subcauses(
        cause_metadata=cause_metadata, parent_cause_id=constants.LBWSG_CAUSE_ID, rei_id=rei_id
    )
    cause_burden = subcause_split_utils.get_cause_and_subcause_burden(
        parent_cause_id=constants.LBWSG_CAUSE_ID,
        subcause_id=subcause_id,
        codcorrect_version_id=codcorrect_version_id,
        como_version_id=como_version_id,
        location_id=df.loc[0, "location_id"],
        n_draws=df["draw"].max() + 1,
        release_id=release_id,
    )
    mediator_proportions = subcause_split_utils.get_mediator_subcause_proportions(
        med_id=constants.LBWSGA_REI_ID,
        parent_cause_id=constants.LBWSG_CAUSE_ID,
        subcause_id=subcause_id,
        location_id=df.loc[0, "location_id"],
        year_id=df["year_id"].unique().tolist(),
        n_draws=df["draw"].max() + 1,
        release_id=release_id,
        cause_burden=cause_burden,
    ).assign(med_id=constants.LBWSGA_REI_ID)
    lbwsg_outcome_child_pafs = subcause_split_utils.split_parent_cause(
        rei_id=rei_id,
        med_id=[constants.LBWSGA_REI_ID],
        parent_cause_id=constants.LBWSG_CAUSE_ID,
        parent_paf_df=df[df["cause_id"] == constants.LBWSG_CAUSE_ID],
        cause_burden=cause_burden,
        mediator_proportions=mediator_proportions,
    )
    final_df = pd.concat([df, lbwsg_outcome_child_pafs], ignore_index=True)
    return final_df


def _calculate_ptb(
    df: pd.DataFrame, rr_metadata: pd.DataFrame, full_term: List[str]
) -> pd.DataFrame:
    if constants.LBWSG_CAUSE_ID in rr_metadata["cause_id"].values:
        ptb = df[df["cause_id"] == constants.LBWSG_CAUSE_ID].copy()
        ptb["cause_id"] = constants.PTB_CAUSE_ID
    else:
        raise ValueError(
            "LBW/SG outcomes (cause ID 1061) not found in LBW/SG RRs."
            "We expect/require that cause in RR data. Unable to calculate "
            "mediated PM 2.5 -Neonatal preterm PAF."
        )

    # ptb_cat defines which babies are full/preterm
    ptb = ptb.assign(mortality=0, morbidity=1, ptb_cat=~ptb["parameter"].isin(full_term))

    # sum proportions over categories to get the total % of babies who were preterm
    group_cols = [
        col
        for col in ptb.columns
        if col not in ["parameter", "rr", "lbwsga", "lbwsga_shift", "tmrel"]
    ]
    ptb = ptb.groupby(group_cols)[["lbwsga", "lbwsga_shift"]].sum().reset_index()

    # calculate the PAF by taking 1 - the CF proportion of ptb/true proportion of ptb
    # rational here is that we want to estimate what proportion of ptb is
    # attributable to air pollution if there had been no air pollution we would
    # have seen the CF proportion of ptb. By taking 1 - the quotient, we get
    # the fraction of LBW babies that could have been avoided.
    ptb = ptb[ptb["ptb_cat"]]
    ptb["paf"] = 1 - ptb["lbwsga_shift"] / ptb["lbwsga"]
    return ptb


def _gather_data(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> pd.DataFrame:
    """
    Gather input data prior to performing air_pmhap paf calculation.

    The inputs to gather are:
        1) exposures for LBW/SG and shifted LBW/SG (pre-cached)
        2) RRs for LBW/SG (retrieved at runtime)

    The 3 datasets are merged together and returned. Final columns are
        location_id,year_id,age_group_id,sex_id,parameter,lbwsga,lbwsga_shift,
        rr,tmrel
    """
    exposure_df = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id
    )

    draw_map = {f"draw_{i}": i for i in range(settings.n_draws)}
    # we omit location for rr because 1) it could be global and 2) we'll use
    # the exposure location
    id_vars = [
        "cause_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "parameter",
        "mortality",
        "morbidity",
    ]
    rr_df = (
        ihme_cc_risk_utils.get_rr_draws(
            rei_id=constants.LBWSGA_REI_ID,
            release_id=settings.release_id,
            n_draws=settings.n_draws,
            cause_id=None,
            year_id=settings.year_id,
            location_id=location_id,
        )
        .rename(columns=draw_map)
        .melt(
            id_vars=id_vars,
            value_vars=list(draw_map.values()),
            var_name="draw",
            value_name="rr",
        )
    )

    rr_merge_cols = ["age_group_id", "sex_id", "parameter", "draw"]
    if "year_id" in rr_df.columns:
        rr_merge_cols.append("year_id")
    df = exposure_df.merge(rr_df, on=rr_merge_cols).merge(
        lbwsg.PARAMETER_MAP, on=["parameter"]
    )
    return df


def get_exposure_draws(
    release_id: int, location_id: int, year_id: List[int], n_draws: int
) -> pd.DataFrame:
    """
    For air_pmhap, we gather lbwsga and lbwsga_shift exposures and combine
    into one dataset.
    """
    draw_map = {f"draw_{i}": i for i in range(n_draws)}
    id_vars = ["location_id", "year_id", "age_group_id", "sex_id", "parameter"]
    lbwsga_exposure = (
        ihme_cc_risk_utils.get_exposure_draws(
            rei_id=constants.LBWSGA_REI_ID,
            release_id=release_id,
            location_id=location_id,
            year_id=year_id,
            n_draws=n_draws,
        )
        .rename(columns=draw_map)
        .melt(
            id_vars=id_vars,
            value_vars=list(draw_map.values()),
            var_name="draw",
            value_name="lbwsga",
        )
    )
    shifted_lbwsga_exposure = (
        ihme_cc_risk_utils.get_exposure_draws(
            rei_id=constants.AIR_PMHAP_REI_ID,
            release_id=release_id,
            location_id=location_id,
            year_id=year_id,
            n_draws=n_draws,
        )
        .rename(columns=draw_map)
        .melt(
            id_vars=id_vars,
            value_vars=list(draw_map.values()),
            var_name="draw",
            value_name="lbwsga_shift",
        )
    )
    df = pd.merge(lbwsga_exposure, shifted_lbwsga_exposure, on=id_vars + ["draw"])
    return df


def calculate_air_pmhap_paf(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> None:
    """
    The PAFs for air particulate matter and household air pollution are
    calculated with a custom process.

    There are 3 main steps:
      1) Neonatal Preterm (PTB) morbidity PAFs are computed
      2) Mortality PAFs are calculated for the aggregate Low Birth Weight and Short
        Gestation (LBWSGA) cause
      3) Mortality PAFs for LBWSGA subcauses are computed via subcause splitting
    """
    if settings.rei_id != constants.AIR_PMHAP_REI_ID:
        raise ValueError(
            f"Expected rei_id {constants.AIR_PMHAP_REI_ID}, got {settings.rei_id}"
        )

    cause_metadata = io_utils.get(root_dir, constants.CacheContents.CAUSE_METADATA)
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA)

    if cause_metadata.cause_set_id.iloc[0] != constants.LBWSG_CAUSE_SET_ID:
        raise ValueError(
            f"Expected cause_set_id {constants.LBWSG_CAUSE_SET_ID}, got "
            f"{cause_metadata.cause_set_id.iloc[0]}"
        )

    # gather inputs, calculate pafs for parent cause, and split into child causes
    raw_df = _gather_data(root_dir, location_id, settings)
    full_term = _get_fullterm_parameters(raw_df)
    result = _calculate_PAF_for_aggregate_cause(raw_df, rr_metadata, full_term)
    result_with_child_pafs = _split_and_append_lbwsgs_child_pafs(
        df=result,
        rei_id=constants.LBWSGA_REI_ID,
        codcorrect_version_id=settings.codcorrect_version_id,
        como_version_id=settings.como_version_id,
        release_id=settings.release_id,
        cause_metadata=cause_metadata,
    ).assign(rei_id=settings.rei_id)
    result_with_child_pafs = math.expand_morbidity_mortality(result_with_child_pafs)[
        [
            "rei_id",
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "cause_id",
            "mortality",
            "morbidity",
            "draw",
            "paf",
        ]
    ]

    # Transform wide by draw
    paf_wide = pd.pivot(
        result_with_child_pafs,
        index=[c for c in result_with_child_pafs if c not in ["draw", "paf"]],
        columns="draw",
    )
    paf_wide.columns = paf_wide.columns.droplevel(0)
    draw_map = {i: f"draw_{i}" for i in range(settings.n_draws)}
    paf_wide = paf_wide.rename(columns=draw_map).reset_index()

    # save out results for each cause
    io_utils.write_paf(df=paf_wide, root_dir=root_dir, location_id=location_id)


def _get_fullterm_parameters(df: pd.DataFrame) -> List[str]:
    """We need full term categories to calculate PAFs for PTB."""
    return df.loc[df["preterm"] >= 37, "parameter"].unique().tolist()
