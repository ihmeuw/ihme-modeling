"""Custom PAF calculation for injuries.

These PAFs are not directly modeled by researchers but calculated on the fly.
"""

from typing import List

import pandas as pd

import db_queries
from gbd.constants import measures

from ihme_cc_paf_calculator.lib import constants

# Copied from 
# with additional cause_id column. Provided by smoking/BMD modeling team
_HOSPITAL_DEATHS_FILE: str = 

_INJURY_OUTCOME_CAUSES: List[int] = [
    690,  # inj_trans_road_pedest
    691,  # inj_trans_road_pedal
    692,  # inj_trans_road_2wheel
    693,  # inj_trans_road_4wheel
    694,  # inj_trans_road_other
    695,  # inj_trans_other
    697,  # inj_falls
    707,  # inj_mech_other
    711,  # inj_animal_nonven
    727,  # inj_homicide_other
]

_INDEX_COLS: List[str] = ["cause_id", "location_id", "sex_id", "year_id", "age_group_id"]


def calculate_injury_pafs(paf_df: pd.DataFrame, rei_id: int, release_id: int) -> pd.DataFrame:
    """Calculate injury PAFs for a given risk.

    Currently applicable to smoking and low bone mineral density. Raises error
    if no applicable PAFs for calculation in paf_df or if both hip and non-hip
    PAFs are not not paf_df.

    Methods:
        * From dataframe of all PAFs, subset down to ones relevant for calculation
            (where REI is either smoking or BMD and cause is either hip fracture or
            non-hip fracture and measure is YLL).
        * Merge on proportion of hospital deaths due to hip/non-hip fractures for specific
            causes. Essentially a translation from N-codes to GBD causes.
        * Calculate injury PAFs by hip/non-hip:
            PAF = PAF (hip/non-hip) * fraction (hip/non-hip)
        * Sum hip and non-hip PAFs:
            PAF (both) = (PAF (hip) + PAF (non-hip))
                = (PAF (hip) * fraction (hip) + PAF (non-hip) * fraction (non-hip)
        * Copy PAFs for YLLs and YLDs

    Args:
        paf_df: dataframe with at least PAF data relevant to the rei_id and injuries.
        rei_id: REI ID to compute injury PAFs for. Either 99 (smoking) or 109 (BMD).
        release_id: ID of the release

    Raises:
        ValueError: if given rei_id is in either 99 or 109; if not applicable PAFs for
            calculation found; if all fracture causes (hip, non-hip) are not present in
            input PAFs
    """
    if rei_id not in constants.INJURY_REI_IDS:
        raise ValueError(
            f"Can only calculate injury PAFs for REIs {constants.INJURY_REI_IDS}, "
            f"not REI {rei_id}."
        )

    # Filter down to injury input causes, REI, and measure 'YLLs' (drop YLD PAFs)
    injury_pafs = paf_df[
        paf_df["cause_id"].isin(constants.FRACTURE_CAUSE_IDS) & (paf_df["rei_id"] == rei_id)
    ].query("measure_id == @measures.YLL")

    # Confirm that we have PAFs for all fracture causes
    if injury_pafs.empty:
        raise ValueError(
            f"No PAFs found for causes {constants.FRACTURE_CAUSE_IDS}, "
            f"REI {rei_id}, measure {measures.YLL}. Cannot calculate injury PAFs."
        )

    cause_ids = injury_pafs["cause_id"].unique().tolist()
    if set(cause_ids) != set(constants.FRACTURE_CAUSE_IDS):
        raise ValueError(
            "PAFs are expected to exist for all fracture causes: "
            f"{constants.FRACTURE_CAUSE_IDS}. Fracture causes found in PAFs: {cause_ids}. "
            "Cannot calculate injury PAFs."
        )

    # Add 'fracture' column for merging on hospital deaths
    injury_pafs["fracture"] = injury_pafs["cause_id"].apply(
        lambda cause_id: "hip" if cause_id == constants.HIP_FRACTURE_CAUSE_ID else "non-hip"
    )
    crossed_injury_pafs = injury_pafs.drop(
        columns=["cause_id", "measure_id"]
    ).merge(  # drop original cause to 'overwrite' with injury outcomes
        _read_hospital_death_proportions(), on=["age_group_id", "sex_id", "fracture"]
    )

    # Recalculate PAF = PAF (hip/non-hip) * fraction (hip/non-hip),
    # then sum PAF (hip) and PAF (non-hip).
    draw_cols = [col for col in paf_df.columns if col.startswith("draw_")]
    results = (
        crossed_injury_pafs.assign(  # PAF (hip/non-hip) * fraction of hospital deaths
            **{
                col: crossed_injury_pafs[col].multiply(crossed_injury_pafs["fraction"])
                for col in draw_cols
            }
        )
        .drop(columns="fraction")
        .groupby(_INDEX_COLS)  # collapse over hip/non-hip
        .sum(numeric_only=True)  # and sum
        .reset_index()
    )

    # Copy injury PAFs for measures YLDs and YLLs, add rei_id column, reorder columns
    results = (
        results.merge(
            pd.DataFrame({"measure_id": [measures.YLD, measures.YLL]}), how="cross"
        ).assign(rei_id=rei_id)
    )[["rei_id"] + _INDEX_COLS + ["measure_id"] + draw_cols]

    # Confirm we have the number of rows expected
    # Row count is constrained by the overlap of the available age groups and sexes
    # from the cross of relevant PAF draws and hospital death proportions.
    # Results have 2 rows per measure, crossed injury PAFs have 2 rows per hip/non-hip
    if len(results) != len(crossed_injury_pafs):
        raise RuntimeError(
            f"After calculating injury PAFs, expected {len(crossed_injury_pafs)} "
            f"rows. Only {len(results)} rows exist."
        )

    return _apply_restrictions_from_db(results, release_id)


def _read_hospital_death_proportions() -> pd.DataFrame:
    """Read in hospital death proportions by injury categorized as hip/non-hip."""
    # CCOMP-7814: revists if it is ok this csv has less age groups than modeled pafs
    return (pd.read_csv(_HOSPITAL_DEATHS_FILE).query("cause_id in @_INJURY_OUTCOME_CAUSES"))[
        ["sex_id", "age_group_id", "cause_id", "fracture", "fraction"]
    ]


def _apply_restrictions_from_db(df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """Apply cause level age/sex/measure restrictions to input data.

    Note: this function is mostly copied from ihme_cc_paf_aggregator.

    Arguments:
        df: data to restrict
        release_id: release to get restriction mapping for

    Returns:
        data possibly subsetted to fewer demographics
    """
    for restriction_type in ["age", "sex"]:
        df = _restrict_by_type(df, release_id, restriction_type)

    return df


def _restrict_by_type(
    df: pd.DataFrame, release_id: int, restriction_type: str
) -> pd.DataFrame:
    """Drop any rows that are specified by get_restrictions."""
    restrictions = db_queries.get_restrictions(
        restriction_type=restriction_type,
        age_group_id=df["age_group_id"].unique().tolist(),
        cause_id=df["cause_id"].unique().tolist(),
        sex_id=df["sex_id"].unique().tolist(),
        measure_id=measures.DEATH,
        release_id=release_id,
    ).assign(drop_column=1)

    # Return df without change if no restrictions found
    if restrictions.empty:
        return df

    if restriction_type == "age":
        restrictions = restrictions.query(
            "is_applicable == 1"  # is_applicable is 1 if 'true' restriction
        ).drop(columns=["is_applicable", "measure_id"])
        merge_cols = ["cause_id", "age_group_id"]
    elif restriction_type == "sex":
        merge_cols = ["cause_id", "sex_id"]

    return (
        df.merge(restrictions, on=merge_cols, how="left")
        .query("drop_column.isna()", engine="python")
        .drop(columns="drop_column")
        .reset_index(drop=True)
    )
