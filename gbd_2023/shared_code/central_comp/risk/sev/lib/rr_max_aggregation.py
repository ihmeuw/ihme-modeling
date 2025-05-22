"""Functions related to aggregating RRmax."""

from typing import List

import pandas as pd

from ihme_cc_sev_calculator.lib import rr_max


def drop_one_hundred_percent_attributable(
    rr_max_df: pd.DataFrame,
    child_rei_ids: List[int],
    pafs_of_one: pd.DataFrame,
    cause_metadata: pd.DataFrame,
    release_id: int,
) -> pd.DataFrame:
    """Drops all RRmaxes that are for most detailed 100% attributable outcomes.

    Also drops rows for aggregate causes if all children are 100% attributable
    to a single risk. `pafs_of_one` only contains most detailed causes, so
    we have to do some munging in order to check for this.
    """
    # If there are no PAFs of 1 for any child risks, return unedited RRmaxes
    pafs_of_one = pafs_of_one.query(f"rei_id.isin({child_rei_ids})")
    if pafs_of_one.empty:
        return rr_max_df

    # Explode cause metadata to give us a row for each parent - child cause, including
    # indirect parents like all-cause for road traffic accidents.
    # Only keep columns cause_id and most_detailed
    cause_metadata = (
        cause_metadata[["cause_id", "most_detailed", "path_to_top_parent"]]
        .set_index(["cause_id", "most_detailed"])
        .apply(lambda x: x.str.split(",").explode())
        .reset_index()
        .rename(columns={"path_to_top_parent": "parent_id"})
        .apply(pd.to_numeric)  # convert all cols to numeric, parent_id specifically
        .query("most_detailed == 1 | cause_id == parent_id")
    )

    pafs_of_one = pafs_of_one.merge(cause_metadata, on="cause_id", how="left")

    # Find risk - parent cause pairs where all the most detailed child causes are NOT
    # in the PAFs of 1 causes for the risk, and remove them from the list of risk - causes
    # to drop
    for rei_id in pafs_of_one["rei_id"].unique():
        # Get all causes related to risk that have PAFs of 1
        pafs_of_one_cause_ids = (
            pafs_of_one.query(f"rei_id == {rei_id}")["cause_id"].unique().tolist()
        )

        # For each parent cause (including causes multiple levels up and the cause itself),
        # get its most detailed child causes. If any of the child causes are not
        # also PAFs of 1 causes, remove risk - parent cause rows from pafs_of_one
        for parent_id in pafs_of_one.query(f"rei_id == {rei_id}")["parent_id"].unique():
            most_detailed_child_cause_ids = cause_metadata.query(
                f"most_detailed == 1 & parent_id == {parent_id}"
            )["cause_id"].tolist()
            child_cause_ids_not_in_pafs_of_one = set(most_detailed_child_cause_ids) - set(
                pafs_of_one_cause_ids
            )

            if child_cause_ids_not_in_pafs_of_one:
                pafs_of_one = pafs_of_one.query(
                    f"~(rei_id == {rei_id} & parent_id == {parent_id})"
                )

    return rr_max_df.query(f"~cause_id.isin({pafs_of_one['parent_id'].tolist()})")


def aggregate_rr_max(rr_max_df: pd.DataFrame, draw_cols: List[str]) -> pd.DataFrame:
    """Aggregate child REI RRmax draws to create RRmax results for the parent REI."""
    # Aggregate RRmax one draw at a time
    draws_list = []
    for col in draw_cols:
        draws_list.append(_aggregate_rr_max(rr_max_df, col))

    return (
        pd.concat(draws_list, axis=1)
        .reset_index()
        .rename(columns={i: draw_cols[i] for i in range(len(draw_cols))})
    )


def _aggregate_rr_max(rr_max_df: pd.DataFrame, draw_col: str) -> pd.DataFrame:
    """Aggregate RRmax helper.

    Done at the cause/age group/sex level.

    RRmax for aggregate REIs is approximated assuming independence for two main reasons:
        * Calculating joint RRmaxes directly is rather difficult since risk functions are
            non-linear and often RRs and exposures have non-standard distributions
        * Taking the product of RRmaxes across risks would result in the RRmax at the
            99.99999th etc percentile.

    Method:
        In general, the approximation is based on the product of RRmaxes across risks
        multiplied by the geometric mean of a ratio for each risk.

        1) Calculate a ratio per child risk, defined as:
            r (risk X) = ((RRmax - 1) * 0.25 + 1) / RRmax
                - We believe the 0.25, 1 terms intend to avoid the issue of looking at
                    a percentile over 99.
                - r = 1 if there's only one child risk to 1 to maintain the same RRmax value
                    for the aggregate risk
        2) Aggregate ratio as the product of child risk ratios to power of 1/# child risks:
            r = (r (risk 1) * r (risk 2) * r (risk 3) ... ) ^ (1 / number of risks)
        3) Multiply the product of child RRmaxs by the ratio calculated above
            RRmax (parent) = r * RRmax (risk 1) * RRmax (risk 2) * RRmax (risk 3) ...
    """
    ratio_col = f"ratio_{draw_col}"
    rr_max_df = rr_max_df[rr_max.INDEX_COLS + [draw_col]]

    # 1)
    rr_max_df[ratio_col] = rr_max_df.apply(
        lambda row: ((row[draw_col] - 1) * 0.25 + 1) / row[draw_col], axis=1
    )

    # 2) and 3)
    return (
        rr_max_df.groupby(rr_max.INDEX_COLS)[[draw_col, ratio_col]]
        .agg(
            **{
                draw_col: (draw_col, "prod"),
                ratio_col: (ratio_col, "prod"),
                "n_risks": (draw_col, "count"),
            }
        )
        .apply(lambda row: row[draw_col] * _nth_root(row[ratio_col], row["n_risks"]), axis=1)
    )


def _nth_root(ratio: float, n_risks: int) -> float:
    """Calculate nth root of the ratio, ratio ^ (1 / n)

    If n_risks = 1, set ratio to 1. See 1) in _aggregate_rr_max.
    """
    if n_risks == 1:
        ratio = 1.0

    return ratio ** (1 / n_risks)
