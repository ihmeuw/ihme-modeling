import logging

import gbd.constants as gbd
import pandas as pd

from dalynator.lib.utils import get_index_draw_columns

logger = logging.getLogger(__name__)


def risk_attr_burden_to_paf(
    risk_cause_df: pd.DataFrame, hundred_percent_pafs_df: pd.DataFrame
) -> pd.DataFrame:
    """Converts risk-attributable burden to PAFs."""
    index_columns, value_columns = get_index_draw_columns(risk_cause_df)
    merge_columns = list(set(index_columns) - {"cause_id", "rei_id", "star_id"})
    # Get the cause-level envelope
    if "star_id" in risk_cause_df:
        burden_by_cause = risk_cause_df.query(
            "rei_id == @gbd.risk.TOTAL_ATTRIBUTABLE and "
            "star_id == @gbd.star.ANY_EVIDENCE_LEVEL"
        )
    else:
        burden_by_cause = risk_cause_df.query("rei_id == @gbd.risk.TOTAL_ATTRIBUTABLE")

    logger.info(
        "APPLY PAFS BEGIN burden_by_cause {}".format(
            get_index_draw_columns(burden_by_cause)[0]
        )
    )
    # Merge cause-level envelope onto data
    paf_df = risk_cause_df.merge(
        burden_by_cause, on=merge_columns + ["cause_id"], suffixes=("", "_bbc")
    )
    # Divide attributable burden by cause-level envelope
    bbc_vcs = ["{}_bbc".format(col) for col in value_columns]
    paf_df[value_columns] = paf_df[value_columns].values / paf_df[bbc_vcs].values
    paf_df[value_columns] = paf_df[value_columns].fillna(0)

    # Set certain cause-risk pairs to 100 % pafs
    if hundred_percent_pafs_df.empty:
        logger.debug("No hundred-percent PAFs detected")
    else:
        hundred_percent_pafs_df["full_paf"] = 1
        paf_df = pd.merge(
            paf_df, hundred_percent_pafs_df, on=["cause_id", "rei_id"], how="left"
        )

        # Skip over AGE_STANDARDIZED entries. Child age groups for this aggregate may have
        # hundred-percent PAFs, but, with age restrictions, the aggregation results in PAFs
        # < 100%.
        set_to_one = (paf_df["full_paf"] == 1) & (
            paf_df["age_group_id"] != gbd.age.AGE_STANDARDIZED
        )
        paf_rows = paf_df.loc[set_to_one].index.tolist()
        # for all the 100% pafs, make sure that the draws arent all equal
        # to 0. If they are all 0 they are not 100% attributable
        should_be_one_rows = paf_df.index.isin(paf_rows)
        not_actually_one_rows = (paf_df.loc[should_be_one_rows, value_columns] == 0).all(
            axis=1
        )
        paf_rows = list(
            set(paf_rows) - set(not_actually_one_rows[not_actually_one_rows].index)
        )
        paf_df.loc[paf_rows, value_columns] = 1.0

    # Change metric to percent
    paf_df["metric_id"] = gbd.metrics.PERCENT
    logger.info("APPLY PAFS END")
    # Keep only the columns we need
    if "star_id" in paf_df:
        keep_cols = merge_columns + ["cause_id", "rei_id", "star_id"] + value_columns
    else:
        keep_cols = merge_columns + ["cause_id", "rei_id"] + value_columns
    return paf_df[keep_cols]