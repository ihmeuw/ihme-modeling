from typing import List

import pandas as pd

import db_queries
import db_tools_core

from ihme_cc_paf_aggregator.lib import constants, utils


def get_pafs_of_one(release_id: int) -> pd.DataFrame:
    """Retrieve a dataset of 100% attributable risk-cause pairs for most-detailed causes
    and any risk with cause-risk metadata for the given release.

    Returns:
        dataframe with columns rei_id and cause_id
    """
    query = """
    SELECT
    cm.cause_id, cm.rei_id
FROM
    shared.cause_risk_metadata_history cm
        JOIN
    (SELECT
        cause_risk_metadata_version_id
    FROM
        shared.cause_risk_metadata_version_release_active
    WHERE
        release_id = :release_id) cmv
            ON cmv.cause_risk_metadata_version_id = cm.cause_risk_metadata_version_id
        JOIN
    (SELECT
        cause_id
    FROM
        shared.cause_hierarchy_history chh
    JOIN shared.cause_set_version_release_active csva USING (cause_set_version_id)
    WHERE
        release_id = :release_id AND most_detailed = 1
            AND chh.cause_set_id = :cause_set) most_detailed_causes
                ON cm.cause_id = most_detailed_causes.cause_id
WHERE
    cm.cause_risk_metadata_type_id = 1
        AND cm.cause_risk_metadata_value = 1;
        """
    with db_tools_core.session_scope("shared-vip") as sesh:
        df = db_tools_core.query_2_df(
            query,
            session=sesh,
            parameters={
                "release_id": release_id,
                "cause_set": constants.COMPUTATION_CAUSE_SET_ID,
            },
        )
    return df


def filter_pafs_of_one(
    input_pafs: pd.DataFrame, pafs_of_one_df: pd.DataFrame
) -> pd.DataFrame:
    """Filter input PAFs so that 100% attributable risk/causes are not present."""
    input_pafs = input_pafs.merge(pafs_of_one_df.assign(should_drop=1), how="left")
    return input_pafs[input_pafs.should_drop.isna()].drop("should_drop", axis=1)


def expand_demographics(
    age_group_id: List[int],
    sex_id: List[int],
    year_id: List[int],
    location_id: List[int],
    measure_id: List[int],
    pafs_of_one: pd.DataFrame,
    draw_cols: List[str],
) -> pd.DataFrame:
    """Given a set of input data, expand pafs of one so all risk/causes have the cartesian
    product of input demographics. Set draw column values to 1.
    """
    demographics = {
        constants.LOCATION_ID: location_id,
        constants.YEAR_ID: year_id,
        constants.SEX_ID: sex_id,
        constants.AGE_GROUP_ID: age_group_id,
        constants.MEASURE_ID: measure_id,
    }

    cartesian_product = pd.MultiIndex.from_product(
        demographics.values(), names=demographics.keys()
    ).to_frame(index=False)

    output = pafs_of_one.merge(cartesian_product, how="cross").assign(
        **{draw: 1 for draw in draw_cols}
    )
    return output


def append_pafs_of_one(
    location_id: int, year_ids: List[int], paf_df: pd.DataFrame, release_id: int
) -> pd.DataFrame:
    """
    Prior to aggregation we want to insert risk/causes with PAFs of 1. If those
    risk/causes also exist in the input PAFS, we drop them.

    We only insert risk/causes with PAFs of 1 if those risks are in the input pafs
    (because we may be running on a smaller hierarchy).

    Arguments:
        location_id: used to generate demographics
        year_ids: used to generate demographics
        paf_df: input PAFs prior to aggregation
        release_id: used to determine which version of cause/risk pairs to use

    Returns:
        paf_df with pafs of 1 appended
    """
    pafs_of_one = get_pafs_of_one(release_id)
    demographics = db_queries.get_demographics("epi", release_id=release_id)
    _, draw_cols = utils.get_index_draw_columns(paf_df)

    paf_df = filter_pafs_of_one(paf_df, pafs_of_one)
    pafs_of_one = expand_demographics(
        age_group_id=demographics[constants.AGE_GROUP_ID],
        sex_id=demographics[constants.SEX_ID],
        year_id=year_ids,
        location_id=[location_id],
        measure_id=constants.MEASURE_IDS,
        pafs_of_one=pafs_of_one,
        draw_cols=draw_cols,
    )

    present_reis = paf_df.rei_id.unique()
    combined_output_df = pd.concat(
        [paf_df, pafs_of_one[pafs_of_one.rei_id.isin(present_reis)]], ignore_index=True
    )
    return combined_output_df
