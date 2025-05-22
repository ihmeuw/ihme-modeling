"""Risk-related functions. Consider chunking differently as makes sense in the future."""

from typing import List

import pandas as pd

from ihme_cc_sev_calculator.lib import constants


def get_child_rei_ids(
    aggregate_rei_id: int, rei_metadata: pd.DataFrame, exclude_no_rr_rei_ids: bool = True
) -> List[int]:
    """Get most detailed child REI IDs for the given aggregate REI ID.

    Intended to be used for deciding RR max aggregation job dependencies, so
    some child REIs whose RR max is custom calculated are excluded.

    Assumes rei_metadata has been processed as in parameters._pull_rei_metadata.

    Args:
        aggregate_rei_id: REI ID of an aggregate risk to return child risks for
        rei_metadata: REI metadata. Expected to be the aggregation set
        exclude_no_rr_rei_ids: True if returned child risks should not include risks
            without RR models. False if child risks without RR models should be included.
    """
    child_rei_ids = (
        rei_metadata.query("most_detailed == 1")
        # path_to_top_parent starts with aggregate_rei_id followed by a comma or
        # it's sandwiched between two commas.
        # "?:" is syntax for a matching group, not a capturing group
        .query(f"path_to_top_parent.str.contains('(?:^|,){aggregate_rei_id},')")
    )["rei_id"].tolist()

    if exclude_no_rr_rei_ids:
        child_rei_ids = [
            rei_id for rei_id in child_rei_ids if rei_id not in constants.NO_RR_REI_IDS
        ]

    return child_rei_ids
