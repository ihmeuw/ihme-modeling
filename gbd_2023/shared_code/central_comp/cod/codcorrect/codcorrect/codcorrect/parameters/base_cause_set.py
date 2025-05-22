"""Functions for converting the base cause set into a correction (scaling set).

The CodCorrect correction algorthim depends upon a correction (scaling) hierarchy that
only has causes that have data that go into the machinery run. Ie, if there is no
model for a cause, that cause must be dropped from the correction hierarchy. Cause
levels and parent causes must be adjusted subsequently, which means that any cause who's
parent cause does not have estimates will have their parent switched to all-cause (294) and
level to one below all-cause (1). That causes's child causes will also be shifted up.

Causes that ONLY have shock models are not included in scaling.

Eventually, automatic generation of the correction cause set may replace
the offical correction cause set, cause set 1.
"""

from typing import Optional

import pandas as pd
from sqlalchemy import orm

import db_queries

from codcorrect.legacy.utils.constants import Causes


def create_correction_hierarchy(
    cause_set_id: int, release_id: int, session: Optional[orm.Session] = None
) -> pd.DataFrame:
    """Given a complete cause set, manipulates to turn it into a
    proper correction (scaling) hierarchy.

    Rules:
        1) is_estimate_cod == 1, these are all causes where we expect fatal models
        2) No shock-only causes. Shock-only causes are determined by shock_cause==2
        3) Include all-cause (294) that we sub with the non-shock envelope
        4) Reset level and parent ids, dropping causes without estimates
    """
    full_hierarchy = db_queries.get_cause_metadata(
        cause_set_id=cause_set_id, release_id=release_id
    )

    if (
        full_hierarchy["is_estimate_cod"].isna().any()
        or full_hierarchy["shock_cause"].isna().any()
    ):
        raise ValueError(
            f"Cause set {cause_set_id} for release id {release_id} "
            "does not have metadata for columns 'is_estimate_cod' and 'shock_cause'. "
            "These columns are needed to create the correction hierarchy."
        )

    # Apply rules 1 - 4
    correction_hierarchy = full_hierarchy[
        (full_hierarchy.cause_id == Causes.ALL_CAUSE)
        | (
            (full_hierarchy["is_estimate_cod"] == 1)  # Must be estimated fatally
            & ~(full_hierarchy["shock_cause"] == 2)  # Not shock only
        )
    ]

    # Apply rule 5
    return _trim_hierarchy(correction_hierarchy)


def _trim_hierarchy(hierarchy: pd.DataFrame) -> pd.DataFrame:
    """Trim cause hierarchy, reseting the level and parent id
    as necessary to create a correction (scaling) hierarchy.

    Args:
        hierarchy: a cause hierarchy with at least the columns 'parent_id', 'level',
            'cause_id', and 'sort_order'

    Algorithm:
        If a cause's parent is not in the hierarchy, bump up to level 1 and change parent to
        all-cause. If the parent is in the hierarchy, change level to parent level + 1
    """
    # Move from top (lowest level) to bottom, as the results of children depend on parents
    for level in range(1, max(hierarchy.level) + 1):
        current_level = hierarchy[hierarchy.level == level].copy()
        other_levels = hierarchy[hierarchy.level != level].copy()

        if current_level.empty:
            continue

        for index, row in current_level.iterrows():
            parent_cause_in_hierarchy = row["parent_id"] in set(hierarchy.cause_id)

            if parent_cause_in_hierarchy:
                parent_level = hierarchy[hierarchy.cause_id == row["parent_id"]].level.iat[0]
                current_level.loc[index, "level"] = parent_level + 1
            else:
                current_level.loc[index, "level"] = 1
                current_level.loc[index, "parent_id"] = Causes.ALL_CAUSE

        # Maintain level/parent changes in next iteration
        hierarchy = pd.concat([current_level, other_levels])

    return hierarchy.sort_values(by=["sort_order"])
