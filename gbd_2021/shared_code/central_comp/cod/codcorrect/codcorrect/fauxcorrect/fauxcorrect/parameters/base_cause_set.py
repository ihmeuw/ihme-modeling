"""Functions for converting the base cause set into a correction (scaling set).

The CodCorrect correction algorthim depends upon a correction (scaling) hierarchy that
only has causes that have data that go into the machinery run. Ie, if there is no
model for a cause, that cause must be dropped from the correction hierarchy. Cause
levels and parent causes must be adjusted subsequently, which means that any cause who's
parent cause does not have estimates will have their parent switched to all-cause (294) and
level to one below all-cause (1). That causes's child causes will also be shifted up.

HIV causes and causes that ONLY have shock models are not included in scaling.

Eventually, automatic generation of the correction cause set may replace
the offical correction cause set, cause set 1. For now since it is still actively
maintained, we need to validate the auto-generated correction set against the
official one for official runs.
"""
from typing import List

import pandas as pd

from db_tools import ezfuncs, query_tools
import db_queries

from fauxcorrect.queries.queries import HIV
from fauxcorrect.utils.constants import (
    Causes, CauseSetId, ConnectionDefinitions, ModelVersionTypeId
)


def create_correction_hierarchy(
    cause_set_id: int,
    gbd_round_id: int,
    decomp_step: str,
) -> pd.DataFrame:
    """Given a complete cause set, manipulates to turn it into a
    proper correction (scaling) hierarchy.

    Rules:
        1) is_estimate_cod == 1, these are all causes where we expect fatal models
        3) No HIV causes or shock-only causes. Shock-only causes are determined by
            shock_cause==2
        4) Include all-cause (294) that we sub with non shock, non HIV env
        5) Reset level and parent ids, dropping causes without estimates
    """
    full_hierarchy = db_queries.get_cause_metadata(
        cause_set_id=cause_set_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )

    if (
        full_hierarchy["is_estimate_cod"].isna().any() or
        full_hierarchy["shock_cause"].isna().any()
    ):
        raise ValueError(
            f"Cause set {cause_set_id} for GBD round {gbd_round_id}, {decomp_step} "
            "does not have metadata for columns 'is_estimate_cod' and 'shock_cause'. "
            "These columns are needed to create the correction hierarchy."
        )

    # Apply rules 1 - 4
    correction_hierarchy = full_hierarchy[
        (full_hierarchy.cause_id == Causes.ALL_CAUSE) | (
            (full_hierarchy["is_estimate_cod"] == 1) &  # Must be estimated fatally
            ~(full_hierarchy["shock_cause"] == 2) &     # Not shock only
            ~(full_hierarchy["cause_id"].isin(_get_hiv_causes(cause_set_id, gbd_round_id)))
        )
    ]

    # Apply rule 5
    return _trim_hierarchy(correction_hierarchy)


def validate_correction_hierarchy(
    hierarchy: pd.DataFrame,
    base_cause_set_id: int,
    gbd_round_id: int,
    decomp_step: str
) -> None:
    """Validates the created correction (scaling) hierarchy against an official
    correction hierarchy (CodCorret, set 1). The validation is skipped if the
    base cause set is not the Computation set, meaning this is not an official
    CodCorrect run.

    Checks:
        1) The same causes are contained within each
        2) The levels are identical for each cause
        3) The parent ids are identical for each cause

    Args:
        hierarchy: a created computation hierarchy to validate
        base_cause_set_id: Base cause set id. If Computation (2), skip the validation.
            This parameter isn't used for anything else.
        gbd_round_id: GBD round id
        decomp_step: decomp step
    """
    if base_cause_set_id != CauseSetId.COMPUTATION:
        return

    official_hierarchy = db_queries.get_cause_metadata(
        cause_set_id=CauseSetId.CODCORRECT,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step
    )

    # Confirm rule 1: both have same causes
    sym_diff = set(hierarchy.cause_id).symmetric_difference(set(official_hierarchy.cause_id))
    if sym_diff:
        raise ValueError(
            f"Created correction hierarchy ({len(hierarchy)} causes) and official correction "
            f"hierarchy ({len(official_hierarchy)} causes) have different causes. "
            f"Symmetric difference: {sym_diff}"
        )

    both = pd.merge(
        hierarchy, official_hierarchy, on="cause_id", suffixes=("_created", "_official")
    )
    different_levels = both[both["level_created"] != both["level_official"]][
        ["cause_id", "level_created", "level_official"]
    ]
    different_parents = both[both["parent_id_created"] != both["parent_id_official"]][
        ["cause_id", "parent_id_created", "parent_id_official"]
    ]

    # Confirm rule 2 and 3
    if not different_levels.empty:
        raise ValueError(
            f"{len(different_levels)} causes have different levels compared to official "
            f"correction hierarchy:\n{different_levels}"
        )
    if not different_parents.empty:
        raise ValueError(
            f"{len(different_parents)} causes have different parent causes compared to "
            f"official correction hierarchy:\n{different_parents}"
        )


def _get_hiv_causes(cause_set_id: int, gbd_round_id: int) -> List[int]:
    """Returns list of HIV causes.

    Note:
        This function is copied from CauseParameters._get_hiv_cause_ids.
        I did not want a dependency on that class here, but should find a way
        to combine in the future.

    Args:
        cause_set_id: cause set id, must be valid (not checked)
        gbd_round_id: GBD round id
    """
    with ezfuncs.session_scope(ConnectionDefinitions.COD) as session:
        hiv_causes = query_tools.query_2_df(
            HIV.GET_HIV_CAUSES,
            session=session,
            parameters={
                "cause_set_id": cause_set_id,
                "gbd_round_id": gbd_round_id
            }
        )
    return hiv_causes["cause_id"].tolist()


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
