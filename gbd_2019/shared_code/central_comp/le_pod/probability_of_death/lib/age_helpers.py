from typing import Dict, List

import pandas as pd

from db_tools import ezfuncs
from hierarchies import dbtrees

from probability_of_death.lib.constants import columns, queries

# Age groups for which to produce results, by GBD round.
_AGE_GROUPS: Dict[int, List[int]] = {
    5: [
        # Aggregate age groups.
        1,
        23,
        24,
        28,
        41,
        155,
        158,
        159,
        172,
        230,
        284,
        285,
        286,
        287,
        288,
        289,
    ],
    6: [
        # Aggregate age groups.
        1,
        21,
        22,
        23,
        24,
        25,
        26,
        28,
        37,
        39,
        41,
        155,
        157,
        158,
        159,
        160,
        162,
        169,
        172,
        188,
        197,
        206,
        228,
        230,
        232,
        234,
        243,
        284,
        285,
        286,
        287,
        288,
        289,
        420,
        430,
        # Most-detailed age groups.
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        30,
        31,
        32,
    ],
}


def get_default_age_groups(gbd_round_id: int) -> List[int]:
    """Looks up default age groups associated with a GBD round ID."""
    if gbd_round_id not in _AGE_GROUPS:
        raise RuntimeError(f"No default age groups for GBD round {gbd_round_id}")
    return _AGE_GROUPS[gbd_round_id]


def get_age_group_map(gbd_round_id: int, age_group_ids: List[int]) -> Dict[int, List[int]]:
    """Gets dictionary of age group ID to age group IDs in the aggregate.

    Replaces detailed age groups [2, 3, 4] with aggregate age group 28 since life tables
    are not produced for age groups [2, 3, 4].

    Sorts age groups by age start since probability of death calculation relies on age groups
    being in sorted order.

    Most-detailed age groups are returned in the same format as aggregate age groups in order
    to provide the probability of death calculation with a consistent data structure. E.g:
    {
        6: [6]                # Most detailed
        21: [30, 31, 32, 235] # Aggregate
    }

    Args:
        gbd_round_id: ID of the GBD round with which to build age trees.
        age_group_ids: IDs of age groups, both aggregate and most detailed.

    Returns:
        Dictionary of age group ID to age group IDs that comprise the aggregate.
    """
    age_groups_with_starts = ezfuncs.query(
        queries.GET_AGE_STARTS, conn_def="cod", parameters={"age_group_ids": age_group_ids}
    )

    age_group_map: Dict[int, List[int]] = {}
    for age_group_id in age_group_ids:
        tree = dbtrees.agetree(age_group_id, gbd_round_id)
        detailed_ids = [node.id for node in tree.root.children]
        by_age_start = _sort_age_group_ids(detailed_ids, age_groups_with_starts)
        under_one_replaced = _replace_under_one(by_age_start)
        age_group_map[age_group_id] = under_one_replaced
    return age_group_map


def _sort_age_group_ids(
    age_group_ids: List[int], age_groups_with_starts: pd.DataFrame
) -> List[int]:
    """Sorts age groups by age start."""
    return (
        age_groups_with_starts[
            age_groups_with_starts[columns.AGE_GROUP_ID].isin(age_group_ids)
        ]
        .sort_values(by=columns.AGE_GROUP_DAYS_START)
        .loc[:, columns.AGE_GROUP_ID]
        .tolist()
    )


def _replace_under_one(age_group_ids: List[int]) -> List[int]:
    """Replaces under-one most-detailed age groups with under-one aggregate age group."""
    age_groups_str = ",".join([str(age_group_id) for age_group_id in age_group_ids])
    replaced = age_groups_str.replace("2,3,4", "28")
    return [int(age_group_id) for age_group_id in replaced.split(",")]
