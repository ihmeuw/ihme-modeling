from typing import List, Tuple

import db_queries
from db_queries.get_age_metadata import get_age_spans

from hale.common.constants import age_groups, columns


def get_hale_ages(gbd_round_id: int) -> Tuple[List[int], List[int]]:
    """
    Pull age group IDs used in HALE outputs and age group IDs used to aggregate
    under-one ages.
    """
    # Get age group IDs used in this GBD round and sort chronologically.
    demo_df = db_queries.get_demographics('epi', gbd_round_id)
    age_group_ids = demo_df[columns.AGE_GROUP_ID]
    age_spans = get_age_spans()\
        .query(f'{columns.AGE_GROUP_ID} in @age_group_ids')\
        .sort_values(columns.AGE_GROUP_YEARS_START)

    # Get under-one age groups (including birth) and full list of HALE age
    # groups.
    under_one_ages = [age_groups.BIRTH] + age_spans.loc[
        age_spans[columns.AGE_GROUP_YEARS_START] < 1, columns.AGE_GROUP_ID
    ].tolist()
    hale_ages = [age_groups.UNDER_ONE] + age_spans.loc[
        ~age_spans[columns.AGE_GROUP_ID].isin(under_one_ages),
        columns.AGE_GROUP_ID
    ].tolist()

    return hale_ages, under_one_ages
