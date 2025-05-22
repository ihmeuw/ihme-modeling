from typing import Dict, Final

_DEFAULT_AGE_MIDPOINTS = {21: 82.5, 235: 97.5}  # 80+, 95+


# Maps specific edge case age_group_ids to their age midpoints by GBD round id.
# The default age midpoint is calculated as (age_start + age_end) / 2, but for some age groups
# like 80+, 95+, etc. the logic doesn't follow.
SPECIFIC_AGE_MIDPOINTS: Final[Dict[int, Dict[int, float]]] = {
    5: {5: 2.5, 21: 82.5, 235: 97.5},  # 1-4, 80+, 95+
    6: {5: 2.5, 21: 82.5, 235: 97.5},  # 1-4, 80+, 95+
    7: _DEFAULT_AGE_MIDPOINTS,
    8: _DEFAULT_AGE_MIDPOINTS,
    9: _DEFAULT_AGE_MIDPOINTS,
    10: _DEFAULT_AGE_MIDPOINTS,
}
