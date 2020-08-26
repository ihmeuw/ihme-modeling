from typing import Dict

# Maps specific edge case age_group_ids to their age midpoints by
# GBD round id.
# The default age midpoint is calculated as (age_start + age_end) / 2,
# but for some age groups like 80+, 95+, etc. the logic doesn't follow.
# We aren't sure why age group 5 is hardcoded and have decided to drop it
# for GBD 2020 but keep it for GBD 2019 for legacy reasons.
SPECIFIC_AGE_MIDPOINTS: Dict[int, Dict[int, float]] = {
    5: {
        5: 2.5,  # 1 - 4
        21: 82.5,  # 80+
        235: 97.5  # 95+
    },
    6: {
        5: 2.5,  # 1 - 4
        21: 82.5,  # 80+
        235: 97.5  # 95+
    },
    7: {
        21: 82.5,  # 80+
        235: 97.5  # 95+
    }
}
