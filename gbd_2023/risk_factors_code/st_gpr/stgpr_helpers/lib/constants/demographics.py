"""ST-GPR demographics constants."""

from typing import Dict, Final, FrozenSet

from gbd.constants import measures, sex

# Age group IDs for specific ages
AGE_SPECIFIC: FrozenSet[int] = frozenset(list(range(2, 21)) + [30, 31, 32, 164, 235])

# Age group IDs for all ages
AGE_GROUPED: FrozenSet[int] = frozenset([1, 28] + list(range(21, 27)))

# Sex IDs for specific sexes
SEX_SPECIFIC: FrozenSet[int] = frozenset({sex.MALE, sex.FEMALE})

# Sex IDs for all sexes
SEX_GROUPED: FrozenSet[int] = frozenset({sex.BOTH})

SEX_MAP: Final[Dict[str, int]] = {"male": sex.MALE, "female": sex.FEMALE, "both": sex.BOTH}

MEASURE_MAP: Final[Dict[str, int]] = {
    "proportion": measures.PROPORTION,
    "continuous": measures.CONTINUOUS,
}

# Age group IDs that can be used for pregnancy modeling
PREGNANCY_AGE_GROUP_IDS = frozenset([7, 8, 9, 10, 11, 12, 13, 14, 15])

# Starting year past which pregnancy models can run
PREGNANCY_YEAR_START = 1990
