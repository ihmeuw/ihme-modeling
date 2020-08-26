from typing import Dict, Set

from gbd.constants import sex

# Age group IDs for specific ages
AGE_SPECIFIC: Set[int] = frozenset(list(range(2, 21)) + [30, 31, 32, 164, 235])

# Age group IDs for all ages
AGE_GROUPED: Set[int] = frozenset([1, 28] + list(range(21, 27)))

# The cutoff year for identifying forecasting models
FORECASTING_YEAR: int = 2022

# Sex IDs for specific sexes
SEX_SPECIFIC: Set[int] = frozenset({sex.MALE, sex.FEMALE})

# Sex IDs for all sexes
SEX_GROUPED: Set[int] = frozenset({sex.BOTH})

SEX_MAP: Dict[str, int] = {
    'male': sex.MALE,
    'female': sex.FEMALE,
    'both': sex.BOTH
}
