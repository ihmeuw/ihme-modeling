"""ST-GPR demographics constants."""
from typing import Dict, FrozenSet

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from gbd.constants import measures, sex

# Age group IDs for specific ages
AGE_SPECIFIC: FrozenSet[int] = frozenset(list(range(2, 21)) + [30, 31, 32, 164, 235])

# Age group IDs for all ages
AGE_GROUPED: FrozenSet[int] = frozenset([1, 28] + list(range(21, 27)))

# The cutoff year for identifying forecasting models
FORECASTING_YEAR: Final[int] = 2022

# Sex IDs for specific sexes
SEX_SPECIFIC: FrozenSet[int] = frozenset({sex.MALE, sex.FEMALE})

# Sex IDs for all sexes
SEX_GROUPED: FrozenSet[int] = frozenset({sex.BOTH})

SEX_MAP: Final[Dict[str, int]] = {"male": sex.MALE, "female": sex.FEMALE, "both": sex.BOTH}

MEASURE_MAP: Final[Dict[str, int]] = {
    "proportion": measures.PROPORTION,
    "continuous": measures.CONTINUOUS,
}
