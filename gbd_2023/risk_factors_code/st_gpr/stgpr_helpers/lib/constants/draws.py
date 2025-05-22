"""ST-GPR draws constants."""

from typing import Final, FrozenSet

MAX_GPR_DRAWS: Final[int] = 1000
ALLOWED_GPR_DRAWS: FrozenSet[int] = frozenset({0, 100, MAX_GPR_DRAWS})
