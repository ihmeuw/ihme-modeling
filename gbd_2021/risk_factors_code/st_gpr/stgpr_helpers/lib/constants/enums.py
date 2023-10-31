"""ST-GPR enums."""
import enum


class Holdout(enum.Enum):
    """Holdout values."""

    HELD_OUT = 0
    USED_IN_MODEL = 1


class Outlier(enum.Enum):
    """Outlier values."""

    IS_NOT_OUTLIER = 0
    IS_OUTLIER = 1


class CovariateType(enum.Enum):
    """Covariate types."""

    CUSTOM = 0
    GBD = 1
