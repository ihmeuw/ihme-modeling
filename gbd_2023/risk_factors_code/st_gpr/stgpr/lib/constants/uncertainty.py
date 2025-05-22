from typing import Final

# Lower and upper percentiles for 95% confidence intervals
LOWER_QUANTILE: Final[float] = 0.025
UPPER_QUANTILE: Final[float] = 1 - LOWER_QUANTILE
