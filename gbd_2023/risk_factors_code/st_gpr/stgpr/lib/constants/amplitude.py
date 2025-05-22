from typing import Final

# If a custom amplitude cutoff isn't passed, then set cutoff to 80th percentile of data
# density.
DEFAULT_CUTOFF_PERCENTILE: Final[int] = 80

# After the spacetime stage, ST-GPR saves "amplitude" to final model results as median
# absolute deviation (MAD). During the GPR stage, ST-GPR uses scaled MAD (i.e. MAD * 1.4826)
# as an estimator for standard deviation. It's more appropriate to think of amplitude as this
# scaled quantity than as unscaled MAD.
# https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
MAD_SCALAR: Final[float] = 1.4826
