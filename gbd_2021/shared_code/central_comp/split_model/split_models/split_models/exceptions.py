class BaseSplitModelException(Exception):
    """Base exception for split_models"""


class IllegalSplitEpiArgument(BaseSplitModelException):
    """Raised when an input argument to split_epi_model is invalid."""


class IllegalSplitCoDArgument(BaseSplitModelException):
    """Raised when an input argument to split_cod_model is invalid."""
