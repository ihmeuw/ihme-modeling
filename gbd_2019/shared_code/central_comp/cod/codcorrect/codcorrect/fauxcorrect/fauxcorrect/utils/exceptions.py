class DeathMachineBaseException(Exception):
    """Base exception for FauxCorrect."""


class InvalidModelVersionParameters(DeathMachineBaseException):
    """
    Raised when the model version parameter invariants for a FauxCorrect run
    are violated.

    Examples include (but are not limited to):
        A model version has an age group lower than its min cause metadata.
        A model version has an age group higher than its max age in cause
            metadata.
        A model version has an invalid sex_id for a sex-specific cause.
    """


class DrawsMissingException(DeathMachineBaseException):
    """Raised when missing indices are detected in draw files."""


class InputDrawsEmpty(DeathMachineBaseException):
    """Raised when get_draws call returns no draws."""


class MoreThanOneBestVersion(DeathMachineBaseException):
    """Raised when get_current_best call returns more than one row"""
