

class MoreThanOneBestVersionError(Exception):
    """
    exception for when more than one model is found for the given
    parameters
    """

class NoBestVersionError(Exception):
    """exception for when a model isn't found for the given parameters"""

