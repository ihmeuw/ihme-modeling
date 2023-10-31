import sys


class MoreThanOneBestVersionError(Exception):
    """
    exception for when more than one model is found for the given
    parameters
    """


class NoBestVersionError(Exception):
    """exception for when a model isn't found for the given parameters"""


# We need to pull the traceback from an individual process if it fails during
# multiprocessing, ExceptionWrapper was pulled from the following link:
# https://stackoverflow.com/questions/6126007/python-getting-a-traceback-from-a-multiprocessing-process
class ExceptionWrapper(object):

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        raise self.ee.with_traceback(self.tb)

