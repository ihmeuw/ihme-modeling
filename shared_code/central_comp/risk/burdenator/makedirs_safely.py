import errno
import os
import logging

from dalynator.constants import UMASK_PERMISSIONS

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)


def makedirs_safely(d):
    """Safe across multiple processes. First: it will only do it if it does not exist. Second, if there is a race
        between two processes on that 'if' then it still will not crash"""
    try:
        if not os.path.exists(d):
            os.makedirs(d)
            # python 3.2 has exist_ok flag, python 2 does not.
    except OSError as e:
        if e.errno == errno.EEXIST:
            # FYI errno.EEXIST == 17
            # Race condition - two processes try to create the same directory at almost the same time!
            logger.info("Process could not create directory {} because it already existed,"
                        " probably due to race condition, no error, continuing".format(d))
            pass
        else:
            logger.error("Process could not create directory {}, re-raising".format(d))
            raise
