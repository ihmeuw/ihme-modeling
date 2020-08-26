import errno
import os
import logging

from dalynator.constants import UMASK_PERMISSIONS

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)


def makedirs_safely(d):
    """Safe across multiple processes. First: it will only do it if it does
       not exist. Second, if there is a race between two processes on that
       'if' then it still will not crash"""
    try:
        if not os.path.exists(d):
            os.makedirs(d)

    except OSError as e:
        if e.errno == errno.EEXIST:

            logger.info("Process could not create directory {} because it "
                        "already existed, probably due to race condition, "
                        "no error, continuing".format(d))
            pass
        else:
            logger.error("Process could not create directory {}, re-raising"
                         .format(d))
            raise
