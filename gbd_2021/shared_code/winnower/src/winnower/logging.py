import functools
import inspect
import logging
import logging.config
import os


HERE = os.path.abspath(os.path.dirname(__file__))

logging.config.fileConfig(os.path.join(HERE, 'log_config.ini'))

# loggers
notimplementedLogger = logging.getLogger('winnower.NotImplemented')
caseLogger = logging.getLogger('winnower.compat.case')


# handlers

# formatters


# other
def get_class_logger(obj):
    """
    Return logger for object, namespaced off of the module and class name.
    """
    cls = obj if inspect.isclass(obj) else type(obj)
    # cls.__modulename__ is equivalent to __name__ from within that module
    return logging.getLogger(f"{cls.__module__}.{cls.__name__}")


def log_case_mismatch(fixed, mismatch):
    msg = f"case mismatch - using {fixed} instead of {mismatch}"
    caseLogger.debug(msg)


def log_notimplemented(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        # TODO: detect if wrapping a class method
        # get additional class information (if possible)
        try:
            return func(*args, **kwargs)
        except NotImplementedError as e:
            msg = '{}: {!r}'.format(func.__name__, e)
            notimplementedLogger.critical(msg)
            # raise NotImplementedError using original traceback
            # suppressing original error (`from None`)
            import sys  # noqa
            tb = sys.exc_info()[2]
            raise NotImplementedError(msg).with_traceback(tb) from None

    return wrapped
