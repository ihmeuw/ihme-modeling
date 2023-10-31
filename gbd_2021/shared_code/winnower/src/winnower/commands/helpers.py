import functools
import sys


def suppress_broken_pipes(f):
    """
    Decorator function that handles/suppresses BrokenPipeError.

    This helps facilitate using reports in tandem with e.g., `head`
    """
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except BrokenPipeError:
            pass

        # Requires second try/catch to prevent output regarding
        # a BrokenPipeError
        # https://stackoverflow.com/a/18954489
        try:
            sys.stdout.close()
        except IOError:
            pass

    return wrapped
