from retrying import retry
from jobmon import sge


@retry(wait_exponential_multiplier=1000,
       wait_exponential_max=15000,
       stop_max_attempt_number=10)
def qstat_w_retry(*args, **kwargs):
    """
    Call jobmon.sge.qstat with exponential backoff if there's
    a failure. Max out at 10 attempts, then raise
    """
    return sge.qstat(*args, **kwargs)


@retry(wait_exponential_multiplier=1000,
       wait_exponential_max=15000,
       stop_max_attempt_number=10)
def qsub_w_retry(*args, **kwargs):
    """
    Call jobmon.sge.qsub with exponential backoff if there's
    a failure. Max out at 10 attempts, then raise
    """
    return sge.qsub(*args, **kwargs)


def get_commit_hash(*args, **kwargs):
    return sge.get_commit_hash(*args, **kwargs)


def add_holds(*args, **kwargs):
    return sge.add_holds(*args, **kwargs)
