import sys
import os
import logging

'''
   If you want to see debugging statements as well as warnings,
   errors, and stack traces: change logging.INFO to logging.DEBUG
'''

def setup_logging(log_dir, job_type, *args):
    """
    Setup logging settings

    If *args doesn't uniquely specify a job, then multiple jobs
    will write to the same log. Don't do that.

    :param log_dir: str
        directory where log files appear
    :param job_type: str
        name of script (ie, correct or diagnostics)
    :param *args:
        any other arguments that uniquely identify any invocation of the job
        (ie codcorrect_version + location_id + sex_name)

    """

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()

    # Assign filename for log, and format
    # how each line is labeled
    log_filename = log_dir + r'/' + job_type + '.txt'
    fh = logging.FileHandler(log_filename,mode='w')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)

    intro_message = 'Starting {j} job. Inputs are : '.format(j=job_type)
    logger.info(intro_message)
