import os
import logging

'''
   If you want to see debugging statements as well as warnings,
   errors, and stack traces: change logging.INFO to logging.DEBUG
'''


def get_log_dir(model_version_id, acause):
    return 'ADDRESS'.format(acause, model_version_id)


def setup_logging(model_version_id, acause, job_type):
    """
    Setup logging settings

    :param model_version_id: str
    :param acause: str
    :param job_type: str
    """
    log_dir = get_log_dir(model_version_id=model_version_id, acause=acause)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()

    # Assign filename for log, and format
    # how each line is labeled
    log_filename = log_dir + r'/' + job_type + '.txt'
    fh = logging.FileHandler(log_filename, mode='w')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)

    intro_message = 'Starting {j} job for model_version {m} and acause {a}: '.format(j=job_type,
                                                                                     m=model_version_id,
                                                                                     a=acause)
    logger.info(intro_message)
    return log_dir
