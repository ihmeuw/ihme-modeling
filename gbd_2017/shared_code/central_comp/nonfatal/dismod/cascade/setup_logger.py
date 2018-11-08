import os
import logging


def setup_logger(level=logging.DEBUG):
    '''Create a logger for cascade_ode and __main__ and add specific
    formatter and handler if no handler is set
    '''
    fmt_str = (
        '%(asctime)s : %(levelname)s : %(name)s : %(module)s : %(funcName)s : '
        '%(lineno)d :  %(message)s')

    logFormatter = logging.Formatter(fmt_str)
    stderrHandler = logging.StreamHandler()
    stderrHandler.setFormatter(logFormatter)

    log_names = ['cascade_ode', '__main__']
    for n in log_names:
        log = logging.getLogger(n)
        if not log.handlers:
            log.setLevel(level)
            log.addHandler(stderrHandler)

        log.info("HOSTNAME: {}".format(
            os.environ.get('HOSTNAME', 'No hostname in env')))
        log.info("JOB_ID: {}".format(
            os.environ.get('JOB_ID', 'No job_id in env')))
        log.info("JOB_NAME: {}".format(
            os.environ.get('JOB_NAME', 'No job_name in env')))
