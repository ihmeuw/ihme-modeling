import logging
import os


def setup_logger(filepath, level=0):
    """Create a logger for cascade_ode and __main__ and add specific
    formatters and handlers if no handler is set

    Args:
        filepath (str): specific filepath to location
            where logfile is to be written. This logfile will exist permenantly
            until it's deleted, so throughout the cascade, these files are saved
            to a model_version's output directory.
        level (int): -1 for debug, 0 for info, 1 for warning, 2 for error,
            3 for exception.
    """
    fmt_str = (
        "%(asctime)s : %(levelname)s : %(name)s : %(module)s : %(funcName)s : "
        "%(lineno)d :  %(message)s"
    )

    log_level = max(logging.DEBUG, logging.INFO + 10 * level)
    logFormatter = logging.Formatter(fmt_str)
    stderrHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler(filepath)
    stderrHandler.setFormatter(logFormatter)
    fileHandler.setFormatter(logFormatter)

    log_names = ["cascade_ode", "__main__"]
    for n in log_names:
        log = logging.getLogger(n)
        if not log.handlers:
            log.setLevel(log_level)
            log.addHandler(stderrHandler)
            log.addHandler(fileHandler)

    hostname = os.environ.get(
        "HOSTNAME", os.environ.get("SLURM_NODELIST", "No hostname in env")
    )
    job_id = os.environ.get("JOB_ID", os.environ.get("SLURM_JOB_ID", "No job_id in env"))
    job_name = os.environ.get(
        "JOB_NAME", os.environ.get("SLURM_JOB_NAME", "No job_name in env")
    )
    log.info(f"HOSTNAME: {hostname}")
    log.info(f"JOB_ID: {job_id}")
    log.info(f"JOB_NAME: {job_name}")
