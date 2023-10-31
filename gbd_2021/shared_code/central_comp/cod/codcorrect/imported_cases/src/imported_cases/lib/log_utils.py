"""Logging utilities for imported cases."""
import logging
import os


def setup_logging(log_dir: str, job_type: str, *args: str) -> None:
    """Setup logging settings.

    Arguments:
        log_dir: directory where log files appear
        job_type: name of script (ie, correct or diagnostics)
        *args: any other arguments that uniquely identify
            any invocation of the job
            (ie codcorrect_version + location_id + sex_name)
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()

    # Assign filename for log, and format
    # how each line is labeled
    log_filename = log_dir + r"/" + "_".join([job_type] + [arg for arg in args]) + ".txt"
    fh = logging.FileHandler(log_filename, mode="w")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    intro_message = "Starting {j} job. Inputs are: ".format(j=job_type)
    intro_message = intro_message + " ".join(args)
    logger.info(intro_message)
