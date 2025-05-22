import logging
import os

from hybridizer.reference import paths

"""
   If you want to see debugging statements as well as warnings,
   errors, and stack traces: change logging.INFO to logging.DEBUG
"""


def get_log_dir(base_dir: str) -> str:
    """Return directory where logs are saved."""
    return paths.setup_dir(f"{base_dir}/logs")


def setup_logging(base_dir: str) -> str:
    """
    Setup logging settings

    :param model_version_id: str
    :param acause: str
    :param job_type: str
    """
    log_dir = get_log_dir(base_dir=base_dir)
    logger = logging.getLogger()

    # Assign filename for log, and format how each line is labeled
    log_filename = os.path.join(log_dir, "hybridizer.log")
    fh = logging.FileHandler(log_filename, mode="w")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.INFO)
    # Change httpx level to WARNING to avoid logging all rules GET requests.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logger.info(f"Setup logger at {log_filename}.")
    return log_dir
