import logging

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")


def module_logger(module_name: str) -> logging.Logger:
    """Returns module-named logger when called like module_logger(__name__)."""
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    # Change httpx level to WARNING to avoid logging all rules GET requests.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logger
