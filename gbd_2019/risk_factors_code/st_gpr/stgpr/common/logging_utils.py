import logging
import sys


def configure_logging() -> None:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(levelname)s – %(asctime)s – %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S'
    )
    logging.captureWarnings(True)
