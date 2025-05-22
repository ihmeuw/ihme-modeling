"""Logging helpers."""

import logging
import sys
import warnings

import pandas as pd


def configure_logging() -> None:
    """Sets up a basic logging configuration."""
    warnings.simplefilter("ignore", category=pd.io.pytables.PerformanceWarning)
    warnings.simplefilter("ignore", category=RuntimeWarning)
    warnings.simplefilter("ignore", category=pd.errors.ParserWarning)

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(levelname)s – %(asctime)s – %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
    )
    logging.captureWarnings(True)
