import logging
import sys
from argparse import Namespace


def configure_logging() -> None:
    """Sets up a basic logging configuration."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(levelname)s – %(asctime)s – %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
    )
    logging.captureWarnings(True)


def list_args(args: Namespace) -> str:
    """Converts namespace into string of arguments separated by newlines.

    Ex:
        "release_id: 9\nversion_id: 123\n..."
    """
    return "\n".join(f"{k}: {v}" for k, v in vars(args).items())
